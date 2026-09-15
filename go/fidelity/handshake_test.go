package fidelity

// The CNPG-I handshake, run for real against the plugin binary's own server
// code — just without Kubernetes in the middle.
//
// The sequence below is not invented. It is CloudNativePG's
// internal/cnpi/plugin/connection.LoadPlugin, step for step:
//
//	GetPluginMetadata                      (fails here => plugin is unusable)
//	GetPluginCapabilities                  (the declared service list)
//	then, ONLY for each declared service, that service's GetCapabilities
//
// The gating in the last line is why the Identity test in the plugin package
// insists the declared list matches the registered one exactly. CloudNativePG
// never probes a service we did not declare, so a service we forget to declare
// is simply never used — and a service we declare but do not register answers
// Unimplemented during discovery, which fails the whole plugin load.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/http"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	postgresi "github.com/cloudnative-pg/cnpg-i/pkg/postgres"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

// startPlugin runs the REAL server from cnpg-i-machinery — the same Start() the
// quicksilver-plugin binary reaches through CreateMainCmd — over mTLS on a
// loopback port, and returns its address.
func startPlugin(t *testing.T, certs *certSet) string {
	t.Helper()

	// Pick a free port, then hand the address to the server. The machinery
	// binds it itself, so this is the same code path as production rather than
	// a listener we built and passed in.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	addr := l.Addr().String()
	must(t, l.Close())

	srv := &http.Server{
		IdentityImpl: qsplugin.IdentityImpl{},
		Enrichers: []http.ServerEnricher{func(s *grpc.Server) error {
			operator.RegisterOperatorServer(s, qsplugin.OperatorImpl{})
			lifecycle.RegisterOperatorLifecycleServer(s, qsplugin.LifecycleImpl{})
			postgresi.RegisterPostgresServer(s, qsplugin.PostgresImpl{})
			return nil
		}},
		ServerCertPath: certs.serverCert,
		ServerKeyPath:  certs.serverKey,
		ClientCertPath: certs.caPath(),
		ServerAddress:  addr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitListening(t, addr)
	return addr
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("plugin never listened on %s", addr)
}

// dial reproduces CNPG's connection.ProtocolTCP.Dial exactly: grpc.NewClient
// with TLS transport credentials, nothing else.
func dial(t *testing.T, addr string, cfg *tls.Config) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	must(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// operatorTLS is the client side the operator presents: our client certificate,
// and the CA it verifies the server against. ServerName is the Service name,
// which is how the SANs on the server certificate come to matter.
func operatorTLS(t *testing.T, certs *certSet, serverName string) *tls.Config {
	t.Helper()
	pair, err := tls.X509KeyPair(certs.clientCertPEM, certs.clientKeyPEM)
	must(t, err)
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      certs.caPool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}
}

// TestHandshake is the one this whole module exists for: CloudNativePG's plugin
// load sequence, against the real server, over real mTLS.
func TestHandshake(t *testing.T) {
	certs := newCertSet(t, "quicksilver", "quicksilver.cnpg-system",
		"quicksilver.cnpg-system.svc", "quicksilver.cnpg-system.svc.cluster.local")
	addr := startPlugin(t, certs)
	conn := dial(t, addr, operatorTLS(t, certs, "quicksilver"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// --- step 1: identity -------------------------------------------------
	idc := identity.NewIdentityClient(conn)
	meta, err := idc.GetPluginMetadata(ctx, &identity.GetPluginMetadataRequest{})
	if err != nil {
		t.Fatalf("GetPluginMetadata failed; CloudNativePG would reject the plugin here: %v", err)
	}
	if meta.GetName() != qsplugin.PluginName {
		t.Fatalf("plugin reports name %q but Clusters reference %q; CNPG keys plugins "+
			"by this name, so they would never match", meta.GetName(), qsplugin.PluginName)
	}
	t.Logf("plugin %s version %s, licence %s", meta.GetName(), meta.GetVersion(), meta.GetLicense())

	// --- step 2: declared services ----------------------------------------
	capsRes, err := idc.GetPluginCapabilities(ctx, &identity.GetPluginCapabilitiesRequest{})
	must(t, err)
	var declared []identity.PluginCapability_Service_Type
	for _, c := range capsRes.GetCapabilities() {
		declared = append(declared, c.GetService().GetType())
	}
	t.Logf("declared services: %v", declared)

	// --- step 3: capabilities of each declared service --------------------
	// CNPG only asks a service that was declared, so this loop is the whole of
	// the gating. Anything that errors here fails the plugin load outright.
	if slices.Contains(declared, identity.PluginCapability_Service_TYPE_OPERATOR_SERVICE) {
		res, err := operator.NewOperatorClient(conn).GetCapabilities(ctx,
			&operator.OperatorCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("Operator service declared but GetCapabilities failed: %v", err)
		}
		var rpcs []operator.OperatorCapability_RPC_Type
		for _, c := range res.GetCapabilities() {
			rpcs = append(rpcs, c.GetRpc().GetType())
		}
		t.Logf("operator RPCs: %v", rpcs)
		for _, want := range []operator.OperatorCapability_RPC_Type{
			operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CREATE,
			operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CHANGE,
			operator.OperatorCapability_RPC_TYPE_MUTATE_CLUSTER,
		} {
			if !slices.Contains(rpcs, want) {
				t.Errorf("operator RPC %v not advertised; CNPG would never call it", want)
			}
		}
	} else {
		t.Fatal("Operator service not declared; validation and mutation would never run")
	}

	if slices.Contains(declared, identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE) {
		res, err := lifecycle.NewOperatorLifecycleClient(conn).GetCapabilities(ctx,
			&lifecycle.OperatorLifecycleCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("Lifecycle service declared but GetCapabilities failed: %v", err)
		}
		found := false
		for _, c := range res.GetLifecycleCapabilities() {
			t.Logf("lifecycle hook: group=%q kind=%q ops=%v", c.GetGroup(), c.GetKind(),
				c.GetOperationTypes())
			if c.GetKind() == "Pod" {
				found = true
				// A hook that does not declare CREATE never sees a new Pod, so
				// the sidecar would appear only on Pods that happen to be
				// patched later — which is to say, unpredictably.
				var ops []lifecycle.OperatorOperationType_Type
				for _, o := range c.GetOperationTypes() {
					ops = append(ops, o.GetType())
				}
				if !slices.Contains(ops, lifecycle.OperatorOperationType_TYPE_CREATE) {
					t.Error("Pod hook does not declare CREATE; new instance Pods would " +
						"be created without the mirror sidecar")
				}
			}
		}
		if !found {
			t.Error("no Pod lifecycle hook declared; the sidecar would never be injected")
		}
	} else {
		t.Fatal("Lifecycle service not declared; the sidecar would never be injected")
	}

	if slices.Contains(declared, identity.PluginCapability_Service_TYPE_POSTGRES) {
		res, err := postgresi.NewPostgresClient(conn).GetCapabilities(ctx,
			&postgresi.PostgresCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("Postgres service declared but GetCapabilities failed: %v", err)
		}
		t.Logf("postgres RPCs: %d", len(res.GetCapabilities()))
	} else {
		t.Fatal("Postgres service not declared; wal_level would never be set to logical")
	}

	// --- and the services we do NOT declare -------------------------------
	// These must fail, and failing is correct: CNPG never calls them because we
	// did not declare them. A plugin that answered here while declaring nothing
	// would be relying on CNPG asking anyway, which it does not.
	for name, call := range map[string]func() error{
		"WAL": func() error {
			_, err := wal.NewWALClient(conn).GetCapabilities(ctx, &wal.WALCapabilitiesRequest{})
			return err
		},
		"ReconcilerHooks": func() error {
			_, err := reconciler.NewReconcilerHooksClient(conn).GetCapabilities(ctx,
				&reconciler.ReconcilerHooksCapabilitiesRequest{})
			return err
		},
	} {
		if slices.Contains(declared, serviceTypeFor(name)) {
			t.Errorf("%s is declared but this test assumes it is not", name)
			continue
		}
		if err := call(); err == nil {
			t.Errorf("%s answered although it is not registered; the capability list "+
				"and the registered services have drifted apart", name)
		} else {
			t.Logf("undeclared %s correctly unavailable: %v", name, statusOf(err))
		}
	}
}

func serviceTypeFor(name string) identity.PluginCapability_Service_Type {
	switch name {
	case "WAL":
		return identity.PluginCapability_Service_TYPE_WAL_SERVICE
	case "ReconcilerHooks":
		return identity.PluginCapability_Service_TYPE_RECONCILER_HOOKS
	}
	return identity.PluginCapability_Service_TYPE_UNSPECIFIED
}

func statusOf(err error) string {
	s := err.Error()
	if len(s) > 80 {
		return s[:80] + "…"
	}
	return s
}

// TestMTLSIsEnforced checks the thing that decides whether the plugin is a
// private endpoint or an open one. A gRPC server in the operator namespace that
// accepts anonymous calls lets anything in the cluster mutate Cluster specs.
func TestMTLSIsEnforced(t *testing.T) {
	certs := newCertSet(t, "quicksilver")
	addr := startPlugin(t, certs)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	probe := func(cfg *tls.Config) error {
		conn := dial(t, addr, cfg)
		_, err := identity.NewIdentityClient(conn).GetPluginMetadata(ctx,
			&identity.GetPluginMetadataRequest{})
		return err
	}

	t.Run("no client certificate is refused", func(t *testing.T) {
		if err := probe(&tls.Config{
			RootCAs: certs.caPool, ServerName: "quicksilver", MinVersion: tls.VersionTLS12,
		}); err == nil {
			t.Fatal("an anonymous caller reached the plugin; anything in the operator's " +
				"namespace could mutate Cluster specs")
		} else {
			t.Logf("refused: %v", statusOf(err))
		}
	})

	t.Run("a client certificate from another CA is refused", func(t *testing.T) {
		certPEM, keyPEM, _ := issueClientFrom(t)
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		must(t, err)
		if err := probe(&tls.Config{
			Certificates: []tls.Certificate{pair},
			RootCAs:      certs.caPool, ServerName: "quicksilver", MinVersion: tls.VersionTLS12,
		}); err == nil {
			t.Fatal("a certificate signed by an unrelated CA was accepted")
		} else {
			t.Logf("refused: %v", statusOf(err))
		}
	})

	t.Run("a server name outside the certificate's SANs is refused", func(t *testing.T) {
		// This is the chart's most likely misconfiguration: the operator dials
		// quicksilver.cnpg-system.svc and the certificate only carries the short
		// name. It presents as a plugin outage with a TLS error in the operator
		// log and nothing at all in the plugin's.
		if err := probe(operatorTLS(t, certs, "quicksilver.cnpg-system.svc")); err == nil {
			t.Fatal("a name absent from the certificate SANs was accepted")
		} else {
			t.Logf("refused: %v", statusOf(err))
		}
	})

	t.Run("the operator's own certificate and names work", func(t *testing.T) {
		if err := probe(operatorTLS(t, certs, "quicksilver")); err != nil {
			t.Fatalf("the legitimate client was refused: %v", err)
		}
	})
}

var _ = fmt.Sprintf
