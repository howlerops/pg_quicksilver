// Package fidelity tests the plugin against the real CloudNativePG contract
// without a Kubernetes cluster.
//
// The CNPG-I "handshake" is not a Kubernetes thing. It is CloudNativePG's
// plugin client dialling a gRPC server over mutual TLS and asking it a fixed
// sequence of questions. Only one step of that needs a cluster — finding the
// Service by label — and it is a label lookup. Everything else can be run here:
// real certificates, the real cnpg-i-machinery server, a real mTLS dial, and the
// same call sequence CloudNativePG's own connection.LoadPlugin performs.
//
// What is faithfully reproduced, and where it came from:
//
//	mTLS dial              CNPG internal/cnpi/plugin/connection/remote.go
//	handshake sequence     CNPG internal/cnpi/plugin/connection/connection.go
//	                       (LoadPlugin, and the capability gating in it)
//	instance Pod           CNPG pkg/specs.NewInstance — the actual builder,
//	                       imported, not a hand-written fixture
//
// What is NOT reproduced: Service discovery by the cnpg.io/pluginName label.
package fidelity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// certSet is what the Helm chart asks cert-manager to produce: one self-signed
// CA, a server certificate with the Service's DNS names, and a client
// certificate the operator presents.
type certSet struct {
	dir                        string
	caPEM                      []byte
	serverCert, serverKey      string // file paths, as the plugin flags take
	clientCertPEM, clientKeyPEM []byte
	caPool                     *x509.CertPool
}

func newCertSet(t *testing.T, serverDNSNames ...string) *certSet {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "quicksilver-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	must(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	must(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	serverPEM, serverKeyPEM := issue(t, caCert, caKey, "quicksilver-server",
		serverDNSNames, x509.ExtKeyUsageServerAuth)
	clientPEM, clientKeyPEM := issue(t, caCert, caKey, "quicksilver-client",
		nil, x509.ExtKeyUsageClientAuth)

	cs := &certSet{
		dir:           dir,
		caPEM:         caPEM,
		serverCert:    write(t, dir, "server.crt", serverPEM),
		serverKey:     write(t, dir, "server.key", serverKeyPEM),
		clientCertPEM: clientPEM,
		clientKeyPEM:  clientKeyPEM,
		caPool:        x509.NewCertPool(),
	}
	cs.caPool.AppendCertsFromPEM(caPEM)
	// The plugin's --client-cert flag takes the CA that signs client certs; it
	// is what the server verifies incoming certificates against.
	write(t, dir, "ca.crt", caPEM)
	return cs
}

// caPath is the file the plugin's --client-cert flag points at.
func (c *certSet) caPath() string { return filepath.Join(c.dir, "ca.crt") }

// issueClientFrom makes a client certificate signed by a DIFFERENT CA, for the
// negative test: a cert that is perfectly valid and simply not ours.
func issueClientFrom(t *testing.T) ([]byte, []byte, *x509.CertPool) {
	t.Helper()
	other := newCertSet(t, "quicksilver")
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(other.caPEM)
	return other.clientCertPEM, other.clientKeyPEM, pool
}

func issue(
	t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey,
	cn string, dnsNames []string, usage x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	must(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	must(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func write(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	must(t, os.WriteFile(p, data, 0o600))
	return p
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
