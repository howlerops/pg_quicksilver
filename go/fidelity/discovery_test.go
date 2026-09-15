package fidelity

// The one step of the CNPG-I handshake that genuinely needs an API server:
// finding the plugin.
//
// CloudNativePG does not read any configuration to learn which plugins exist.
// It LISTS SERVICES in its own namespace and treats every Service carrying the
// cnpg.io/pluginName label as a plugin, reading the address and the mTLS Secret
// names off the annotations. That is the entire registration mechanism, and
// every way of getting it wrong presents the same way: the plugin simply is not
// there, with nothing in either log naming a cause.
//
// This runs a real kube-apiserver and etcd as local processes (the envtest
// control plane — no container runtime, no cluster), applies the Helm chart's
// own rendered output to it, and then performs the discovery the way the
// operator does. What is asserted is not that our YAML parses; it is that an
// operator looking for plugins would find this one, in the right namespace,
// under the right name, pointing at the right port and Secrets.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

// These are CloudNativePG's own constants for plugin discovery, from
// internal/cnpi/plugin/repository. They are duplicated here rather than
// imported because that package is internal to the operator — which means a
// rename upstream would break us silently, and this test is where that would
// show up as a discovery miss rather than as a compile error.
const (
	pluginNameLabel     = "cnpg.io/pluginName"
	pluginPortAnnot     = "cnpg.io/pluginPort"
	pluginClientSecretA = "cnpg.io/pluginClientSecret"
	pluginServerSecretA = "cnpg.io/pluginServerSecret"
)

func startAPIServer(t *testing.T) client.Client {
	t.Helper()
	if _, err := os.Stat("/usr/local/kubebuilder/bin/kube-apiserver"); err != nil {
		t.Skip("no envtest control plane installed; see docs/18")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("could not start the API server: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	must(t, err)
	t.Logf("kube-apiserver up at %s", short(cfg))
	return c
}

func short(cfg *rest.Config) string {
	if i := strings.LastIndex(cfg.Host, "/"); i >= 0 {
		return cfg.Host[i+1:]
	}
	return cfg.Host
}

// helmTemplate renders the chart exactly as `helm install` would.
func helmTemplate(t *testing.T, args ...string) []*unstructuredDoc {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}
	cmd := exec.Command("helm", append([]string{"template", "quicksilver",
		"../../charts/quicksilver"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	var docs []*unstructuredDoc
	for _, chunk := range strings.Split(string(out), "\n---\n") {
		if strings.TrimSpace(stripComments(chunk)) == "" {
			continue
		}
		var m map[string]any
		if err := yaml.Unmarshal([]byte(chunk), &m); err != nil {
			t.Fatalf("chart emitted invalid YAML: %v", err)
		}
		if len(m) == 0 {
			continue
		}
		docs = append(docs, &unstructuredDoc{raw: []byte(chunk), obj: m})
	}
	return docs
}

type unstructuredDoc struct {
	raw []byte
	obj map[string]any
}

func (d *unstructuredDoc) kind() string {
	k, _ := d.obj["kind"].(string)
	return k
}

func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// TestPluginIsDiscoverable applies the chart to a real API server and then
// performs CloudNativePG's discovery against it.
func TestPluginIsDiscoverable(t *testing.T) {
	c := startAPIServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const ns = "cnpg-system"
	must(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))

	applied, skipped := applyChart(t, ctx, c, helmTemplate(t))
	t.Logf("applied %d objects; skipped %d needing CRDs we do not install (cert-manager)",
		applied, skipped)

	// --- discovery, the way the operator does it -------------------------
	var services corev1.ServiceList
	must(t, c.List(ctx, &services, client.InNamespace(ns),
		client.HasLabels{pluginNameLabel}))

	if len(services.Items) == 0 {
		t.Fatal("an operator listing Services with the cnpg.io/pluginName label in " +
			"cnpg-system would find NOTHING; the plugin would never load, and neither " +
			"the operator nor the plugin would log a reason")
	}
	if len(services.Items) != 1 {
		t.Fatalf("expected exactly one plugin Service, found %d", len(services.Items))
	}
	svc := services.Items[0]
	t.Logf("discovered Service %s/%s", svc.Namespace, svc.Name)

	// --- and what the operator reads off it ------------------------------
	name := svc.Labels[pluginNameLabel]
	if name != qsplugin.PluginName {
		t.Errorf("Service advertises plugin %q but Clusters reference %q; CNPG matches "+
			"these as exact strings, so no Cluster would ever resolve this plugin",
			name, qsplugin.PluginName)
	}

	port := svc.Annotations[pluginPortAnnot]
	if port == "" {
		t.Error("no cnpg.io/pluginPort annotation; the operator has no address to dial")
	}
	// The annotation is what the operator dials. If it names a port the Service
	// does not expose, the dial fails with a connection error that names a port
	// nobody configured anywhere visible.
	exposed := false
	for _, p := range svc.Spec.Ports {
		if fmt.Sprint(p.Port) == port {
			exposed = true
		}
	}
	if !exposed {
		var have []string
		for _, p := range svc.Spec.Ports {
			have = append(have, fmt.Sprint(p.Port))
		}
		t.Errorf("cnpg.io/pluginPort=%s but the Service exposes %v", port, have)
	}

	for _, annot := range []string{pluginClientSecretA, pluginServerSecretA} {
		if svc.Annotations[annot] == "" {
			t.Errorf("no %s annotation; the operator cannot find the mTLS material", annot)
		}
	}
	t.Logf("port=%s clientSecret=%s serverSecret=%s", port,
		svc.Annotations[pluginClientSecretA], svc.Annotations[pluginServerSecretA])

	// --- the Service must actually select the Deployment -----------------
	// A selector that matches nothing is a Service with no endpoints: discovery
	// succeeds, the dial times out, and the failure looks like a network problem.
	var deploys appsv1.DeploymentList
	must(t, c.List(ctx, &deploys, client.InNamespace(ns)))
	matched := 0
	for _, d := range deploys.Items {
		if labelsMatch(svc.Spec.Selector, d.Spec.Template.Labels) {
			matched++
			n := int32(1)
			if d.Spec.Replicas != nil {
				n = *d.Spec.Replicas
			}
			t.Logf("Service selects Deployment %s (%d replica(s))", d.Name, n)
		}
	}
	if matched != 1 {
		t.Errorf("the plugin Service selects %d Deployments; it must select exactly one, "+
			"or discovery succeeds and every dial times out", matched)
	}
}

// TestDiscoveryMissesAnotherNamespace is the negative half, and it is the
// mistake people actually make: installing the plugin next to the Cluster
// instead of next to the operator.
func TestDiscoveryMissesAnotherNamespace(t *testing.T) {
	c := startAPIServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for _, ns := range []string{"cnpg-system", "databases"} {
		must(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}
	// Install it where the Cluster lives, which is the natural-looking mistake.
	applyChart(t, ctx, c, helmTemplate(t, "--set", "operatorNamespace=databases"))

	var found corev1.ServiceList
	must(t, c.List(ctx, &found, client.InNamespace("cnpg-system"),
		client.HasLabels{pluginNameLabel}))
	if len(found.Items) != 0 {
		t.Fatal("test is not testing what it claims: something was installed in cnpg-system")
	}
	t.Log("as expected: an operator in cnpg-system finds no plugin, although the " +
		"chart installed cleanly in databases — this is the silent failure the " +
		"chart's operatorNamespace value exists to prevent")

	// And it IS there, in the wrong place, which is why nothing reports an error.
	var elsewhere corev1.ServiceList
	must(t, c.List(ctx, &elsewhere, client.InNamespace("databases"),
		client.HasLabels{pluginNameLabel}))
	if len(elsewhere.Items) != 1 {
		t.Fatalf("expected the misplaced Service in databases, found %d", len(elsewhere.Items))
	}
}

func applyChart(t *testing.T, ctx context.Context, c client.Client,
	docs []*unstructuredDoc,
) (applied, skipped int) {
	t.Helper()
	for _, d := range docs {
		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(d.raw, nil, nil)
		if err != nil {
			// cert-manager Certificates and Issuers: their CRDs are not
			// installed here, and installing them would test cert-manager
			// rather than us. The mTLS material they produce is tested for real
			// in handshake_test.go with certificates generated directly.
			skipped++
			continue
		}
		co, ok := obj.(client.Object)
		if !ok {
			skipped++
			continue
		}
		if err := c.Create(ctx, co); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("the API server rejected %s/%s from the chart: %v",
				d.kind(), co.GetName(), err)
		}
		applied++
	}
	return applied, skipped
}

func labelsMatch(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

var _ runtime.Object = (*corev1.Service)(nil)
