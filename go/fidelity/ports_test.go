package fidelity

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
)

// Every container in a Pod shares one network namespace, so two containers that
// bind the same port are not two listeners — they are one listener and one
// crash.
//
// The mirror used to serve /readyz, /healthz and /metrics on 9187, which is
// where CloudNativePG's instance manager serves its own metrics. The mirror is
// a NATIVE sidecar, so it starts first, binds the port, and the instance
// manager then cannot:
//
//	{"level":"error","msg":"Error while running the web server",
//	 "address":":9187","error":"listen tcp :9187: bind: address already in use"}
//	Error: unretryable: listen tcp :9187: bind: address already in use
//
// It treats that as unretryable and exits, so PostgreSQL never starts and the
// instance Pod crash-loops — in shadow mode, on every instance. A plugin whose
// entire premise is that it does not disturb the database was stopping it from
// booting, and nothing in this repository could see it, because the collision
// only exists inside a real CNPG instance Pod.
//
// This test is that Pod. It asks CloudNativePG to build an instance, runs the
// real hook over it, and then refuses any port claimed twice.
func TestNoContainerPortIsClaimedTwice(t *testing.T) {
	cluster := quicksilverCluster()
	pod := realInstancePod(t, cluster, 1)
	patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE)
	if patched == nil {
		t.Fatal("no patch emitted; there is nothing to check for collisions")
	}

	type claim struct {
		container string
		name      string
	}
	seen := map[int32]claim{}

	check := func(cs []corev1.Container, kind string) {
		for _, c := range cs {
			for _, p := range c.Ports {
				if prev, dup := seen[p.ContainerPort]; dup {
					t.Errorf("port %d is claimed by BOTH %q (%s) and %q (%s) in one "+
						"network namespace; whichever starts second fails to bind",
						p.ContainerPort, prev.container, prev.name, c.Name, p.Name)
					continue
				}
				seen[p.ContainerPort] = claim{container: c.Name, name: p.Name}
			}
		}
		_ = kind
	}
	check(patched.Spec.InitContainers, "init")
	check(patched.Spec.Containers, "main")

	for port, by := range seen {
		t.Logf("port %5d  %s (%s)", port, by.container, by.name)
	}

	// Named explicitly as well as structurally. CloudNativePG serves the
	// instance manager's metrics on 9187; if a future edit moves the mirror back
	// onto it, the loop above only catches it while CNPG still DECLARES the port
	// in the Pod spec. This line catches it regardless.
	for _, c := range patched.Spec.InitContainers {
		if c.Name != "quicksilver-mirror" {
			continue
		}
		for _, p := range c.Ports {
			if p.ContainerPort == 9187 {
				t.Error("the mirror is back on 9187, which is CloudNativePG's " +
					"instance-manager metrics port; PostgreSQL will not start")
			}
		}
	}
}
