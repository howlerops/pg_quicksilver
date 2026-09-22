package fidelity

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The rollout tests in this package prove a property of the CloudNativePG
// version in go.mod. The cluster test installs a CloudNativePG of its own. When
// those two drift, the unit tests keep passing while the deployed operator
// cannot do the thing they prove — which is not hypothetical:
//
//	go/fidelity/go.mod          v1.30.0   rollout test passes
//	bench/scripts/cluster_e2e   1.25.1    section 4 could never pass
//
// 1.25's specs.PodWithExistingStorage builds the target Pod with NO plugin
// client and writes that unpatched spec into the cnpg.io/podSpec annotation, so
// the sidecar is missing from both sides of checkPodSpecIsOutdated's comparison
// and no plugin parameter can ever roll an instance. 1.26 replaced it with
// specs.NewInstance, which calls the lifecycle hook with OperationVerbEvaluate
// before the annotation is written. docs/37 has the full account.
//
// This test is the tie: both versions must be at or above the floor, so the
// property the unit tests demonstrate is a property of something that gets
// installed.
const operatorFloorMinor = 26 // CloudNativePG 1.26

func TestTheDeployedOperatorMeetsTheRolloutFloor(t *testing.T) {
	t.Run("cluster_e2e.sh installs 1.26 or newer", func(t *testing.T) {
		src, err := os.ReadFile("../../bench/scripts/cluster_e2e.sh")
		if err != nil {
			t.Fatalf("cannot read the cluster harness: %v", err)
		}
		m := regexp.MustCompile(`CNPG_VERSION=\$\{CNPG_VERSION:-([0-9]+)\.([0-9]+)\.([0-9]+)\}`).
			FindSubmatch(src)
		if m == nil {
			t.Fatal("no CNPG_VERSION default in cluster_e2e.sh; the floor cannot be checked, " +
				"and an unchecked floor is how 1.25.1 stayed there")
		}
		major, minor := atoi(t, string(m[1])), atoi(t, string(m[2]))
		if major != 1 || minor < operatorFloorMinor {
			t.Errorf("cluster_e2e.sh installs CloudNativePG %s.%s.%s, below the 1.%d floor: "+
				"it would build the target Pod without the plugin, so no parameter change "+
				"could ever roll an instance and sections 4 and 5 cannot pass",
				m[1], m[2], m[3], operatorFloorMinor)
		}
	})

	t.Run("go.mod is 1.26 or newer", func(t *testing.T) {
		src, err := os.ReadFile("go.mod")
		if err != nil {
			t.Fatalf("cannot read go.mod: %v", err)
		}
		m := regexp.MustCompile(`cloudnative-pg/cloudnative-pg\s+v([0-9]+)\.([0-9]+)\.`).
			FindSubmatch(src)
		if m == nil {
			t.Fatal("no cloudnative-pg requirement in go.mod")
		}
		major, minor := atoi(t, string(m[1])), atoi(t, string(m[2]))
		if major != 1 || minor < operatorFloorMinor {
			t.Errorf("go.mod pins CloudNativePG %s.%s, below the 1.%d floor: the rollout "+
				"tests in this package would be proving a property the operator does not have",
				m[1], m[2], operatorFloorMinor)
		}
	})

	// The floor is only meaningful while it is written down where an operator
	// of this software will read it, not just where its tests will.
	t.Run("the requirement is documented", func(t *testing.T) {
		for _, f := range []string{"../../docs/16-deploying.md", "../../charts/quicksilver/README.md"} {
			src, err := os.ReadFile(f)
			if err != nil {
				t.Errorf("cannot read %s: %v", f, err)
				continue
			}
			if !strings.Contains(string(src), "1.26") {
				t.Errorf("%s does not mention the CloudNativePG 1.26 requirement, so someone "+
					"installing this on 1.25 would find that mode and freshnessSLO silently "+
					"never take effect", f)
			}
		}
	})
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("not a number: %q", s)
	}
	return n
}
