package main

// One replication slot per Pod.
//
// The plugin hands every instance Pod the same QS_SLOT, because the Pod spec is
// one template. A logical slot is not a topic with subscribers: it is ONE
// cursor, and whoever holds it moves it for everyone. Two sidecars given the
// same name did exactly what that implies. The second looped on "slot is
// active" while the first ran; when the first stopped, the second took the slot
// over at the FIRST one's confirmed position, received nothing from before it,
// passed readiness and served a mirror that was short by every row written
// while it was locked out. No error anywhere.
//
// So the configured name is a prefix and the slot is derived here, where the
// Pod's name is known. It has to be deterministic — a restarted Pod must find
// its own slot again, or every restart is a re-snapshot — and it has to fit
// PostgreSQL's rules for a slot name: [a-z0-9_] and at most 63 bytes.

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// maxSlotName is NAMEDATALEN-1.
const maxSlotName = 63

var (
	notSlotChar  = regexp.MustCompile(`[^a-z0-9_]`)
	plainPodName = regexp.MustCompile(`^[a-z0-9-]*$`)
)

func sanitiseSlotPart(s string) string {
	return notSlotChar.ReplaceAllString(strings.ToLower(s), "_")
}

// podSlotName derives this Pod's slot from the configured name and the Pod's
// name. With no Pod name (a sidecar run by hand, outside Kubernetes) the
// configured name is used as it is: there is nothing to tell two of them apart,
// and the caller says so.
//
// A Kubernetes Pod name is normally [a-z0-9-], and turning '-' into '_' loses
// nothing because '_' cannot appear in one. Anything else ('.', upper case) is
// folded by the sanitiser, so two such names could land on one slot; those get a
// hash of the UNSANITISED pair appended. So does a name that has to be cut to
// fit, so that Pods differing only past the cut still get different slots.
func podSlotName(base, pod string) string {
	name := sanitiseSlotPart(base)
	if pod != "" {
		name += "_" + sanitiseSlotPart(pod)
	}
	return fitSlotName(name, base+"\x00"+pod, !plainPodName.MatchString(pod))
}

// snapshotSlotName is the TEMPORARY slot a re-snapshot borrows its consistent
// point from. Derived from this Pod's slot so that it, too, is this Pod's own.
func snapshotSlotName(slot string) string {
	return fitSlotName(slot+"_resnap", slot+"\x00resnap", false)
}

func fitSlotName(name, identity string, alwaysTag bool) string {
	if len(name) <= maxSlotName && !alwaysTag {
		return name
	}
	sum := sha256.Sum256([]byte(identity))
	tag := "_" + hex.EncodeToString(sum[:4])
	if len(name) > maxSlotName-len(tag) {
		name = name[:maxSlotName-len(tag)]
	}
	return name + tag
}
