# 33 — The probe that gated the wrong thing

Implementing `mode: takeover` turned up a bug in the mode that ships **on by
default**, and then established that takeover cannot mean what
[docs/05](05-cnpg-integration.md) said it would.

Both findings come from the same sentence in the Kubernetes documentation.

---

## The sentence

The mirror is injected as a **native sidecar** — a restartable init container,
via `InjectPluginSidecarInitContainer`. For those, Kubernetes says:

> If a `readinessProbe` is specified for this init container, its result will be
> used to determine the `ready` state of the Pod.

So a readiness probe on the sidecar is not a probe on the mirror. It is a probe
on the **instance Pod**, PostgreSQL included. A Pod that is not ready leaves
every Service that selects it.

The sidecar's `/readyz` reports not-ready when the mirror is behind its
`freshnessSLO`. It was wired unconditionally, in every mode that injects a
sidecar.

---

## What that did to `shadow`

`shadow` is the default, and [docs/05](05-cnpg-integration.md) defines it as
"**`-ro` untouched**" — the evidence-gathering mode, where the mirror is built
and measured and nothing depends on it.

With the probe wired, a mirror that fell behind took its PostgreSQL replica out
of `<cluster>-ro` and `<cluster>-r` with it. The mirror nobody was querying
could remove the server everybody was querying.

And it would not do it to one replica. Every replica follows the same writer, so
they fall behind **together**. The condition that trips the probe is a write
burst, which trips it everywhere at once and empties the read endpoint.

This is not hypothetical arithmetic. [docs/28](28-the-slot-is-a-loaded-gun.md)
measured exactly that condition on this code: 11.7 million row-changes in 60
seconds against a mirror draining at roughly half the rate. Every replica in
that cluster would have left `-ro`, in shadow mode, because a mirror was slow.

The fix is one line of policy: the probe belongs to `takeover` and to nothing
else. `/readyz` is still served in every mode — it stays the way to ask whether
a mirror is fresh, from a dashboard or from `qs-verify`. What changes is that in
`shadow` the answer no longer decides whether PostgreSQL gets traffic.

---

## What that does to `takeover`

docs/05 specified takeover as:

> `-ro` retargeted to mirror pods; original standbys remain on `app-ro-row`.

That was written for an architecture with a **separate mirror tier**. The one
that got built puts the mirror in the instance Pod, to share the data volume
([docs/16](16-deploying.md)). Two consequences follow, and neither is a matter
of effort:

**There is nothing to retarget.** `-ro` already selects the replica Pods, and
those Pods are the mirror Pods. One PostgreSQL answers both engines. A selector
rewrite would name the same endpoints it names today.

**`app-ro-row` cannot exist.** The point of it was a row-store fallback for
traffic that regresses on the mirror. But Service endpoints are gated on **Pod**
readiness, and the Pod is one Pod. A replica whose mirror is stale is absent
from every Service that selects it, so there is no Service that can offer "this
node's heap, regardless of mirror freshness". `publishNotReadyAddresses` would
technically include it — along with genuinely broken PostgreSQL, which is worse
than the problem.

So the `-ro`/`app-ro-row` split in docs/05 is not deferred work. It is not
reachable from here, and the plugin should not pretend otherwise.

### What takeover therefore is

Exactly one thing: **mirror freshness becomes a condition of serving.** A
replica whose mirror is behind `freshnessSLO` leaves `-ro`.

That is a real, enforceable property, and it is the one that makes bounded
staleness a guarantee rather than a hope — which was always
[G4](05-cnpg-integration.md#readiness-gating-goal-g4). It is also strictly worse
for availability than `shadow`, for the reason above: a write burst can empty
the read endpoint. Both halves are now in the text a user must acknowledge to
enable it, alongside the 935× figure that was already there.

The `.status.pluginStatus` message used to read "the `<cluster>-ro` service is
served by the columnar mirror". That was never true and cannot become true here.
It now describes the gate.

---

## What is verified, and what is not

| | |
|---|---|
| the probe is absent in `off` and `shadow`, present in `takeover` | unit-tested, both directions |
| liveness is unconditional in every mode | unit-tested |
| the acknowledgement gate still refuses takeover without the opt-in | unit-tested, unchanged |
| a stale mirror actually removing a Pod from `-ro` | **not observed** — needs a kubelet |

The last row is the honest limit. The Kubernetes semantics are documented and
the Pod spec is real, but nothing in this sandbox runs a kubelet, so no probe has
ever failed and removed anything ([docs/21](21-against-the-real-operator.md) is
the same boundary). What can be said is that the spec no longer asks Kubernetes
to do the wrong thing in the default mode.

---

## The shape of this bug

It presented as a feature. [docs/16](16-deploying.md) described the probe
approvingly — "`/readyz` is what actually keeps a stale mirror out of service …
a correctness dependency rather than a nicety" — and that sentence is true, in
takeover. It was written about a mode that did not exist yet and applied to the
one that did.

Nothing failed. No test caught it, because no test asserted on the probe at all;
the only assertion was that one existed, which is the opposite of the property
that mattered. It would have been found the first time a busy cluster's read
endpoint emptied, in the mode documented as the safe one to start with.
