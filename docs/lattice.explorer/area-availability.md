---
agent_spec: "docs/agents/capabilities.yaml"
---

# Area availability

The Explorer compiles in its native areas, but you only see the ones you can
use. Each area decides for itself, on every navigation, whether you may see it.
This page explains the four answers an area can give, why the Explorer fails
closed, and what each area checks.

## Four answers

Every area answers one question for the signed-in caller on the connected
cluster: may this caller see you, now?

| Answer | Spine and Home | Opening its address |
|---|---|---|
| Visible | A normal stop, optionally with a short badge such as a count. | The area's page. |
| Unavailable | A demoted stop showing a one-sentence reason, such as "Sign in to administer access on this cluster." | The area's name and the same reason under "Not available". |
| Not checked | A demoted stop reading "Could not be checked: the cluster did not answer.", with a **Check again** button. | The area's name and the same sentence under "Could not be checked", with **Check again**. |
| Hidden | No stop at all. | The not-found page, exactly as for an address that does not exist. |

**Unavailable** is for something you can act on or wait out: you are not signed
in, the Explorer is not connected, the cluster did not answer, or (in Backups)
you hold no grant. The reason says what would change the answer.

**Not checked** is for a probe the cluster did not answer at all: a transient
fault, such as a facade reporting `Internal` while a silo is leaving the
cluster, or no answer in time. It is not an answer, so it neither hides the area
nor admits you to it; see [Faults the cluster did not answer](#faults-the-cluster-did-not-answer).

**Hidden** is for everything else: the cluster does not serve the facade the
area needs, the host did not register it, or you are signed in and the cluster
refused you. A hidden area is left out, not demoted, and its address renders the
not-found page, so the Explorer never confirms that an area you cannot use
exists.

## Failing closed

An area that cannot prove you may see it is not opened.

- **Time-boxed.** The Explorer asks every area in parallel and waits at most
  three seconds for each. A definite fault, or a probe cancellation not caused
  by the navigation, hides that area for the navigation; a transient fault or a
  timeout leaves it not checked. Cancelling the navigation itself propagates
  instead of being rendered as Hidden. One slow facade never stalls the spine.
- **Absent means hidden.** An answer that was never given reads as hidden, and
  an area whose facade is not served by the host or the cluster is hidden.
- **Nothing before the session.** No area is asked anything until the circuit's
  connection settings and sign-in have loaded, so no probe runs as the wrong
  caller. Until then the content region shows a loading skeleton.
- **No page before the answer.** An area's page renders only after its area has
  answered Visible for the current navigation.
- **Asked again when things change.** The spine asks again on every navigation,
  and whenever sign-in, the connection or the connection settings change. Areas
  remember an answer for the current caller, so this stays cheap, and ask afresh
  when a different caller signs in.

## Faults the cluster did not answer

A capability probe can fail because the cluster refused the caller, or because
it did not answer at all - for example while a silo is leaving the cluster and
the grain activations it held are being moved. The two need opposite handling,
so the Explorer reads the gRPC status code of the failure rather than guessing.

- **Transient.** `Unavailable`, `Internal`, `DeadlineExceeded` and `Aborted`,
  a transport failure with no status (a reset connection, a timeout), and no
  answer within the three-second bound. The question is still open.
- **Definite.** A denial (`PermissionDenied`, `Unauthenticated`, or a refusal
  anywhere in the fault), a facade the cluster does not serve (`Unimplemented`),
  and any fault the Explorer does not recognise. These hide the area as before.
  A denial is definite even when other parts of the fault look transient.

After a transient fault the Explorer:

- **Keeps the area listed.** With no earlier answer for you, the area is shown
  as not checked, which cannot be opened. If this circuit already had a shown
  answer for you, that answer is kept and marked stale ("Last known: not
  rechecked yet."), so an area you were using a moment ago stays usable. An area
  that was hidden from you stays hidden: a fault never reveals one.
- **Checks again on its own,** after 2, 4, 8, 16, 30 and 60 seconds, and stops
  there. A definite answer ends the rechecks at once.
- **Offers to check again now.** Home, the spine and the area's own page show
  a status line - "Some areas could not be checked because the cluster did not
  answer." - with a **Check again** button, which asks every area at once and
  restarts the backoff.
- **Says so on Home.** An area's Home status line that could not be read shows
  its last line marked stale, or "Status could not be checked: the cluster did
  not answer.", instead of disappearing.

What is remembered belongs to the caller it was read for, and is forgotten on
sign-in, sign-out, a connection change or a tenant switch, like every other
per-circuit answer.

## This is not a security boundary

Availability is advisory. The cluster authorizes every call on its own,
whatever the Explorer chose to draw, and a visible area can still be refused an
individual operation. The areas treat a refusal as the cluster's final word and
say so where it happens.

Hiding what you cannot use serves you, not the cluster: it keeps the spine to the
places you can work in. An area shows only what the cluster returned to you.

## What each area checks

Each area answers from a probe against the facade it depends on. The probes read
as little as possible: typically one page of one item, or a capability check
against a placeholder name that is never read or written.

| Area | Hidden when | Unavailable when |
|---|---|---|
| Data | No state-API reader is available, or the caller is refused the catalogue. | Before a catalogue is loaded, a disconnected endpoint or another catalogue-read failure is Unavailable with a reason. A successfully loaded catalogue is memoized for the current caller and endpoint and can keep the area Visible until that memo is invalidated or refreshed. |
| Apps | The caller has neither a workspace, catalogue access nor permission to list installed apps, and every probe that failed failed definitely. | Never. A capability probe the cluster did not answer leaves an area that shows nothing not checked, and is not remembered. |
| Access | Neither cluster access administration nor delegated tenant access administration is served, or the caller is signed in and admitted by neither probe. | An anonymous caller is refused by the cluster-wide access probe: "Sign in to administer access on this cluster." A cluster probe the cluster did not answer is not checked, and is not remembered. |
| Schema | The schema facade is not served, a signed-in caller holds no schema capability, or the probe faults definitely. | An anonymous caller holds no schema capability: "Sign in to manage schema on this cluster." A probe the cluster did not answer is not checked. |
| Tenancy | Tenancy is off, the tenant self-service facade is not served, a signed-in caller has no tenant standing, or the probe faults definitely. | An anonymous caller has no tenant standing: "Sign in to see the tenants you administer." A probe the cluster did not answer is not checked. |
| Replication | Neither replication facade is served, or neither the peer status read nor a replication configuration with at least one tree admits the caller, and neither read met a transient fault. | Never. When neither read admits and either one met no answer, the area is not checked, and that read is not reused. |
| Backups | The backup control facade is not served, or the probe faults definitely. | The caller cannot list backups: "You do not hold a backup grant on this cluster. Ask an administrator for one." A probe the cluster did not answer is not checked. |
| Telemetry | The telemetry catalogue read fails definitely. | Never. A catalogue read the cluster did not answer is not checked, and is read again on the next ask. |
| Cluster | The tree administration facade is not registered, or the caller is refused. | The Explorer is disconnected, the cluster does not serve tree administration, or the probe did not answer. |

The areas themselves are described in [The Explorer areas](areas.md).

## Nothing else can add an area

The areas are compiled into the Explorer. There is no public API to register an
area, a completion source or a palette command, and no plugin model: an area
registration API would be a plugin API by another name. The only way a third
party puts a user interface into the Explorer is a
[Lattice App](lattice-apps.md), whose UI runs in a sandboxed frame with no
credential.

## See also

- [The Explorer navigation model](navigation-model.md)
- [The Explorer areas](areas.md)
- [Lattice Apps in the Explorer](lattice-apps.md)
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
