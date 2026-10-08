---
agent_spec: "docs/agents/capabilities.yaml"
---

# Area availability

The Explorer compiles in its native areas, but you only see the ones you can
use. Each area decides for itself, on every navigation, whether you may see it.
This page explains the three answers an area can give, why the Explorer fails
closed, and what each area checks.

## Three answers

Every area answers one question for the signed-in caller on the connected
cluster: may this caller see you, now?

| Answer | Spine and Home | Opening its address |
|---|---|---|
| Visible | A normal stop, optionally with a short badge such as a count. | The area's page. |
| Unavailable | A demoted stop showing a one-sentence reason, such as "Sign in to administer access on this cluster." | The area's name and the same reason under "Not available". |
| Hidden | No stop at all. | The not-found page, exactly as for an address that does not exist. |

**Unavailable** is for something you can act on or wait out: you are not signed
in, the Explorer is not connected, the cluster did not answer, or (in Backups)
you hold no grant. The reason says what would change the answer.

**Hidden** is for everything else: the cluster does not serve the facade the
area needs, the host did not register it, or you are signed in and the cluster
refused you. A hidden area is left out, not demoted, and its address renders the
not-found page, so the Explorer never confirms that an area you cannot use
exists.

## Failing closed

An area that cannot prove you may see it is not shown.

- **Time-boxed.** The Explorer asks every area in parallel and waits at most
  three seconds for each. A fault, a probe cancellation not caused by the
  navigation, or a timeout hides that area for the navigation. Cancelling the
  navigation itself propagates instead of being rendered as Hidden. One slow
  facade never stalls the spine.
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
| Apps | The caller has neither a workspace, catalogue access nor permission to list installed apps. | Never. |
| Access | Neither cluster access administration nor delegated tenant access administration is served, or the caller is signed in and admitted by neither probe. | An anonymous caller is refused by the cluster-wide access probe: "Sign in to administer access on this cluster." |
| Schema | The schema facade is not served, a signed-in caller holds no schema capability, or the probe faults. | An anonymous caller holds no schema capability: "Sign in to manage schema on this cluster." |
| Tenancy | Tenancy is off, the tenant self-service facade is not served, a signed-in caller has no tenant standing, or the probe faults. | An anonymous caller has no tenant standing: "Sign in to see the tenants you administer." |
| Replication | Neither replication facade is served, or the peer status read fails and there is no replication configuration with at least one tree. | Never. |
| Backups | The backup control facade is not served, or the probe cannot reach it. | The caller cannot list backups: "You do not hold a backup grant on this cluster. Ask an administrator for one." |
| Telemetry | The telemetry catalogue read fails for any reason. | Never. |
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
