# The agent-operated backlog

An **agent-operated backlog** lets a fleet of agents pick up, execute, and close
out work on this repository with the product owner steering rather than
dispatching. Work items live as repository-context memory entries, mirrored to
GitHub issues for human oversight; scheduled or on-demand **worker** agents drain
them; a **project manager** agent curates them and is the human's single point of
contact.

This page is the operator's view: what the mechanism is, what it guarantees, and
where each part is defined. It does not restate the data model or the agent
protocols, both of which live in the repository's own instruction files and would
drift if copied here.

| Concern | Defined in |
| --- | --- |
| Item schema, relation vocabulary, ready-set algorithm, grouping model, mirroring, gating | `.github/instructions/repocontext.instructions.md`, section *The agent-operated backlog* |
| Worker behaviour | `.github/agents/backlog-worker.agent.md` |
| Project-manager behaviour | `.github/agents/backlog-pm.agent.md` |
| Epic branch convention and CI trigger | `.github/copilot-instructions.md` |
| Claim and lease tools | [Tools](tools.md) |

## Why a claim needs a fence

The backlog is a shared, concurrently-drained queue held in a CRDT store, and
that combination has a specific hazard. Repository-context memory records merge
without coordination: scalar fields (`title`, `body`, `author`, `provenance`) are
last-writer-wins registers, so two concurrent writers silently lose one write,
while `tags` and `links` are add-wins sets, so two concurrent writers both
survive. Nothing in the surface offers compare-and-swap: `repocontext_update`
preconditions on record *existence* only, never on value.

So an edge asserting "this run owns this item" is an audit record of who tried,
not a lock. Two workers can both assert it and both believe they won.

Worse, the natural fix - a boolean "claimed" flag - fails in exactly the case
that matters. A scheduled agent session can be stopped at any instant, and in
practice a meaningful fraction of them are. A flag set by a killed session stays
set forever, and the item is stranded with no way to tell a live owner from a
dead one.

The claim surface addresses both by wrapping the cluster-wide
[distributed lock](../lattice/distributed-lock.md) rather than reimplementing
mutual exclusion:

- **Exclusion and fairness** come from the lock, which is FIFO-fair across the
  cluster.
- **Liveness** comes from the lock's bounded, expiry-reclaimed lease. A claim is
  never a flag; it always expires. A worker that dies mid-item releases it by
  doing nothing, and `Claimed -> Ready` on lease expiry is the normal path, not an
  exception.
- **Safety after a handover** comes from the lock's monotonic fencing token,
  which strictly increases and is never reused across activations or crashes.

## The fence is enforced, not advertised

A fencing token that only the well-behaved consult is decoration. The
repository-context store therefore checks the token **on the write path itself**:
`repocontext_remember`, `repocontext_update`, and `repocontext_forget` each take
an optional fencing token, and each is refused when the token does not admit the
write. Refusal raises `RepoContextClaimConflictException`.

The admission rules, in order:

| Record state | Token presented | Outcome |
| --- | --- | --- |
| Never claimed | any, or none | **Accepted.** Every pre-existing caller is unchanged. |
| Claim live | none | **Refused** - a live claim excludes unfenced writes. |
| Claim live or released | below the record's high-water mark | **Refused** - a superseded holder can never write. |
| Claim released | at or above the high-water mark | **Refused** - re-claim first. |
| Claim live, token current, different region | current token | **Refused** - claims are region-scoped. |
| Claim live, token current, same region | current token | **Accepted.** |

Two consequences are worth stating plainly, because collapsing them is how
"fenced" gets misread as "true":

- The fence guarantees **authorship**: the resume block in an item's `body` was
  written by the live claim holder and cannot have been overwritten by a
  superseded one. That is what makes an LWW register safe here - not convention,
  but enforcement, because the fence serialises the writers.
- The fence guarantees nothing about **content**. A resume note is the last
  attempt's own account of itself. A resuming worker re-decides from it and never
  continues blindly.

The high-water mark is stored as a bounded register keyed by the token itself, so
it behaves as a join-semilattice maximum: no lower token can displace it, whether
by a direct write or by a concurrent replica merge. That is how a trustworthy
high-water mark is obtained without compare-and-swap. Claim state sits outside
the settable-scalar allow-list, so `repocontext_update` cannot forge it.

Claims apply to **memory records only**, which is the family the write-path check
guards. A token presented against any other record family is rejected rather than
silently ignored.

## Reading a claim decision

The claim tools report contention rather than throwing, so a worker branches on
the result instead of catching an exception:

```csharp verify
using Orleans.Lattice.Api.Mcp.RepoContext;

static string Describe(RepoContextClaimResult claim) =>
    claim switch
    {
        { Granted: true, FencingToken: { } token } =>
            $"claimed {claim.Key} with fencing token {token}, lease to {claim.LeaseExpiresAtUtc}",

        // Losing a race is an ordinary outcome, not a fault: another worker holds
        // the lock, or the wait elapsed, or the record does not exist.
        _ => $"not claimed ({claim.Reason}); pick another item",
    };
```

`repocontext_renew_claim` returns the same shape, and a reason of `superseded` is
the authoritative signal that this run has lost the item: it must abandon
immediately without writing anything further.

`repocontext_claim_status` is **advisory only**. Its `authoritative` property is
hard-wired to `false` so that no call site can project an authoritative status
from a read. Use it to observe and report; never to gate a decision. Only a
granted claim or a renew verdict is authoritative.

## Grouping, and why an epic gets one branch

Items are grouped: an epic and the sub-items joined to it by `partOf` edges. A
grouping runs in up to three phases - an optional research phase that may never
nest inside itself, an implementation phase that fans out behind a small seam
item, and an integration phase.

The integration phase exists because maximum parallelism concentrates risk at the
join. N pull requests, each green in isolation against a different base, none ever
tested against the others, can satisfy every sub-item's acceptance criteria while
failing the epic's. Every grouping therefore terminates in exactly one integration
item that is blocked by the whole fan-out, runs the full cross-package suite
rather than per-item targeted runs, verifies against the *epic's* criteria, and
holds an exclusive claim with the grouping's other workers quiesced. A grouping is
not complete until it completes, however green its sub-items are.

Groupings also share **one branch**. Because `main` requires a status check with
"up to date before merging", every merge into `main` invalidates every other open
pull request, which must then update and re-run the whole suite: N concurrent
sub-items cost quadratic CI. So an epic gets `<type>/epic/<slug>`, sub-items nest
under it as `<type>/epic/<slug>/<item>`, and the epic reaches `main` as a single
fully-gated pull request. CI runs on epic-targeted pull requests, and an epic
branch deliberately carries no protection - CI *running* is what gives feedback,
while a *strict* required check is what serialises. The full rules, including who
keeps the branch current with `main`, are in `.github/copilot-instructions.md`.

## Oversight stays with the human

Two stores, one source of truth each, and neither copies the other's content:
GitHub owns identity, specification, priority, audit trail, and notification;
repository-context memory owns the dependency graph, code anchors, claims, and
resume pointers.

Every item is mirrored to a GitHub issue at creation and takes its id from that
issue, so nothing can be enqueued invisibly. An item an agent proposed
additionally opens carrying the existing `needs-specification` label and stays out
of the ready set until a human removes it - and an agent never removes that label
from its own item. A human can reprioritise or respecify without an agent in the
loop, because the thing they edit is the thing that is authoritative.

Linking an item to the code it concerns (`anchoredTo`) captures those targets'
content digests, so an item whose code has drifted is reported `stale` and
re-validated before a run is spent on it. That is the one capability the issue
tracker cannot provide, and it doubles as the mitigation for items that would
otherwise fail repeatedly against a spec the code has outgrown.

## See also

- [Tools](tools.md) - the claim and lease tool contracts.
- [Record model](record-model.md) - record families and the CRDT store-of-record model.
- [Memory and TTL](memory-and-ttl.md) - topics and per-entry expiry. Note that a
  backlog item never carries a TTL: expiry is silent, so a lapsed item starves its
  dependents with no event to explain it.
- [Distributed lock](../lattice/distributed-lock.md) - the fencing and lease
  primitive the claim surface wraps.
- [Agent backlog sample](../../samples/AgentBacklog/README.md) - a runnable
  walkthrough of claiming, fencing, and release.
- [Adopting the backlog](../../samples/AgentBacklog/template/README.md) - the
  copyable base protocol and agent definitions, plus the GitHub-side setup no
  file copy can do. This repository consumes that template unmodified.
