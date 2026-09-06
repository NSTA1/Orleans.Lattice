# AgentBacklog - claiming, fencing, and releasing backlog work

A runnable walkthrough of the claim and lease surface that makes an
**agent-operated backlog** safe for several agents to drain at once.

Where the [DistributedLock](../DistributedLock/README.md) sample shows the lock
primitive in isolation, this one shows what it buys you once it guards a shared,
concurrently-drained work queue: a claim that expires instead of stranding an
item, and a fence that a superseded worker cannot write through.

The claim surface is exposed as Model Context Protocol tools rather than a public
C# API, so this sample drives it the way an agent does - by calling
`repocontext_*` tools against a running host. It reuses the
[RepoContextContainer](../RepoContextContainer/README.md) box rather than
standing up a second one.

Background on the design is in
[The agent-operated backlog](../../docs/lattice.api.mcp.repocontext/backlog.md).

To adopt the backlog in your own repository, the copyable base protocol and
agent definitions are in [`template/`](template/README.md). This repository
consumes them unmodified, through two small binding overrides.

## What you will observe

1. A claim is granted with a **fencing token** and a bounded lease.
2. While the claim is live, an **unfenced write is refused** - the claim is not
   advisory.
3. A second worker claiming after the lease lapses gets a **strictly higher**
   token.
4. The first worker, now superseded, is **refused** even though it still holds
   its token and still believes it owns the item. This is the case a boolean
   "claimed" flag cannot handle.
5. Releasing readmits ordinary unfenced writes.

## Prerequisites

Start the container box and set `REPO_PATH`, exactly as in the
[RepoContextContainer](../RepoContextContainer/README.md) sample:

```bash
cd ../RepoContextContainer
REPO_PATH=/absolute/path/to/a/repo docker compose up -d
```

The host must be running with writes enabled (`AddRepoContextTools(enableWrites: true)`),
which the container sample's default configuration does. Without it the mutating
tools are not contributed at all, and you will not see them in the tool list -
that is the fail-closed gate working, not a fault.

Every step below is a tool call. Issue them from any MCP client pointed at the
host, or from the agent session you are running.

## 1. Create an item and claim it

Backlog items are ordinary memory records, so the item is created with
`repocontext_remember`. The id is deterministic and derived from the mirrored
issue number - never a generated GUID - so a retry merges in place.

```text
repocontext_remember(
  repoId: "lattice", topic: "backlog", id: "issue-2101",
  kind: "Note", author: "backlog-pm",
  title: "Batch the shipper poll against the new seam",
  body: "Spec: https://github.com/NSTA1/Orleans.Lattice/issues/2101",
  tags: ["backlog", "priority:P1", "phase:implementation",
         "homeRegion:uksouth", "baseBranch:feat/epic/wal-batching"])

repocontext_claim(key: "repo/lattice/mem/backlog/issue-2101",
                  owner: "worker-a", leaseSeconds: 60)
```

The claim returns `granted: true` with a `fencingToken` and a
`leaseExpiresAtUtc`. Note what it does **not** do on contention: a claim that
loses a race returns `granted: false` with a reason of `contended`, `timeout`, or
`missing`. Losing is an ordinary outcome, so a worker branches on the result
rather than catching an exception.

## 2. The claim excludes unfenced writes

With the claim live, write the resume block the way a careless caller would -
without presenting the token:

```text
repocontext_update(key: "repo/lattice/mem/backlog/issue-2101",
                   fields: { "body": "started" })
```

This is **refused** with `RepoContextClaimConflictException`. That refusal is the
whole point: an item's `body` is a last-writer-wins register, and it is only safe
to hold a resume block there because the fence guarantees a single writer.

Present the token and the same write succeeds:

```text
repocontext_update(key: "repo/lattice/mem/backlog/issue-2101",
                   fields: { "body": "branch feat/epic/wal-batching/issue-2101, seam wired, tests pending" },
                   fencingToken: <token from step 1>)
```

## 3. Let the lease lapse and claim as a second worker

Wait out `leaseSeconds` without calling `repocontext_renew_claim`, simulating a
worker whose session was stopped. This is the **normal** path, not an exceptional
one: scheduled agent sessions are stopped mid-flight often enough that a claim
must never be a flag a dead session leaves set.

```text
repocontext_claim(key: "repo/lattice/mem/backlog/issue-2101",
                  owner: "worker-b", leaseSeconds: 60)
```

Granted, with a **strictly higher** fencing token. The lock never reuses a token,
across activations or crashes.

## 4. The superseded worker is refused

Now have the first worker do what a partitioned or paused process does - carry on
as though nothing happened:

```text
repocontext_update(key: "repo/lattice/mem/backlog/issue-2101",
                   fields: { "body": "worker-a still thinks it owns this" },
                   fencingToken: <token from step 1>)
```

**Refused.** The record keeps its fence as a high-water mark, and a token below it
can never write again - not by a direct write, and not by a concurrent replica
merge, because the mark is stored keyed by the token itself and so behaves as a
maximum.

`worker-a` can also discover this without guessing, by renewing:

```text
repocontext_renew_claim(key: "repo/lattice/mem/backlog/issue-2101",
                        fencingToken: <token from step 1>)
```

A reason of `superseded` is the authoritative "you have lost this item" signal. A
worker seeing it abandons immediately and writes nothing further.

## 5. Release

```text
repocontext_release_claim(key: "repo/lattice/mem/backlog/issue-2101",
                          fencingToken: <token from step 3>)
```

Release is idempotent, and it readmits ordinary unfenced writes. It only raises
the released marker; it never lowers the fence, so a superseded holder stays
superseded after the item is free again.

## A note on `repocontext_claim_status`

`repocontext_claim_status` reports the live claim, the queue depth, and the lease
expiry - and its `authoritative` property is hard-wired to `false`. That is
deliberate: a status read is a snapshot that can be stale by the time you act on
it, so no call site can project an authoritative decision from it. Use it to
observe and report. Only a granted claim or a renew verdict is authoritative.

## What this does not show

Region scoping. Claims are region-scoped, so a claim taken in one region refuses
a write from another, which is why an item carries a `homeRegion:` tag. Observing
it needs a multi-region deployment; see
[Cross-cluster replication](../CrossClusterReplication/README.md).

## See also

- [Adopting the agent-operated backlog](template/README.md)
- [The agent-operated backlog](../../docs/lattice.api.mcp.repocontext/backlog.md)
- [Tools](../../docs/lattice.api.mcp.repocontext/tools.md)
- [Distributed lock](../../docs/lattice/distributed-lock.md)
