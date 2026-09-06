# Adopting the agent-operated backlog

A copyable template for running an **agent-operated backlog** in your own
repository: a durable work queue held in `repocontext` memory, drained
concurrently by agent sessions under fenced claims, and mirrored to GitHub
issues so a human keeps oversight and control of admission.

This directory is the **base**. Your repository supplies a small override with
its own values; nothing here is edited. Orleans.Lattice itself adopts the
template this way, so the base is exercised continuously rather than kept in
step by hand.

## What is here

| File | Role |
|------|------|
| [`backlog-protocol.md`](backlog-protocol.md) | The data model: item schema, relation vocabulary, grouping model, ready-set computation, mirroring, entry gating. |
| [`backlog-worker.base.md`](backlog-worker.base.md) | Generic worker behaviour: claim one item, do the work, complete or release. |
| [`backlog-pm.base.md`](backlog-pm.base.md) | Generic project-manager behaviour: curate, decompose, mirror, deploy workers. |
| [`bindings.example.md`](bindings.example.md) | The override table to copy, and how to obtain each value. |

## 1. Prerequisites

The claim surface ships in the `Orleans.Lattice.Api.Mcp.RepoContext` package and
is exposed as MCP tools, not as a public C# API. Register it **with writes
enabled**:

```csharp
builder.AddRepoContextTools(enableWrites: true);
```

Without `enableWrites` the mutating tools are not contributed at all, so
`repocontext_claim`, `repocontext_renew_claim` and `repocontext_release_claim`
will not appear in the tool list. That is the fail-closed gate working, not a
fault. `repocontext_claim_status` is read-only and is always contributed.

Your repository must also be indexed, so that `repocontext_list_repos` reports a
`repoId`. See the [container quickstart](../../../docs/lattice.api.mcp.repocontext/container.md).

## 2. Copy the template

Copy this directory into your repository. Keeping it at
`samples/AgentBacklog/template/` means the relative links below resolve
unchanged; any other location works if you fix the paths in your override.

## 3. Write the two overrides

Create `.github/agents/backlog-worker.agent.md` and
`.github/agents/backlog-pm.agent.md`. Each needs only front matter, a pointer to
its base, the bindings table from [`bindings.example.md`](bindings.example.md),
and any rules genuinely specific to your repository:

```markdown
---
name: Backlog Worker
description: <one-line description used for agent discovery>
---

Your behaviour is defined in `samples/AgentBacklog/template/backlog-worker.base.md`
and the data model in `samples/AgentBacklog/template/backlog-protocol.md`.
Read both now, before you do anything else.

## Bindings

| Binding | Value |
|---------|-------|
| `{repoId}` | `my-repo` |
...
```

Orleans.Lattice's own overrides are working examples:
[`backlog-worker.agent.md`](../../../.github/agents/backlog-worker.agent.md) and
[`backlog-pm.agent.md`](../../../.github/agents/backlog-pm.agent.md). Each is
under 70 lines, which is the whole point: that is the complete deviation from
the base.

## 4. Add the always-on memory rules

Two protocol rules bind **every** agent that touches memory, not only backlog
agents, because an agent auditing or tidying memory will not have read the
protocol. Put them in a file that always applies (for Copilot, an
`.instructions.md` with `applyTo: "**"`):

- **The backlog relations must never be pruned.** `blockedBy`, `anchoredTo`,
  `claims`, `integrates` and `informs` extend the knowledge-linking vocabulary.
  Tooling that audits memory must recognise them, or it will prune a `blockedBy`
  edge and silently release work that was deliberately gated.
- **Never set a TTL on a backlog item.** Expiry is silent and unlogged, so a
  lapsed item that others declare `blockedBy` starves its dependents invisibly.
  Retire items with `forget` instead.

Orleans.Lattice keeps these in
[`.github/instructions/repocontext.instructions.md`](../../../.github/instructions/repocontext.instructions.md),
section `## The agent-operated backlog`, which is otherwise a pointer to this
template.

## 5. GitHub-side setup

No file copy can do these, and the backlog does not work without them.

**Labels.** Create two labels on the mirror repository:

```bash
gh label create needs-specification --description "Agent-proposed backlog item awaiting human admission"
gh label create stale --description "Parked backlog item; exceeded the attempt threshold"
```

`needs-specification` is the **entry gate**: an agent-authored item carries it
and is excluded from the ready set until a human removes it. Without the label
the gate silently passes and agents pick their own work. `stale` parks poison
items so a failing item does not burn a session per tick.

**Issue types and sub-issues.** Epics mirror as GitHub issues with native
sub-issues. No configuration is needed, but the PM agent uses
`gh api repos/{owner}/{repo}/issues/<n>/sub_issues`, which requires a token with
issue write scope.

**CI on epic branches.** An epic uses one shared `<type>/epic/<slug>` branch that
sub-item pull requests target. Extend your workflow triggers so those pull
requests are still gated:

```yaml
on:
  pull_request:
    branches: [main, '*/epic/**']
```

Without the second pattern a pull request into an epic branch runs **no checks
at all**. Do not put branch protection on the epic branch itself, and especially
not a required check with `strict` (up to date before merging): that is exactly
what serialises pull requests and what the epic branch exists to avoid.

## 6. Run a worker

A worker is one agent session. Start it on a schedule, or deploy it from the PM
agent. It computes the ready set itself and takes its own claim; a dispatch may
narrow *where* it looks, but must never hand it a pre-selected item, because the
interchangeability is what makes two contending workers resolve to exactly one
proceeding.

Set the schedule's concurrency where workers are *started*. A worker cannot
count its siblings: it has no reverse index, so it will exit cheaply when every
candidate's blast radius overlaps in-flight work rather than widening its own
overlap tolerance.

## 7. Verify

- `repocontext_list_repos` reports your `{repoId}`.
- `repocontext_claim_status` on any key returns `exists: false` rather than an
  error, confirming the read-only tool is contributed.
- `repocontext_claim` on a real memory key returns `granted: true` with a
  `fencingToken`, confirming writes are enabled.
- An unfenced `repocontext_update` against that claimed key is **refused**. If it
  succeeds, the fence is not being enforced and the backlog is not safe to drain
  concurrently.

The [walkthrough](../README.md) steps through exactly that sequence.

## See also

- [The agent-operated backlog](../../../docs/lattice.api.mcp.repocontext/backlog.md) - the design rationale
- [Tools](../../../docs/lattice.api.mcp.repocontext/tools.md) - the claim surface reference
- [Agent backlog walkthrough](../README.md) - the claim and fence, observed end to end
