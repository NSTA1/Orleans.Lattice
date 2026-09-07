# Bindings - example override

Copy this table into your repository's two agent override files
(`.github/agents/backlog-worker.agent.md` and `.github/agents/backlog-pm.agent.md`)
and fill in your own values. Nothing else in the template needs editing.

| Binding | Value |
|---------|-------|
| `{repoId}` | `my-repo` |
| `{owner}/{repo}` | `my-org/my-repo` |
| `{ghAccount}` | `my-github-account` |
| `{homeRegion}` | `local` (derive it - see below; do not copy this) |
| `{conventionsDoc}` | [`.github/copilot-instructions.md`](../../../.github/copilot-instructions.md) |
| `{implementationAgent}` | `feature-dev`, or omit if you have no implementation agent |

## How to get each value

- **`{repoId}`** is what `repocontext_list_repos` reports, which defaults to the
  final path segment of the *indexed* path. It is **not** your current working
  directory. In a git worktree it is the base repository's id, so
  `repocontext_list_repos` is the only reliable way to read it. Guessing it from
  your directory is the single most common adoption mistake.
- **`{owner}/{repo}`** is the GitHub repository that mirrors backlog items as
  issues. It does not have to be the repository the code lives in, but the
  agents assume one of each.
- **`{ghAccount}`** is the account every `gh` call authenticates as. Set it
  explicitly rather than relying on an ambient token, which is often not the
  identity you intend.
- **`{homeRegion}`** is the region claims are taken in. **Derive it from your
  own cluster; do not copy a value from this table.** Call
  `lattice_list_regions` to see the regions the server routes to, and
  `repocontext_claim_status` on any claimed item to see the region a claim
  actually records - that recorded value is what the tag must match. On a
  single-region deployment these commonly differ in a way that catches people
  out: the only routable id is `current`, while claims record `local`.

  Region scoping is **a property of your deployment, not of this tag**. The
  intent is that a claim taken in one region refuses a write from another, and
  that does hold where the cluster genuinely spans regions. Where it does not,
  a geographic value such as `uksouth` is **not enforced at all** - it is not a
  region the cluster routes, every worker's region check passes vacuously, and
  a claim from anywhere succeeds. That failure is worse than a no-op, because
  the protocol documents the guarantee as enforced and workers rely on it.
  Treat a value your cluster does not route as a **defect in the binding** and
  fix it here.

## Why bindings rather than editing the template

The template files are the source of truth and are consumed unmodified. Keeping
every repository-specific value in one small override means:

- upgrading to a newer template is a file replace, not a merge;
- a diff of your override shows your entire deviation from the base;
- the template cannot accumulate one repository's assumptions.

If a binding is unavailable at runtime the agents **stop and report** rather
than guessing. That is deliberate: a `gh` call under the wrong identity and a
claim taken in the wrong region are both expensive to unpick after the fact.
