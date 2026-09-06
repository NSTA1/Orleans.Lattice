# Bindings - example override

Copy this table into your repository's two agent override files
(`.github/agents/backlog-worker.agent.md` and `.github/agents/backlog-pm.agent.md`)
and fill in your own values. Nothing else in the template needs editing.

| Binding | Value |
|---------|-------|
| `{repoId}` | `my-repo` |
| `{owner}/{repo}` | `my-org/my-repo` |
| `{ghAccount}` | `my-github-account` |
| `{homeRegion}` | `uksouth` |
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
- **`{homeRegion}`** is the region claims are taken in. Claims are region-scoped
  by enforcement, not by convention: a claim taken in one region refuses a write
  from another. A single-region deployment still needs a value, and every item's
  `homeRegion:` tag must match it.

## Why bindings rather than editing the template

The template files are the source of truth and are consumed unmodified. Keeping
every repository-specific value in one small override means:

- upgrading to a newer template is a file replace, not a merge;
- a diff of your override shows your entire deviation from the base;
- the template cannot accumulate one repository's assumptions.

If a binding is unavailable at runtime the agents **stop and report** rather
than guessing. That is deliberate: a `gh` call under the wrong identity and a
claim taken in the wrong region are both expensive to unpick after the fact.
