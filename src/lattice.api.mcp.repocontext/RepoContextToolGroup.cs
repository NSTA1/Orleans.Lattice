using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context tool module: the <see cref="ILatticeApiMcpToolGroup"/>
/// for <see cref="LatticeApiMcpGroup.RepoContext"/>. It plugs the companion
/// repository-context package into the <c>Orleans.Lattice.Api.Mcp</c> binding's
/// permission-aware discovery core so a caller holding a data read-or-write grant
/// is offered the <c>repocontext_*</c> tools scoped exactly like the built-in
/// groups (per-session discovery, region routing, and the fail-closed
/// authorization gate all come from the core seam - this module adds none of its
/// own).
/// </summary>
/// <remarks>
/// <para>
/// In the default single-repository mode the group contributes the always-on
/// read-only tools (<c>repocontext_health</c>, <c>repocontext_recall</c>,
/// <c>repocontext_scan</c>, <c>repocontext_list_topics</c>,
/// <c>repocontext_search</c>, <c>repocontext_index_status</c>,
/// <c>repocontext_neighbors</c>, and the structural-graph trio
/// <c>repocontext_outline</c>, <c>repocontext_changed</c>, and
/// <c>repocontext_related</c>) and - when the
/// host opts writes in - the mutating onboarding tool <c>repocontext_bootstrap</c>
/// together with <c>repocontext_remember</c>, <c>repocontext_update</c>, and
/// <c>repocontext_forget</c>. In workspace mode the read-only
/// <c>repocontext_list_repos</c> is added and the single-repository
/// <c>repocontext_bootstrap</c> is replaced by the mutating
/// <c>repocontext_add_repo</c> and <c>repocontext_remove_repo</c>.
/// </para>
/// <para>
/// The tool list is built <b>once</b> in the constructor, so the per-session
/// discovery filter selects from this prebuilt list and never re-materialises a
/// tool per <c>tools/list</c> or <c>tools/call</c>.
/// </para>
/// </remarks>
internal sealed class RepoContextToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the repository-context tool list once. When
    /// <paramref name="enableWrites"/> is <see langword="false"/> (the default)
    /// only the read-only tools are contributed; when <see langword="true"/> the
    /// mutating tools are added.
    /// <para>
    /// When <paramref name="workspaceMode"/> is <see langword="false"/> (the
    /// default) the surface targets a single, host-configured repository: the
    /// mutating onboarding tool is <c>repocontext_bootstrap</c>. When
    /// <see langword="true"/> the host mounts a broad parent workspace read-only
    /// and the client manages repositories dynamically: the read-only
    /// <c>repocontext_list_repos</c> is added, and <c>repocontext_bootstrap</c> is
    /// replaced by the workspace-scoped <c>repocontext_add_repo</c> and
    /// <c>repocontext_remove_repo</c>.
    /// </para>
    /// The tools resolve any collaborators from the request service provider at
    /// call time, so no per-session state is captured here.
    /// </summary>
    /// <param name="enableWrites">Whether the mutating repository-context tools are
    /// contributed.</param>
    /// <param name="workspaceMode">Whether the dynamic multi-repository workspace
    /// tools replace the single-repository onboarding tool.</param>
    public RepoContextToolGroup(bool enableWrites = false, bool workspaceMode = false)
    {
        var capacity = 12
            + (workspaceMode ? 1 : 0)
            + (enableWrites ? (workspaceMode ? 5 : 4) : 0);
        var tools = new List<McpServerTool>(capacity)
        {
            McpServerTool.Create(
                RepoContextToolHandlers.Health,
                new McpServerToolCreateOptions
                {
                    Name = "repocontext_health",
                    Title = "Repository-context health",
                    Description =
                        "Reports whether the Orleans.Lattice repository-context surface is registered and "
                        + "reachable for the current authenticated caller. Returns success only when the caller "
                        + "cleared the fail-closed authorization gate, so an agent can confirm the surface is "
                        + "wired end to end before attempting the capture, maintenance, and retrieval tools. "
                        + "Read-only.",
                    ReadOnly = true,
                    Destructive = false,
                    UseStructuredContent = true,
                }),
            BuildRecallTool(),
            BuildScanTool(),
            BuildListTopicsTool(),
            BuildSearchTool(),
            BuildIndexStatusTool(),
            BuildNeighborsTool(),
            BuildOutlineTool(),
            BuildChangedTool(),
            BuildRelatedTool(),
            BuildContextTool(),
            BuildStatsTool(),
        };

        if (workspaceMode)
        {
            tools.Add(BuildListReposTool());
        }

        if (enableWrites)
        {
            if (workspaceMode)
            {
                tools.Add(BuildAddRepoTool());
                tools.Add(BuildRemoveRepoTool());
            }
            else
            {
                tools.Add(BuildBootstrapTool());
            }

            tools.Add(BuildRememberTool());
            tools.Add(BuildUpdateTool());
            tools.Add(BuildForgetTool());
        }

        // Bracket every tool call with a timestamped start / completion line. The
        // decorator forwards name, schema, annotations, and metadata unchanged, so
        // the wrapped list is indistinguishable from the raw tools to discovery.
        for (var i = 0; i < tools.Count; i++)
        {
            tools[i] = new RepoContextToolInvocationLogger(tools[i]);
        }

        Tools = tools;
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.RepoContext;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static McpServerTool BuildRecallTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.RecallAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_recall",
                Title = "Recall a repository-context entry",
                Description =
                    "Fetches a single repository-context record by its full key - a structural node, a symbol, "
                    + "or an agent memory entry - and returns its flattened fields, tags, links, and remaining "
                    + "life. For a memory entry it also evaluates link staleness: each structural link (to a file "
                    + "or symbol) whose target's content digest has drifted since the link was made is reported "
                    + "through 'stale' and 'staleLinks'. A key with no live entry returns 'exists=false' so the "
                    + "caller can tell an absent or expired entry from an empty one. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildScanTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.ScanAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_scan",
                Title = "Scan a repository-context range",
                Description =
                    "Walks an ordered range of repository-context entries under a scope (all files, packages, "
                    + "or symbols; all memory; or the memory under one topic) and returns one page at a time with "
                    + "an opaque continuation token. Expired and tombstoned entries are never returned. Because a "
                    + "scan is a bulk read it does not evaluate each entry's time-to-live or memory link staleness, "
                    + "so the expiry fields ('expires', 'hasExpired', 'expiresAtUtc', 'remainingSeconds') and the "
                    + "staleness fields ('stale', 'staleLinks') are reported as null ('not evaluated'); call "
                    + "'repocontext_recall' on a key for its authoritative expiry and staleness. Use the "
                    + "returned token as the next call's 'continuationToken' to page through the whole range. "
                    + "Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildListTopicsTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.ListTopicsAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_list_topics",
                Title = "List repository memory topics",
                Description =
                    "Enumerates the distinct agent memory topics available for a repository, each with its live "
                    + "entry count, so an agent can discover what working memory, notes, and decisions have been "
                    + "captured before recalling or scanning them. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSearchTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.SearchAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_search",
                Title = "Search the repository-context store",
                Description =
                    "Finds the repository-context records most relevant to a natural-language query and returns "
                    + "them hydrated from the store of record, ranked best-first. When an embedding provider and "
                    + "vectors are available it runs an exact semantic (nearest-neighbour) search; otherwise it "
                    + "degrades to a deterministic BM25 keyword/structural scan over record names and file content, "
                    + "so a query always returns the best available matches instead of failing. The result's 'mode' "
                    + "reports which path answered ('semantic', 'keyword', or 'empty'). Every hit carries a "
                    + "machine-readable 'reasons' list (server-derived, deterministic, ordinal-ordered, bounded, and "
                    + "never null) explaining why it ranked: a semantic hit lists 'semantic', the matched chunk kind "
                    + "('chunk:symbol' or 'chunk:file'), and 'symbol:<fqName>' for a symbol vector; a keyword hit "
                    + "lists 'path-name-match', 'symbol:<fqName>', 'tag:<tag>', 'topic-match', 'content-match', and "
                    + "'key-match' for whichever projected fields the query terms hit. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildIndexStatusTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.IndexStatusAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_index_status",
                Title = "Inspect a repository indexing job",
                Description =
                    "Reports the progress of a repository's asynchronous indexing job: its status (none, "
                    + "running, completed, or failed), the phase it is executing (walking, reconciling, "
                    + "applying, or vectorising), the running file and chunk counters, the cumulative "
                    + "index-run count (a run-start tally that rises on every reconcile and back-fill, not a "
                    + "retry counter), and "
                    + "timing. Because onboarding runs in the background and survives a client disconnect or a "
                    + "host restart, poll this tool with the repository id to follow a long onboarding pass to "
                    + "completion. A repository that was never onboarded reports status 'none'. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildNeighborsTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.NeighborsAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_neighbors",
                Title = "Walk knowledge-linking edges",
                Description =
                    "Walks the typed knowledge-linking edges out of a memory entry and returns the adjacent "
                    + "entries, hydrated from the store of record, as a bounded breadth-first traversal. Follows "
                    + "each entry's link relations (for example 'broader', 'narrower', 'related', 'partOf') up to "
                    + "'depth' hops - optionally restricted to a single 'relation' - and stops once 'maxNodes' "
                    + "distinct neighbors have been collected, reporting 'truncated' when the cap was hit. A seed "
                    + "key with no live entry returns 'exists=false'; a dangling edge whose target has no live "
                    + "value is still returned with its own 'exists=false' so it is observable. Each walked memory "
                    + "entry has its link staleness evaluated ('stale' / 'staleLinks'), as 'repocontext_recall' "
                    + "does, so the walk surfaces which linked concepts point at drifted code. Use it to explore "
                    + "the curated concept graph an agent has captured across sessions. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildOutlineTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.OutlineAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_outline",
                Title = "Outline a file's declared symbols",
                Description =
                    "Returns the structural skeleton of one indexed file without reading its body: each symbol "
                    + "the file declares (its kind, signature, and 1-based start/end line span), ordered by "
                    + "position, plus the token cost of reading the whole file under the configured tokenizer "
                    + "profile. It is the cheapest way to grasp a file's shape and decide whether a full read is "
                    + "worth the tokens - prefer it over fetching the file when you only need to know what the "
                    + "file contains and where. The token count is read from the indexed per-file count when "
                    + "present and computed from the stored content projection otherwise; it is reported as null "
                    + "only when the file was never content-processed. A path with no stored file node returns "
                    + "'exists=false' so an absent file is distinguishable from an empty one. This is a pure read "
                    + "over stored records and never touches the workspace on disk. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildChangedTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.ChangedAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_changed",
                Title = "Report workspace drift from the index",
                Description =
                    "Reports how the current workspace has drifted from the stored index - the files added, "
                    + "updated, and removed - by comparing each file's content digest against the indexed digest, "
                    + "without invoking git, so it works in any checkout regardless of version-control state. It "
                    + "also lists the indexed files that depend on the changed ones (the reverse-reference impact "
                    + "set), so an agent can see the blast radius of a set of edits before re-indexing. The "
                    + "workspace is walked only through the fail-closed workspace boundary, so a supplied path "
                    + "that resolves outside the mounted workspace is refused. Use it to find what an index needs "
                    + "to catch up on, or to scope a review to what actually moved. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRelatedTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.RelatedAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_related",
                Title = "Find a file's structural neighbours",
                Description =
                    "Resolves the structural neighbourhood of one file so an agent can navigate the code graph "
                    + "without full-file reads: the type-names the file references (its outbound imports), the "
                    + "indexed symbols that reference the file's declarations (its inbound dependents, resolved to "
                    + "the files that declare them), and the test types that cover it (recorded from the "
                    + "'{Name}Tests' / '{Name}Test' naming convention). Dependents and tests are maintained "
                    + "incrementally from a reverse cross-reference projection, so the lookup is a bounded read "
                    + "rather than a whole-repository scan. Edges are keyed by simple (unqualified) type-name, a "
                    + "syntactic approximation: two distinct types sharing a simple name are not disambiguated. A "
                    + "path with no stored file node returns 'exists=false'. This is a pure read over stored "
                    + "records and never touches the workspace on disk. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildContextTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.ContextAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_context",
                Title = "Bundle budgeted context for a task",
                Description =
                    "Returns a ranked, explained bundle of source for a natural-language task, packed under a "
                    + "HARD token ceiling in a single call - collapsing the search -> recall -> read loop into one "
                    + "round trip that can never overrun your context budget. It searches the store for the task "
                    + "(semantic when an embedder and vectors are available, otherwise a degraded keyword scan - a "
                    + "keyword bundle is still returned), resolves the top hits to unique files, and packs each at a "
                    + "detail level under the budget: 'paths' (path only), 'outline' (declared-symbol skeleton, "
                    + "reusing the outline projection), or 'slices' (bounded body text). 'auto' (the default) packs "
                    + "the richest level that yields a non-empty bundle and reports the concrete level in 'detail'. "
                    + "Every entry carries its match 'reasons', its exact BPE 'tokenCount', and the whole-file "
                    + "'fullReadTokenCount'. The bundle's 'totalTokens' is the exact BPE sum and never exceeds "
                    + "'budgetTokens'. When even the cheapest entry does not fit, it FAILS CLOSED: 'entries' is "
                    + "empty and 'retryBudgetTokens' reports a budget guaranteed to admit at least one entry on a "
                    + "retry (null when the search matched nothing, so no larger budget would help). 'truncated' "
                    + "flags a bundle that dropped lower-ranked candidates. The 'top', 'responseBudgetTokens', and "
                    + "'detail' arguments are validated and clamped, never trusted to drive unbounded work. "
                    + "REUSE ECONOMICS: never pay twice for context you already hold. Each delivered unit (a path "
                    + "pointer, a body span, or an outline symbol) carries a stable opaque 'receipt', and each entry "
                    + "a per-version 'contentHash'. Hand receipts back in 'seen' to suppress exactly those units (the "
                    + "rest of the file still arrives), or assert 'known' whole-file possession as 'path@hash'. Pass a "
                    + "'session' to persist this bookkeeping across calls: the session auto-suppresses units it "
                    + "already delivered and validates 'known' claims. A whole-file claim is honoured ONLY for a "
                    + "version actually delivered as a complete body, so partial evidence can never be promoted to "
                    + "whole-file possession. Suppressed content is acknowledged in 'reused' and is never charged "
                    + "against 'top' or the token budget. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildStatsTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.Stats,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_stats",
                Title = "Repository-context usage summary",
                Description =
                    "Reports an aggregate summary of the repository-context surface's own usage over a bounded "
                    + "recent window, so a team can see whether the surface actually reduces context cost. Returns "
                    + "only summed token figures: how many calls were answered ('calls'), the exact response tokens "
                    + "they spent ('responseTokens'), the whole-file read tokens they conservatively replaced "
                    + "('readsReplacedTokens' - credited only for delivered whole-file-equivalent content, never for "
                    + "discovery or partial detail or content the caller already held), the net tokens saved "
                    + "('netSavedTokens'), and the window length ('windowSeconds'). It carries no body, query, path, "
                    + "or repository identity - aggregate figures only. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildBootstrapTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.BootstrapAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_bootstrap",
                Title = "Bootstrap a repository into the context store",
                Description =
                    "Onboards a codebase into the repository-context store so an agent starts from a "
                    + "populated, queryable baseline instead of empty memory. Walks the repository at "
                    + "'repoRoot', records a structural node and content digest for every file under the "
                    + "'repoId' keyspace, and reconciles the scan against the stored records. Idempotent and "
                    + "resumable: re-running on an unchanged repository is a no-op, a changed repository "
                    + "updates only changed files and prunes deleted ones, and an interrupted run resumes "
                    + "without duplication. Starts asynchronously and returns at once with the job's initial "
                    + "progress; the walk continues in the background and survives a client disconnect or a "
                    + "host restart, so poll 'repocontext_index_status' with the same 'repoId' to follow it to "
                    + "completion. Fails closed: offered only to a caller who cleared the authorization gate "
                    + "and for whom the host opted writes in. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildListReposTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.ListReposAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_list_repos",
                Title = "List registered repositories",
                Description =
                    "Lists every repository currently registered in the context store, each with its "
                    + "last-ingested marker and recorded file count, in ascending id order. Use it to discover "
                    + "which repositories under the mounted workspace are queryable before recalling, scanning, "
                    + "searching, or removing one. It enumerates committed, materialised structural records, "
                    + "which is a different source from the live progress counters 'repocontext_index_status' "
                    + "reports: a repository still in its first ingest can already show status 'Running' there "
                    + "while it is absent here - and not yet answering scan or search - until its structural "
                    + "records materialise, so treat 'repocontext_index_status' as the authority for an "
                    + "onboarding still in progress. The per-row marker and file count are likewise best-effort "
                    + "while an ingest runs. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildAddRepoTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.AddRepoAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_add_repo",
                Title = "Add a repository under the workspace",
                Description =
                    "Registers a repository under the mounted read-only workspace and starts indexing it, "
                    + "so it becomes queryable through recall, scan, and search. This is the workspace-mode "
                    + "onboarding tool - prefer it over any separate bootstrap step. Supply 'path' pointing at a "
                    + "repository under the workspace root; a path that resolves outside the workspace (via '..' "
                    + "or a symbolic link) is rejected. Omit 'repoId' to derive it from the final path segment. "
                    + "Walks the tree, records a structural node and content digest per file, and reconciles the "
                    + "scan against the store: idempotent and resumable, so re-adding an unchanged repository is "
                    + "a no-op and a changed one updates only what changed. Starts asynchronously and returns at "
                    + "once with the job's initial progress; the walk continues in the background and survives a "
                    + "client disconnect or a host restart, so poll 'repocontext_index_status' with the "
                    + "repository id to follow it to completion. Fails closed: offered only to a caller who "
                    + "cleared the authorization gate and for whom the host opted writes in. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRemoveRepoTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.RemoveRepoAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_remove_repo",
                Title = "Remove a repository from the context store",
                Description =
                    "Removes every record for a repository from the context store - its structural nodes, "
                    + "agent memory, and vector data - and drops it from 'repocontext_list_repos'. The working "
                    + "tree on disk is never touched; only the indexed context is forgotten. Removing an unknown "
                    + "repository is a no-op that reports zero deletions. Fails closed: offered only to a caller "
                    + "who cleared the authorization gate and for whom the host opted writes in. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRememberTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.RememberAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_remember",
                Title = "Remember a repository memory entry",
                Description =
                    "Creates or updates an agent memory or decision entry under a repository topic, with an "
                    + "optional time-to-live. Omit 'id' to create a new entry with a generated id; supply an "
                    + "existing 'id' to merge into it in place using CRDT semantics rather than a blind "
                    + "overwrite. When no explicit 'ttlSeconds' is given, a newly created entry inherits the "
                    + "repository's default memory TTL if one is configured, otherwise it is durable. Fails "
                    + "closed: offered only to a caller who cleared the authorization gate and for whom the host "
                    + "opted writes in. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildUpdateTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.UpdateAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_update",
                Title = "Update a repository-context record",
                Description =
                    "Patches scalar fields and tags on an existing structural or memory record using CRDT-merge "
                    + "semantics: each field is applied as a last-writer-wins register at a fresh logical tick and "
                    + "merged into the current record, so concurrent updates converge instead of clobbering each "
                    + "other. Any remaining time-to-live on the record is preserved. Fails if no record exists at "
                    + "the key. Fails closed: offered only to a caller who cleared the authorization gate and for "
                    + "whom the host opted writes in. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildForgetTool()
        => McpServerTool.Create(
            RepoContextToolHandlers.ForgetAsync,
            new McpServerToolCreateOptions
            {
                Name = "repocontext_forget",
                Title = "Forget a repository-context entry",
                Description =
                    "Removes a repository-context entry. By default it hard-deletes the entry immediately; set "
                    + "'lapse' to true to instead re-write it with a short time-to-live (default 60 seconds) so it "
                    + "lapses on its own, which lets concurrent readers drain gracefully. Fails closed: offered "
                    + "only to a caller who cleared the authorization gate and for whom the host opted writes in. "
                    + "Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });
}
