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
/// The group contributes a read-only <c>repocontext_health</c> probe that proves
/// the registration and the authorization gate work end to end, and - when the
/// host opts writes in - the mutating <c>repocontext_bootstrap</c> onboarding tool
/// that ingests a codebase into the context store. The capture, maintenance, and
/// retrieval tools land in later work.
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
        var capacity = 5
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
                    + "life. A key with no live entry returns 'exists=false' so the caller can tell an absent or "
                    + "expired entry from an empty one. Read-only.",
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
                    + "an opaque continuation token. Expired and tombstoned entries are never returned. Use the "
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
                    + "degrades to a deterministic keyword/structural scan, so a query always returns the best "
                    + "available matches instead of failing. The result's 'mode' reports which path answered "
                    + "('semantic', 'keyword', or 'empty'). Read-only.",
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
                    + "without duplication. Returns a summary of files scanned, added, updated, removed, and "
                    + "unchanged, symbols captured, and elapsed time. Fails closed: offered only to a caller "
                    + "who cleared the authorization gate and for whom the host opted writes in. Destructive.",
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
                    + "searching, or removing one. Read-only.",
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
                    "Registers a repository under the mounted read-only workspace and ingests it in one call, "
                    + "so it becomes queryable through recall, scan, and search. This is the workspace-mode "
                    + "onboarding tool - prefer it over any separate bootstrap step. Supply 'path' pointing at a "
                    + "repository under the workspace root; a path that resolves outside the workspace (via '..' "
                    + "or a symbolic link) is rejected. Omit 'repoId' to derive it from the final path segment. "
                    + "Walks the tree, records a structural node and content digest per file, and reconciles the "
                    + "scan against the store: idempotent and resumable, so re-adding an unchanged repository is "
                    + "a no-op and a changed one updates only what changed. Returns a summary of files scanned, "
                    + "added, updated, removed, and unchanged. Fails closed: offered only to a caller who cleared "
                    + "the authorization gate and for whom the host opted writes in. Destructive.",
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
