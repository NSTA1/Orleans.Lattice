using System.ComponentModel;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The adapter behind the repository-context tool module. It exposes the
/// read-only <c>repocontext_health</c> probe - which proves the module is
/// registered and that the caller cleared the fail-closed authorization gate -
/// the mutating <c>repocontext_bootstrap</c> onboarding tool that ingests a
/// codebase into the context store, and the day-to-day capture, maintenance, and
/// retrieval tools: the read-only <c>repocontext_recall</c>, <c>_scan</c>, and
/// <c>_list_topics</c>, and the mutating <c>repocontext_remember</c>,
/// <c>_update</c>, and <c>_forget</c>.
/// </summary>
/// <remarks>
/// The health result is invariant, so it is built once and reused on every call:
/// the probe adds no per-invocation allocation to the hot path. The bootstrap
/// handler resolves its coordinator from the request service provider and adds no
/// authorization path of its own - the fail-closed gate that advertises the
/// mutating tool only to a write-opted-in caller is inherited from the discovery
/// core.
/// </remarks>
internal static class RepoContextToolHandlers
{
    /// <summary>
    /// The single, shared health result. It carries no caller- or
    /// request-specific state, so one immutable instance serves every session and
    /// no allocation occurs per <c>tools/call</c>.
    /// </summary>
    private static readonly RepoContextHealthResult Healthy = new()
    {
        Available = true,
        Group = LatticeApiMcpGroupCapabilityMap.DisplayName(LatticeApiMcpGroup.RepoContext),
        Status = "The Orleans.Lattice repository-context MCP surface is registered and reachable.",
    };

    /// <summary>
    /// Reports that the repository-context surface is available to the caller.
    /// Reaching this handler means the caller was advertised the tool and cleared
    /// the authorization gate, so it always returns the ready result.
    /// </summary>
    /// <returns>The shared, immutable health result.</returns>
    public static RepoContextHealthResult Health() => Healthy;

    /// <summary>
    /// Reports an aggregate roll-up of the repository-context surface's usage over a bounded
    /// recent window: how many calls were answered, the exact response tokens they spent, the
    /// whole-file read tokens they conservatively replaced, and the net tokens saved. Read-only
    /// and behind the fail-closed authorization gate; it returns only summed token figures and
    /// never any body, query, path, or repository identity.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the usage recorder.</param>
    /// <returns>The aggregate usage summary.</returns>
    public static RepoContextStatsResult Stats(RequestContext<CallToolRequestParams> context)
    {
        var recorder = ResolveUsageRecorder(context);
        return RepoContextStatsResult.From(recorder.Summarize(), recorder.Window);
    }

    /// <summary>
    /// Starts (or re-attaches to) an asynchronous indexing job for a repository:
    /// walks the tree, records a structural node and content digest per file, and
    /// reconciles the scan against the stored records idempotently (unchanged files
    /// are skipped, changed files updated, and deleted files pruned). Reaching this
    /// handler means the caller cleared the fail-closed authorization gate and the
    /// host opted writes in.
    /// <para>
    /// The call returns as soon as the job is durably recorded and handed to the
    /// background runner - it does not wait for the walk to finish - so a dropped
    /// MCP stream can never abort the index. Poll <c>repocontext_index_status</c>
    /// with the returned <c>repoId</c> to follow the run to completion.
    /// </para>
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the workspace
    /// guard and the job grain from the request service provider.</param>
    /// <param name="repoRoot">The absolute path to the repository working tree the
    /// server should walk.</param>
    /// <param name="repoId">The stable repository identity records are filed under.</param>
    /// <param name="includeGlobs">Optional include globs; when non-empty a file is
    /// ingested only if it matches at least one.</param>
    /// <param name="excludeGlobs">Optional exclude globs; a match removes a file
    /// from the walk even when it also matched an include.</param>
    /// <param name="respectGitignore">When <see langword="true"/> (the default), the
    /// tree's <c>.gitignore</c> files are honoured so ignored files and directories
    /// are not ingested.</param>
    /// <param name="excludeBinary">When <see langword="true"/> (the default), files
    /// that look binary (a NUL byte in their leading bytes) are dropped so compiled
    /// artefacts, images, and other blobs are not ingested.</param>
    /// <returns>The progress snapshot at acceptance, with the job running.</returns>
    /// <exception cref="McpException">A required argument is missing, the repository
    /// root resolves outside the workspace, or it does not exist (caller errors).</exception>
    public static Task<RepoIndexProgress> BootstrapAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Absolute path to the repository working tree the server should walk and ingest.")]
        string repoRoot,
        [Description("Stable repository identity to file records under, so re-ingesting the same codebase updates the same records.")]
        string repoId,
        [Description("Optional include globs (for example 'src/**' or '*.cs'); when non-empty a file is ingested only if it matches at least one. '**' matches any depth, '*' a single path segment.")]
        IReadOnlyList<string>? includeGlobs = null,
        [Description("Optional exclude globs; a match removes a file even when it also matched an include. The '.git' directory is always skipped.")]
        IReadOnlyList<string>? excludeGlobs = null,
        [Description("When true (the default), the tree's .gitignore files are honoured so ignored files and directories are not ingested. Set false to ingest every file regardless of ignore rules.")]
        bool respectGitignore = true,
        [Description("When true (the default), files that look binary (a NUL byte in their leading bytes) are dropped so compiled artefacts, images, and other blobs are not ingested. Set false to ingest binary files too.")]
        bool excludeBinary = true)
    {
        if (string.IsNullOrWhiteSpace(repoRoot))
        {
            throw new McpException("The 'repoRoot' parameter is required and must be a non-empty path.");
        }

        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        return StartIndexAsync(context, repoRoot, repoId.Trim(), includeGlobs, excludeGlobs, respectGitignore, excludeBinary);
    }

    /// <summary>
    /// Fetches a single repository-context entry by its full key and projects it,
    /// evaluating memory link staleness (the target-digest drift surfaced through
    /// <c>stale</c> / <c>staleLinks</c>) for a memory record. A key with no live
    /// entry projects with <c>exists=false</c>.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="key">The full repository-context key to recall.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The projected entry view.</returns>
    /// <exception cref="McpException">The key is missing or malformed.</exception>
    public static Task<RepoContextEntryView> RecallAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The full repository-context key to fetch, for example 'repo/{repoId}/file/{path}' or 'repo/{repoId}/mem/{topic}/{id}'.")]
        string key,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new McpException("The 'key' parameter is required and must be a non-empty repository-context key.");
        }

        return ResolveStore(context).RecallAsync(key, evaluateStaleness: true, cancellationToken);
    }

    /// <summary>
    /// Returns one ordered, paged range of live entries under a repository scope
    /// (files, packages, symbols, all memory, or a single memory topic).
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="repoId">The repository to scan.</param>
    /// <param name="scope">The range to walk: Files, Packages, Symbols, Memory, or MemoryTopic.</param>
    /// <param name="topic">The topic, required when scope is MemoryTopic.</param>
    /// <param name="pathPrefix">An optional directory path prefix, honoured only for the Files scope.</param>
    /// <param name="continuationToken">An opaque token from a prior page, or null to start at the beginning.</param>
    /// <param name="pageSize">The maximum entries per page (clamped to [1, 500]).</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>A page of projected entries with a continuation token.</returns>
    /// <exception cref="McpException">A required argument is missing or the scope is unknown.</exception>
    public static Task<RepoContextScanResult> ScanAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier to scan.")]
        string repoId,
        [Description("The range to walk: 'Files', 'Packages', 'Symbols', 'Memory', or 'MemoryTopic'.")]
        string scope,
        [Description("The memory topic to scan; required when scope is 'MemoryTopic', otherwise ignored.")]
        string? topic = null,
        [Description("An optional directory path prefix to restrict a 'Files' scan to a subtree (for example 'src/'); ignored for other scopes.")]
        string? pathPrefix = null,
        [Description("An opaque continuation token from a prior page; omit to start at the beginning of the range.")]
        string? continuationToken = null,
        [Description("The maximum number of entries to return on this page. Clamped to the range [1, 500]; defaults to 100.")]
        int pageSize = 0,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (!Enum.TryParse<RepoContextScanScope>(scope, ignoreCase: true, out var parsedScope)
            || !Enum.IsDefined(parsedScope)
            || !string.Equals(parsedScope.ToString(), scope, StringComparison.OrdinalIgnoreCase))
        {
            throw new McpException(
                $"The 'scope' value '{scope}' is not recognised. Use one of: Files, Packages, Symbols, Memory, MemoryTopic.");
        }

        return ResolveStore(context)
            .ScanAsync(repoId, parsedScope, topic, pathPrefix, continuationToken, pageSize, cancellationToken);
    }

    /// <summary>
    /// Enumerates the distinct agent memory topics for a repository with their
    /// live entry counts.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="repoId">The repository whose topics to list.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>The distinct topics and their entry counts.</returns>
    /// <exception cref="McpException">The repository id is missing.</exception>
    public static Task<RepoContextTopicsResult> ListTopicsAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier whose memory topics to enumerate.")]
        string repoId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        return ResolveStore(context).ListTopicsAsync(repoId, cancellationToken);
    }

    /// <summary>
    /// Creates or updates an agent memory or decision entry, merging into any
    /// existing record at the same key and applying an optional time-to-live.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="repoId">The repository the entry belongs to.</param>
    /// <param name="topic">The topic bucket to file the entry under; free-form but not enforced, so prefer a small, stable vocabulary (for example 'decisions', 'gotchas', 'conventions', 'glossary', 'todo', or a stable feature name) for cross-session consistency.</param>
    /// <param name="id">The per-topic id, or null to generate one (create).</param>
    /// <param name="kind">The memory kind applied on creation: Decision, Note, or Memory.</param>
    /// <param name="title">An optional short title.</param>
    /// <param name="body">An optional free-form body.</param>
    /// <param name="author">An optional author identity.</param>
    /// <param name="provenance">An optional provenance descriptor.</param>
    /// <param name="tags">Optional tags to add to the entry.</param>
    /// <param name="addLinks">Optional knowledge-linking edges to add (relation to target keys).</param>
    /// <param name="removeLinks">Optional knowledge-linking edges to remove (relation to target keys).</param>
    /// <param name="ttlSeconds">An optional explicit time-to-live in seconds.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The write outcome.</returns>
    /// <exception cref="McpException">A required argument is missing, the kind is unknown, the TTL is not positive, or a link target is malformed.</exception>
    public static Task<RepoContextRememberResult> RememberAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier the entry belongs to.")]
        string repoId,
        [Description("The topic bucket to file the memory entry under. Free-form and not enforced, but prefer a small, stable vocabulary so related notes stay groupable across sessions rather than fragmenting into synonyms - for example 'decisions' (design choices with rationale), 'gotchas' (non-obvious pitfalls), 'conventions' (project norms), 'glossary' (domain terms), 'todo' (follow-ups), or a stable feature or component name.")]
        string topic,
        [Description("The per-topic entry id. Omit to create a new entry with a generated id; supply an existing id to update in place.")]
        string? id = null,
        [Description("The memory kind applied when the entry is created: 'Decision', 'Note', or 'Memory'. Defaults to 'Note'.")]
        string? kind = null,
        [Description("An optional short title for the entry.")]
        string? title = null,
        [Description("An optional free-form body for the entry.")]
        string? body = null,
        [Description("An optional author identity (the agent or session that wrote the entry).")]
        string? author = null,
        [Description("An optional provenance descriptor (where the context came from).")]
        string? provenance = null,
        [Description("Optional tags to add to the entry's add-wins tag set.")]
        IReadOnlyList<string>? tags = null,
        [Description("Optional knowledge-linking edges to add, as a map from relation name (for example 'broader', 'narrower', 'related', 'partOf') to the target repository-context keys the entry links to. Memory entries only; each target must be a well-formed key.")]
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks = null,
        [Description("Optional knowledge-linking edges to remove, as a map from relation name to the target keys to unlink. Memory entries only.")]
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks = null,
        [Description("An optional explicit time-to-live in seconds. When omitted, a newly created entry uses the repository's default memory TTL (if configured), otherwise it is durable.")]
        long? ttlSeconds = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new McpException("The 'topic' parameter is required and must be a non-empty identifier.");
        }

        var memoryKind = MemoryKind.Note;
        if (!string.IsNullOrWhiteSpace(kind)
            && (!Enum.TryParse(kind, ignoreCase: true, out memoryKind)
                || !Enum.IsDefined(memoryKind)
                || !string.Equals(memoryKind.ToString(), kind, StringComparison.OrdinalIgnoreCase)))
        {
            throw new McpException(
                $"The 'kind' value '{kind}' is not recognised. Use one of: Decision, Note, Memory.");
        }

        return ResolveStore(context).RememberAsync(
            repoId, topic, id, memoryKind, title, body, author, provenance, tags,
            addLinks, removeLinks, ttlSeconds, cancellationToken);
    }

    /// <summary>
    /// Patches scalar fields and tags on an existing structural or memory record
    /// using CRDT-merge semantics (never a blind overwrite).
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="key">The full repository-context key of the record to patch.</param>
    /// <param name="fields">The scalar field patches, keyed by field name.</param>
    /// <param name="addTags">Tags to add to the record.</param>
    /// <param name="removeTags">Tags to remove from the record.</param>
    /// <param name="addLinks">Knowledge-linking edges to add (relation to target keys). Memory records only.</param>
    /// <param name="removeLinks">Knowledge-linking edges to remove (relation to target keys). Memory records only.</param>
    /// <param name="cancellationToken">Cancels the read-merge-write.</param>
    /// <returns>The patch outcome.</returns>
    /// <exception cref="McpException">The key is missing or malformed, no record exists, a field is invalid, or a link target is malformed.</exception>
    public static Task<RepoContextUpdateResult> UpdateAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The full repository-context key of the record to patch, for example 'repo/{repoId}/file/{path}'.")]
        string key,
        [Description("Scalar field patches keyed by field name (for example {\"digest\":\"...\",\"language\":\"csharp\"}). Valid names depend on the record family.")]
        IReadOnlyDictionary<string, string>? fields = null,
        [Description("Tags to add to the record's add-wins set.")]
        IReadOnlyList<string>? addTags = null,
        [Description("Tags to remove from the record's add-wins set.")]
        IReadOnlyList<string>? removeTags = null,
        [Description("Knowledge-linking edges to add, as a map from relation name (for example 'broader', 'narrower', 'related', 'partOf') to target repository-context keys. Memory records only; each target must be a well-formed key.")]
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks = null,
        [Description("Knowledge-linking edges to remove, as a map from relation name to the target keys to unlink. Memory records only.")]
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new McpException("The 'key' parameter is required and must be a non-empty repository-context key.");
        }

        return ResolveStore(context).UpdateAsync(
            key, fields, addTags, removeTags, addLinks, removeLinks, cancellationToken);
    }

    /// <summary>
    /// Walks the knowledge-linking edges out of a memory entry and returns the
    /// adjacent entries, hydrated from the store of record, as a bounded
    /// breadth-first traversal.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="key">The seed key to traverse from.</param>
    /// <param name="relation">An optional relation to restrict the walk to.</param>
    /// <param name="depth">The maximum number of hops, clamped to [1, 3].</param>
    /// <param name="maxNodes">The maximum number of neighbors to return, clamped to [1, 100].</param>
    /// <param name="cancellationToken">Cancels the traversal.</param>
    /// <returns>The reached neighbors and whether the walk was truncated.</returns>
    /// <exception cref="McpException">The key is missing or malformed.</exception>
    public static Task<RepoContextNeighborsResult> NeighborsAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The seed repository-context key to walk knowledge-linking edges from, for example 'repo/{repoId}/mem/{topic}/{id}'.")]
        string key,
        [Description("An optional relation name to restrict the walk to (for example 'broader' or 'related'); omit to follow every relation.")]
        string? relation = null,
        [Description("The maximum number of hops to traverse. Clamped to the range [1, 3]; defaults to 1 (immediate neighbors).")]
        int depth = 1,
        [Description("The maximum number of neighbor entries to return. Clamped to the range [1, 100]; defaults to 50.")]
        int maxNodes = 50,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new McpException("The 'key' parameter is required and must be a non-empty repository-context key.");
        }

        return ResolveStore(context).NeighborsAsync(key, relation, depth, maxNodes, cancellationToken);
    }

    /// <summary>
    /// Forgets an entry by hard-deleting it, or by re-writing it with a short
    /// time-to-live so it lapses on its own.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="key">The full repository-context key of the entry to forget.</param>
    /// <param name="lapse">When true, soft-lapse via a short TTL; otherwise hard delete immediately.</param>
    /// <param name="lapseSeconds">The lapse window in seconds; defaults to 60 when lapsing.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The forget outcome.</returns>
    /// <exception cref="McpException">The key is missing or malformed, or the lapse window is not positive.</exception>
    public static Task<RepoContextForgetResult> ForgetAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The full repository-context key of the entry to forget.")]
        string key,
        [Description("When true, re-write the entry with a short time-to-live so it lapses; when false (the default), hard-delete it immediately.")]
        bool lapse = false,
        [Description("The lapse window in seconds when 'lapse' is true; defaults to 60. Ignored for a hard delete.")]
        long? lapseSeconds = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new McpException("The 'key' parameter is required and must be a non-empty repository-context key.");
        }

        return ResolveStore(context).ForgetAsync(key, lapse, lapseSeconds, cancellationToken);
    }

    /// <summary>
    /// Finds the repository-context records most relevant to a natural-language
    /// query, hydrated from the store of record and ranked best-first. Runs an
    /// exact semantic search when an embedder and vectors are available and
    /// otherwise degrades to a keyword/structural scan.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the search service.</param>
    /// <param name="repoId">The repository to search.</param>
    /// <param name="query">The natural-language query.</param>
    /// <param name="k">The maximum number of hits to return (clamped to [1, 100]).</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>The ranked hits and the mode that produced them.</returns>
    /// <exception cref="McpException">The repository id or query is missing.</exception>
    public static Task<RepoContextSearchResult> SearchAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier to search.")]
        string repoId,
        [Description("The natural-language query to find relevant repository-context records for.")]
        string query,
        [Description("The maximum number of hits to return. Clamped to the range [1, 100]; defaults to 10.")]
        int k = 0,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (string.IsNullOrWhiteSpace(query))
        {
            throw new McpException("The 'query' parameter is required and must be a non-empty query string.");
        }

        return ResolveSearchService(context).SearchAsync(repoId, query, k, cancellationToken);
    }

    /// <summary>
    /// Builds a budgeted, ranked, explained context bundle for a natural-language
    /// task in a single call, packing as much relevant source as fits under a hard
    /// token ceiling. The <paramref name="top"/>, <paramref name="responseBudgetTokens"/>,
    /// and <paramref name="detail"/> inputs are validated and clamped by the bundle
    /// service, so a wire caller can never drive unbounded work.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the bundle service.</param>
    /// <param name="repoId">The repository to bundle context from.</param>
    /// <param name="task">The natural-language task to pack context for.</param>
    /// <param name="top">The maximum number of files to consider (clamped to [1, 50]).</param>
    /// <param name="responseBudgetTokens">The hard token ceiling for the bundle (clamped to [1, 200000]).</param>
    /// <param name="detail">The requested detail level: <c>paths</c>, <c>outline</c>, <c>slices</c>, or <c>auto</c>; an unrecognised value resolves to <c>auto</c>.</param>
    /// <param name="seen">Opaque unit receipts the caller already holds; each matching unit is suppressed and never re-charged.</param>
    /// <param name="known">Whole-file possession claims of the form <c>path@hash</c>; each is honoured only for a version the tool actually delivered whole to the same session.</param>
    /// <param name="session">A named caller session that persists reuse bookkeeping across calls.</param>
    /// <param name="cancellationToken">Cancels the bundle.</param>
    /// <returns>The packed bundle, whose exact BPE total never exceeds the clamped budget.</returns>
    /// <exception cref="McpException">The repository id or task is missing.</exception>
    public static Task<RepoContextContextResult> ContextAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier to bundle context from.")]
        string repoId,
        [Description("The natural-language task to pack a ranked, explained context bundle for.")]
        string task,
        [Description("The maximum number of files to consider. Clamped to the range [1, 50]; defaults to 10.")]
        int top = 0,
        [Description(
            "The hard token ceiling for the whole bundle, measured with the exact BPE token counter. "
            + "Clamped to the range [1, 200000]; defaults to 8192. The bundle's reported total never exceeds it.")]
        int responseBudgetTokens = 0,
        [Description(
            "How much of each file to pack: 'paths' (path only, cheapest), 'outline' (declared-symbol skeleton), "
            + "'slices' (bounded body text, richest), or 'auto' (default) which picks the richest level that fits "
            + "and reports the level it settled on. An unrecognised value is treated as 'auto'.")]
        string? detail = null,
        [Description(
            "Opaque unit receipts (from a prior bundle's entry units) the caller already holds. Each matching unit "
            + "is suppressed - the rest of its file still arrives - acknowledged under 'reused', and never charged "
            + "against 'top' or the token budget.")]
        string[]? seen = null,
        [Description(
            "Whole-file possession claims of the form 'path@hash' (from a prior entry's path and contentHash). A claim "
            + "is honoured only for a version this tool actually delivered as a complete body to the same 'session'; "
            + "a partial (outline/paths) delivery can never satisfy it. A honoured claim suppresses the whole file.")]
        string[]? known = null,
        [Description(
            "A named caller session id. Its recorded deliveries auto-suppress units the session already holds and "
            + "validate 'known' claims, and this call's deliveries are recorded into it, so a session never pays "
            + "twice for the same context across calls.")]
        string? session = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (string.IsNullOrWhiteSpace(task))
        {
            throw new McpException("The 'task' parameter is required and must be a non-empty task description.");
        }

        return ResolveBundleService(context)
            .BuildAsync(repoId, task, top, responseBudgetTokens, ParseDetail(detail), seen, known, session, cancellationToken);
    }

    /// <summary>
    /// Parses the wire <c>detail</c> argument to a <see cref="RepoContextContextDetail"/>,
    /// case-insensitively and trimming surrounding whitespace. Any unrecognised,
    /// empty, or null value resolves to <see cref="RepoContextContextDetail.Auto"/>,
    /// so a wire caller can never fault the tool with a bad level.
    /// </summary>
    /// <param name="detail">The raw wire value, or <see langword="null"/>.</param>
    /// <returns>The parsed detail level, defaulting to auto.</returns>
    private static RepoContextContextDetail ParseDetail(string? detail)
        => detail?.Trim().ToLowerInvariant() switch
        {
            "paths" => RepoContextContextDetail.Paths,
            "outline" => RepoContextContextDetail.Outline,
            "slices" => RepoContextContextDetail.Slices,
            _ => RepoContextContextDetail.Auto,
        };

    /// <summary>
    /// Builds the structural outline of one indexed file - its declared symbols with
    /// kind, signature, and line span, plus the token cost of reading the whole file -
    /// so an agent can grasp a file's shape and budget a full read without fetching its
    /// body. A pure read over stored records; it never touches disk.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the graph service.</param>
    /// <param name="repoId">The repository the file belongs to.</param>
    /// <param name="path">The repository-relative file path to outline.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The file's outline, or an <c>exists=false</c> result when no node is stored.</returns>
    /// <exception cref="McpException">The repository id or path is missing.</exception>
    public static Task<RepoContextOutlineResult> OutlineAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier the file belongs to.")]
        string repoId,
        [Description("The repository-relative file path to outline, for example 'src/foo/Bar.cs'.")]
        string path,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (string.IsNullOrWhiteSpace(path))
        {
            throw new McpException("The 'path' parameter is required and must be a non-empty file path.");
        }

        return ResolveGraphService(context).OutlineAsync(repoId, path, cancellationToken);
    }

    /// <summary>
    /// Reports the drift between the stored index and the current workspace - the files
    /// added, updated, and removed by content digest, without git - plus the indexed
    /// files that depend on the changed ones. The workspace is read only through the
    /// fail-closed workspace guard, so a caller-supplied path can never escape the
    /// mounted workspace.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the graph service.</param>
    /// <param name="repoId">The repository whose index is compared.</param>
    /// <param name="path">The workspace path to compare against the index.</param>
    /// <param name="cancellationToken">Cancels the walk and reads.</param>
    /// <returns>The added, updated, removed, and dependent file lists.</returns>
    /// <exception cref="McpException">The repository id or path is missing, or the path
    /// resolves outside the workspace or is not an existing directory.</exception>
    public static async Task<RepoContextChangedResult> ChangedAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier whose stored index is compared.")]
        string repoId,
        [Description("The repository root to compare, or a directory inside it to scope the report to that subtree. The walk is always rooted at the repository's indexed root and uses the filters it was ingested with, so the report compares the same path space the index was built in. Resolved through the mounted workspace boundary; a path outside the indexed repository root is refused.")]
        string path,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (string.IsNullOrWhiteSpace(path))
        {
            throw new McpException("The 'path' parameter is required and must be a non-empty workspace path.");
        }

        try
        {
            return await ResolveGraphService(context).ChangedAsync(repoId, path, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RepoContextWorkspaceViolationException ex)
        {
            throw new McpException(ex.Message);
        }
        catch (ArgumentException ex)
        {
            throw new McpException(ex.Message);
        }
        catch (DirectoryNotFoundException ex)
        {
            throw new McpException(ex.Message);
        }
    }

    /// <summary>
    /// Resolves the structural neighbourhood of one file: the type-names it references
    /// (outbound imports), the indexed symbols that reference its declarations (inbound
    /// dependents), and the test types that cover them. A pure read over stored records;
    /// it never touches disk.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the graph service.</param>
    /// <param name="repoId">The repository the file belongs to.</param>
    /// <param name="path">The repository-relative file path whose neighbourhood to resolve.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The related-neighbourhood result, or an <c>exists=false</c> result when no node is stored.</returns>
    /// <exception cref="McpException">The repository id or path is missing.</exception>
    public static Task<RepoContextRelatedResult> RelatedAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identifier the file belongs to.")]
        string repoId,
        [Description("The repository-relative file path whose related neighbourhood to resolve, for example 'src/foo/Bar.cs'.")]
        string path,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        if (string.IsNullOrWhiteSpace(path))
        {
            throw new McpException("The 'path' parameter is required and must be a non-empty file path.");
        }

        return ResolveGraphService(context).RelatedAsync(repoId, path, cancellationToken);
    }

    /// <summary>
    /// Registers a repository under the mounted workspace and starts an
    /// asynchronous indexing job for it: it resolves the requested path against the
    /// workspace boundary, then hands a durable job to the background runner that
    /// walks the tree and reconciles the scan against the store idempotently. This
    /// is the workspace-mode replacement for <c>repocontext_bootstrap</c> - the
    /// client mounts a broad parent read-only once and adds individual
    /// repositories beneath it on demand, rather than baking a single repository
    /// path into the container's configuration. Reaching this handler means the
    /// caller cleared the fail-closed authorization gate and the host opted writes
    /// in.
    /// <para>
    /// The call returns as soon as the job is durably recorded - it does not wait
    /// for the walk - so a dropped MCP stream cannot abort the index. Poll
    /// <c>repocontext_index_status</c> with the repository id to follow the run.
    /// </para>
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the workspace
    /// guard and the job grain.</param>
    /// <param name="path">The path to the repository under the mounted workspace.</param>
    /// <param name="repoId">The repository identity to file records under, or
    /// <see langword="null"/> to derive it from the final path segment.</param>
    /// <param name="includeGlobs">Optional include globs; when non-empty a file is
    /// ingested only if it matches at least one.</param>
    /// <param name="excludeGlobs">Optional exclude globs; a match removes a file
    /// even when it also matched an include.</param>
    /// <param name="respectGitignore">When <see langword="true"/> (the default), the
    /// tree's <c>.gitignore</c> files are honoured so ignored files and directories
    /// are not ingested.</param>
    /// <param name="excludeBinary">When <see langword="true"/> (the default), files
    /// that look binary (a NUL byte in their leading bytes) are dropped so compiled
    /// artefacts, images, and other blobs are not ingested.</param>
    /// <returns>The progress snapshot at acceptance, with the job running.</returns>
    /// <exception cref="McpException">The path is missing, resolves outside the
    /// workspace, does not exist, or yields no repository id (caller errors).</exception>
    public static Task<RepoIndexProgress> AddRepoAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Path to the repository to register, under the mounted workspace root (for example '/workspace/my-repo'). Resolved against the workspace boundary; a path outside it is rejected.")]
        string path,
        [Description("Optional stable repository identity to file records under; when omitted it is derived from the final path segment. Re-adding the same id updates the same records.")]
        string? repoId = null,
        [Description("Optional include globs (for example 'src/**' or '*.cs'); when non-empty a file is ingested only if it matches at least one. '**' matches any depth, '*' a single path segment.")]
        IReadOnlyList<string>? includeGlobs = null,
        [Description("Optional exclude globs; a match removes a file even when it also matched an include. The '.git' directory is always skipped.")]
        IReadOnlyList<string>? excludeGlobs = null,
        [Description("When true (the default), the tree's .gitignore files are honoured so ignored files and directories are not ingested. Set false to ingest every file regardless of ignore rules.")]
        bool respectGitignore = true,
        [Description("When true (the default), files that look binary (a NUL byte in their leading bytes) are dropped so compiled artefacts, images, and other blobs are not ingested. Set false to ingest binary files too.")]
        bool excludeBinary = true)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new McpException("The 'path' parameter is required and must be a non-empty path under the workspace root.");
        }

        var resolvedId = string.IsNullOrWhiteSpace(repoId) ? DeriveRepoId(path) : repoId!.Trim();
        if (string.IsNullOrWhiteSpace(resolvedId))
        {
            throw new McpException(
                "A repository id could not be derived from the path; supply the 'repoId' parameter explicitly.");
        }

        return StartIndexAsync(context, path, resolvedId, includeGlobs, excludeGlobs, respectGitignore, excludeBinary);
    }

    /// <summary>
    /// Lists every repository currently registered in the context store, each with
    /// its last-ingested marker and recorded file count, so the caller can discover
    /// what is queryable before it recalls, scans, searches, or removes a
    /// repository.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The registered repositories and their count.</returns>
    public static Task<RepoContextRepoListResult> ListReposAsync(
        RequestContext<CallToolRequestParams> context,
        CancellationToken cancellationToken = default)
        => ResolveStore(context).ListReposAsync(cancellationToken);

    /// <summary>
    /// Removes every record for a repository from the context store: its
    /// structural nodes, agent memory, and vector data are tombstoned and the
    /// repository is dropped from <c>repocontext_list_repos</c>. The working tree
    /// on disk is never touched. Removing an unknown repository is a no-op that
    /// reports zero deletions. Reaching this handler means the caller cleared the
    /// fail-closed authorization gate and the host opted writes in.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the store.</param>
    /// <param name="repoId">The repository identity whose records to remove.</param>
    /// <param name="cancellationToken">Cancels the removal.</param>
    /// <returns>The repository id and the number of entries removed.</returns>
    /// <exception cref="McpException">The repository id is missing (a caller error).</exception>
    public static Task<RepoContextRepoRemovalResult> RemoveRepoAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identity whose records to remove from the context store. The working tree on disk is not touched.")]
        string repoId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        return ResolveStore(context).RemoveRepoAsync(repoId, cancellationToken);
    }

    /// <summary>
    /// Derives a repository id from the final segment of a path, tolerating
    /// trailing separators of either platform. Returns an empty string when no
    /// segment remains (for example a bare root path).
    /// </summary>
    /// <param name="path">The repository path.</param>
    /// <returns>The derived repository id, or an empty string.</returns>
    private static string DeriveRepoId(string path)
    {
        var trimmed = path.Trim().TrimEnd('/', '\\');
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        var lastSeparator = trimmed.LastIndexOfAny(new[] { '/', '\\' });
        return lastSeparator < 0 ? trimmed : trimmed[(lastSeparator + 1)..];
    }

    /// <summary>
    /// Returns the current progress snapshot for a repository's indexing job so a
    /// caller can follow an asynchronous onboarding pass to completion. Read-only.
    /// A repository that was never onboarded reports status <c>None</c>.
    /// </summary>
    /// <param name="context">The MCP request context, used to resolve the job grain.</param>
    /// <param name="repoId">The repository identity whose indexing job to inspect.</param>
    /// <returns>The point-in-time progress snapshot.</returns>
    /// <exception cref="McpException">The repository id is missing (a caller error).</exception>
    public static Task<RepoIndexProgress> IndexStatusAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("The repository identity whose indexing job to inspect. A repository that was never onboarded reports status 'None'.")]
        string repoId)
    {
        if (string.IsNullOrWhiteSpace(repoId))
        {
            throw new McpException("The 'repoId' parameter is required and must be a non-empty identifier.");
        }

        var runner = context.Services?.GetRequiredService<IRepoIndexRunner>()
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the repository-context tool cannot resolve the index runner.");
        return runner.GetProgressAsync(repoId.Trim());
    }

    /// <summary>
    /// Resolves and workspace-guards the path synchronously at the tool seam, then
    /// hands a durable job to the indexing job grain and returns its acceptance
    /// snapshot without waiting for the run. Guarding here - not inside the
    /// resumable job - means a persisted, later-resumed job never re-derives a path
    /// that could escape the workspace.
    /// </summary>
    /// <param name="context">The MCP request context.</param>
    /// <param name="repoRoot">The caller-supplied repository path.</param>
    /// <param name="repoId">The already-resolved repository identity.</param>
    /// <param name="includeGlobs">Optional include globs.</param>
    /// <param name="excludeGlobs">Optional exclude globs.</param>
    /// <param name="respectGitignore">Whether the tree's <c>.gitignore</c> files are honoured.</param>
    /// <param name="excludeBinary">Whether files that look binary are dropped from the walk.</param>
    /// <returns>The progress snapshot at acceptance.</returns>
    /// <exception cref="McpException">The path resolves outside the workspace or
    /// does not exist (caller errors).</exception>
    private static async Task<RepoIndexProgress> StartIndexAsync(
        RequestContext<CallToolRequestParams> context,
        string repoRoot,
        string repoId,
        IReadOnlyList<string>? includeGlobs,
        IReadOnlyList<string>? excludeGlobs,
        bool respectGitignore,
        bool excludeBinary)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the onboarding tool cannot resolve its collaborators.");

        var guard = services.GetRequiredService<RepoContextWorkspaceGuard>();
        string resolvedRoot;
        try
        {
            resolvedRoot = guard.Resolve(repoRoot);
        }
        catch (RepoContextWorkspaceViolationException ex)
        {
            throw new McpException(ex.Message);
        }
        catch (ArgumentException ex)
        {
            throw new McpException(ex.Message);
        }

        if (!Directory.Exists(resolvedRoot))
        {
            throw new McpException(
                $"The repository root '{repoRoot}' does not exist or is not a directory.");
        }

        var request = new RepoIndexJobRequest
        {
            RepoRoot = resolvedRoot,
            RepoId = repoId,
            IncludeGlobs = includeGlobs,
            ExcludeGlobs = excludeGlobs,
            RespectGitignore = respectGitignore,
            ExcludeBinary = excludeBinary,
        };

        // The self-index grain is the single owner of this repository's "reach and
        // stay fully indexed" guarantee: EnsureRunningAsync arms the durable
        // per-repository keep-alive reminder and background gap scan, then drives the
        // initial indexing pass and returns its snapshot. Onboarding and self-heal
        // recovery therefore funnel through exactly one path, so they can never drift.
        var grainFactory = services.GetRequiredService<IGrainFactory>();
        return await grainFactory
            .GetGrain<IRepoContextSelfIndexGrain>(repoId).EnsureRunningAsync(request).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the <see cref="RepoContextStore"/> from the MCP request's service
    /// provider, failing with a clear message when the provider is absent.
    /// </summary>
    /// <param name="context">The MCP request context.</param>
    /// <returns>The resolved store.</returns>
    private static RepoContextStore ResolveStore(RequestContext<CallToolRequestParams> context)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the repository-context tool cannot resolve its store.");
        return services.GetRequiredService<RepoContextStore>();
    }

    /// <summary>
    /// Resolves the <see cref="RepoContextSearchService"/> from the MCP request's
    /// service provider, failing with a clear message when the provider is absent.
    /// </summary>
    /// <param name="context">The MCP request context.</param>
    /// <returns>The resolved search service.</returns>
    private static RepoContextSearchService ResolveSearchService(RequestContext<CallToolRequestParams> context)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the repository-context search tool cannot resolve its service.");
        return services.GetRequiredService<RepoContextSearchService>();
    }

    /// <summary>
    /// Resolves the <see cref="RepoContextBundleService"/> from the MCP request's
    /// service provider, failing with a clear message when the provider is absent.
    /// </summary>
    /// <param name="context">The MCP request context.</param>
    /// <returns>The resolved bundle service.</returns>
    private static RepoContextBundleService ResolveBundleService(RequestContext<CallToolRequestParams> context)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the repository-context bundle tool cannot resolve its service.");
        return services.GetRequiredService<RepoContextBundleService>();
    }

    /// <summary>
    /// Resolves the <see cref="RepoContextGraphService"/> from the MCP request's
    /// service provider, failing with a clear message when the provider is absent.
    /// </summary>
    /// <param name="context">The MCP request context.</param>
    /// <returns>The resolved graph service.</returns>
    private static RepoContextGraphService ResolveGraphService(RequestContext<CallToolRequestParams> context)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the repository-context graph tool cannot resolve its service.");
        return services.GetRequiredService<RepoContextGraphService>();
    }

    /// <summary>
    /// Resolves the <see cref="IRepoContextUsageRecorder"/> from the MCP request's
    /// service provider, failing with a clear message when the provider is absent.
    /// </summary>
    /// <param name="context">The MCP request context.</param>
    /// <returns>The resolved usage recorder.</returns>
    private static IRepoContextUsageRecorder ResolveUsageRecorder(RequestContext<CallToolRequestParams> context)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the repository-context stats tool cannot resolve its service.");
        return services.GetRequiredService<IRepoContextUsageRecorder>();
    }
}
