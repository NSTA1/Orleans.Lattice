using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The adapter behind the repository-context capture and maintenance tools
/// (<c>repocontext_recall</c>, <c>_scan</c>, <c>_list_topics</c>,
/// <c>_remember</c>, <c>_update</c>, and <c>_forget</c>). It is a thin projection
/// over the already-built foundation: the #1429 record model and its CRDT
/// <c>Merge</c>, the <see cref="RepoContextKeys"/> grammar, the
/// <see cref="RepoContextValues"/> register helpers, the #1430 TTL surface, and
/// the core <see cref="ILattice"/> read/write and cursor primitives. It adds no
/// storage or traversal primitive of its own.
/// <para>
/// Reads (<see cref="RecallAsync"/>, <see cref="ScanAsync"/>,
/// <see cref="ListTopicsAsync"/>) honour TTL-expiry and tombstone hiding because
/// the core read and cursor surfaces do. Writes go through the record model's
/// static <c>Merge</c> - never a blind overwrite - so concurrent authors
/// converge: <see cref="RememberAsync"/> and <see cref="UpdateAsync"/> read the
/// stored record, fold the change in at a fresh hybrid logical clock, and write
/// the merged result back; <see cref="ForgetAsync"/> either hard-deletes or
/// re-writes the entry with a short time-to-live so it lapses on its own.
/// </para>
/// </summary>
internal sealed partial class RepoContextStore
{
    private const int MaxPageSize = 500;
    private const int DefaultPageSize = 100;
    private const long DefaultLapseSeconds = 60L;

    // Bounded per-step delete budget for the resumable range-delete cursor, so
    // removing a large repository proceeds in reliable, cancellable chunks rather
    // than one unbounded tombstone pass.
    private const int DeleteStepSize = 256;

    private readonly IGrainFactory _grainFactory;
    private readonly IRepoIndexRunner _indexRunner;
    private readonly Serializer _serializer;
    private readonly RepoContextVectorWriter _vectorWriter;
    private readonly IOptionsMonitor<RepoContextTtlOptions> _ttlOptions;
    private readonly TimeProvider _timeProvider;
    private readonly RepoContextAnnIndexScheduler? _annScheduler;
    private readonly ILogger _logger;
    private readonly string _replicaId;

    /// <summary>Creates the capture/maintenance adapter.</summary>
    /// <param name="grainFactory">The grain factory used to reach the named Lattice trees. Must not be <see langword="null"/>.</param>
    /// <param name="indexRunner">The indexing runner, used to drain an in-flight index to a halt before a repository's records are removed. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode records. Must not be <see langword="null"/>.</param>
    /// <param name="vectorWriter">The vector writer that owns the membership layout, used to read the durable embedded-source count. Must not be <see langword="null"/>.</param>
    /// <param name="ttlOptions">The per-repository TTL policy. Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock used to project remaining life. Must not be <see langword="null"/>.</param>
    /// <param name="replicaIdentity">
    /// The stable replica identity authored onto every agent-memory CRDT write, or
    /// <see langword="null"/> to use the local single-cluster identity. The
    /// replication companion registers a cluster-id identity so cross-cluster
    /// concurrent memory writes mint distinct dots and both survive the merge.
    /// </param>
    /// <param name="annScheduler">
    /// The approximate-index build scheduler, used to disarm a repository's build
    /// coordinator during teardown, or <see langword="null"/> when the approximate
    /// retrieval plane is not composed into this host - in which case no coordinator
    /// was ever armed and there is nothing to stop.
    /// </param>
    /// <param name="logger">
    /// Diagnostics for the faults the index reset now tolerates instead of
    /// propagating, or <see langword="null"/> to discard them. A tolerated fault is
    /// still named in the failure this verb raises, but only the log carries its
    /// stack trace, and a tree that could not be drained is exactly the case an
    /// operator needs the stack for.
    /// </param>
    public RepoContextStore(
        IGrainFactory grainFactory,
        IRepoIndexRunner indexRunner,
        Serializer serializer,
        RepoContextVectorWriter vectorWriter,
        IOptionsMonitor<RepoContextTtlOptions> ttlOptions,
        TimeProvider timeProvider,
        IRepoContextReplicaIdentity? replicaIdentity = null,
        RepoContextAnnIndexScheduler? annScheduler = null,
        ILogger<RepoContextStore>? logger = null)    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(indexRunner);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(vectorWriter);
        ArgumentNullException.ThrowIfNull(ttlOptions);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _grainFactory = grainFactory;
        _indexRunner = indexRunner;
        _serializer = serializer;
        _vectorWriter = vectorWriter;
        _ttlOptions = ttlOptions;
        _timeProvider = timeProvider;
        _logger = logger ?? (ILogger)NullLogger.Instance;
        _annScheduler = annScheduler;
        _replicaId = replicaIdentity?.ReplicaId ?? LocalRepoContextReplicaIdentity.LocalReplicaId;
    }

    /// <summary>
    /// Fetches the live record at <paramref name="key"/> and projects it, or an
    /// <see cref="RepoContextEntryView.Exists"/>-false view when the key has no
    /// live entry.
    /// </summary>
    /// <param name="key">The full repository-context key. Must be a well-formed key.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The projected entry view.</returns>
    /// <exception cref="McpException">The key is not a well-formed repository-context key.</exception>
    public Task<RepoContextEntryView> RecallAsync(string key, CancellationToken cancellationToken)
        => RecallAsync(key, evaluateStaleness: false, cancellationToken);

    /// <summary>
    /// Fetches the live record at <paramref name="key"/> and projects it, optionally
    /// evaluating the link staleness of a memory entry. When
    /// <paramref name="evaluateStaleness"/> is <see langword="true"/> and the key
    /// addresses a memory record, every live structural link is checked against its
    /// target's present state - a target that has drifted, that has no live record
    /// at all, or for which no digest was ever captured is reported through
    /// <see cref="RepoContextEntryView.Stale"/> and
    /// <see cref="RepoContextEntryView.StaleLinks"/>, with the subset that points at
    /// nothing also named in <see cref="RepoContextEntryView.DanglingLinks"/>;
    /// otherwise those fields stay <see langword="null"/> ("not evaluated"), the
    /// bulk-read convention.
    /// </summary>
    /// <param name="key">The full repository-context key. Must be a well-formed key.</param>
    /// <param name="evaluateStaleness">Whether to evaluate memory link staleness on this read.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The projected entry view.</returns>
    /// <exception cref="McpException">The key is not a well-formed repository-context key.</exception>
    public async Task<RepoContextEntryView> RecallAsync(
        string key, bool evaluateStaleness, CancellationToken cancellationToken)
    {
        var parsed = ParseKey(key);
        var tree = Tree(RepoContextTrees.ForKind(parsed.Kind));

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        var life = RepoContextRemainingLife.FromVersionedValue(versioned, _timeProvider.GetUtcNow().UtcDateTime);
        var view = RepoContextEntryProjection.Project(parsed, versioned.Value, _serializer, life);

        if (evaluateStaleness
            && parsed.Kind == RepoContextRecordKind.Memory
            && versioned.Value is { } bytes
            && RepoContextMemoryCodec.Fold(bytes, _serializer, key) is { } record)
        {
            view = await EvaluateStalenessAsync(view, record, cancellationToken)
                .ConfigureAwait(false);
        }

        return view;
    }

    /// <summary>
    /// Evaluates each live structural link of <paramref name="record"/> against its
    /// target's present state and returns <paramref name="view"/> with
    /// <see cref="RepoContextEntryView.Stale"/>,
    /// <see cref="RepoContextEntryView.StaleLinks"/>, and
    /// <see cref="RepoContextEntryView.DanglingLinks"/> populated.
    /// <para>
    /// The walk is driven by the <em>live link set</em>, not by the captured-digest
    /// map, so an unlinked-but-still-recorded digest can never produce a phantom
    /// flag and - the point of issue #2654 - a link whose target was absent when
    /// the edge was written cannot escape evaluation merely because nothing was
    /// captured for it. Only file and symbol targets are evaluated, the same set
    /// <see cref="CaptureLinkDigestsAsync"/> captures for; a package or
    /// memory-to-memory edge carries no digest by design and is outside the
    /// measurand.
    /// </para>
    /// <para>
    /// A live structural link is fresh only when a digest was captured for it,
    /// the target still has a live record, and the two digests are ordinal-equal.
    /// Every other outcome is reported, so the answer follows the target's present
    /// state rather than unobservable link-time history: a target that never
    /// reached the corpus and one deleted after the edge was written now give the
    /// same answer, where previously only the second was flagged.
    /// </para>
    /// </summary>
    private async Task<RepoContextEntryView> EvaluateStalenessAsync(
        RepoContextEntryView view, MemoryRecord record, CancellationToken cancellationToken)
    {
        List<string>? stale = null;
        List<string>? dangling = null;
        HashSet<string>? seen = null;

        foreach (var (_, targets) in view.Links)
        {
            foreach (var target in targets)
            {
                if (!(seen ??= new HashSet<string>(StringComparer.Ordinal)).Add(target))
                {
                    continue;
                }

                if (!RepoContextKeys.TryParse(target, out var parsedTarget)
                    || parsedTarget.Kind is not (RepoContextRecordKind.File or RepoContextRecordKind.Symbol))
                {
                    continue;
                }

                var register = record.LinkDigests.Get(target);
                var captured = register is null ? null : RepoContextValues.ReadString(register);

                var targetView = await RecallAsync(target, cancellationToken).ConfigureAwait(false);
                if (!targetView.Exists)
                {
                    // The link points at nothing: either the target was deleted, or
                    // it never reached the indexed corpus at all. Both are reported,
                    // and the dangling list names them so a caller can tell "re-read
                    // the file" from "wait for the target to be indexed".
                    (dangling ??= new List<string>()).Add(target);
                    (stale ??= new List<string>()).Add(target);
                    continue;
                }

                if (captured is null)
                {
                    // The target has a live record but no digest was ever captured
                    // for this edge, so drift is not measurable here. Reporting that
                    // as fresh would render an absence of evidence as a positive
                    // finding; the edge becomes evaluable again when it is rewritten.
                    (stale ??= new List<string>()).Add(target);
                    continue;
                }

                targetView.Fields.TryGetValue("digest", out var current);
                if (!string.Equals(captured, current, StringComparison.Ordinal))
                {
                    (stale ??= new List<string>()).Add(target);
                }
            }
        }

        stale?.Sort(StringComparer.Ordinal);
        dangling?.Sort(StringComparer.Ordinal);
        return view with
        {
            Stale = stale is { Count: > 0 },
            StaleLinks = stale,
            DanglingLinks = dangling,
        };
    }

    /// <summary>
    /// Reads the current content digest of each newly-linked structural target (a
    /// file or symbol) so a later read can detect drift. Only file and symbol
    /// targets carry a digest; a memory-to-memory edge or an absent target is
    /// skipped. The result maps a target key to its captured digest, or
    /// <see langword="null"/> when no structural target was captured.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, string>?> CaptureLinkDigestsAsync(
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks, CancellationToken cancellationToken)
    {
        if (addLinks is null)
        {
            return null;
        }

        Dictionary<string, string>? captured = null;
        foreach (var (_, targets) in addLinks)
        {
            if (targets is null)
            {
                continue;
            }

            foreach (var target in targets)
            {
                if (captured is not null && captured.ContainsKey(target))
                {
                    continue;
                }

                if (!RepoContextKeys.TryParse(target, out var parsedTarget)
                    || parsedTarget.Kind is not (RepoContextRecordKind.File or RepoContextRecordKind.Symbol))
                {
                    continue;
                }

                var targetView = await RecallAsync(target, cancellationToken).ConfigureAwait(false);
                if (targetView.Exists
                    && targetView.Fields.TryGetValue("digest", out var digest)
                    && digest.Length != 0)
                {
                    (captured ??= new Dictionary<string, string>(StringComparer.Ordinal))[target] = digest;
                }
            }
        }

        return captured;
    }

    /// <summary>The hard ceiling on knowledge-linking traversal depth.</summary>
    private const int MaxNeighborDepth = 3;

    /// <summary>The hard ceiling on the number of neighbor entries a traversal returns.</summary>
    private const int MaxNeighborNodes = 100;

    /// <summary>
    /// Walks the knowledge-linking edges out of the memory entry (or any linkable
    /// record) at <paramref name="key"/> and returns the adjacent entries, hydrated
    /// from the store of record. A breadth-first walk follows each entry's
    /// <c>Links</c> relations up to <paramref name="depth"/> hops, optionally
    /// restricted to a single <paramref name="relation"/>, and stops once
    /// <paramref name="maxNodes"/> distinct neighbors have been collected. It is the
    /// read convenience behind <c>repocontext_neighbors</c>: an agent could walk the
    /// same edges by recalling each target key itself, but this removes the round
    /// trips for a bounded walk.
    /// </summary>
    /// <param name="key">The seed key to traverse from. Must be a well-formed key.</param>
    /// <param name="relation">An optional relation to restrict the walk to; when <see langword="null"/> every relation is followed.</param>
    /// <param name="depth">The maximum number of hops, clamped to [1, <see cref="MaxNeighborDepth"/>].</param>
    /// <param name="maxNodes">The maximum number of neighbors to return, clamped to [1, <see cref="MaxNeighborNodes"/>].</param>
    /// <param name="cancellationToken">Cancels the traversal.</param>
    /// <returns>The seed key, whether it exists, the reached neighbors best-first by discovery order, and whether the walk was truncated by the node cap.</returns>
    /// <exception cref="McpException">The seed key is malformed.</exception>
    public async Task<RepoContextNeighborsResult> NeighborsAsync(
        string key,
        string? relation,
        int depth,
        int maxNodes,
        CancellationToken cancellationToken)
    {
        _ = ParseKey(key);
        var clampedDepth = Math.Clamp(depth <= 0 ? 1 : depth, 1, MaxNeighborDepth);
        var clampedMax = Math.Clamp(maxNodes <= 0 ? MaxNeighborNodes : maxNodes, 1, MaxNeighborNodes);

        var seed = await RecallAsync(key, cancellationToken).ConfigureAwait(false);
        if (!seed.Exists)
        {
            return new RepoContextNeighborsResult
            {
                Key = key,
                Exists = false,
                Neighbors = Array.Empty<RepoContextEntryView>(),
                Truncated = false,
            };
        }

        var visited = new HashSet<string>(StringComparer.Ordinal) { key };
        var neighbors = new List<RepoContextEntryView>();
        var frontier = new Queue<(RepoContextEntryView View, int Depth)>();
        frontier.Enqueue((seed, 0));
        var truncated = false;

        while (frontier.Count > 0 && !truncated)
        {
            var (view, currentDepth) = frontier.Dequeue();
            if (currentDepth >= clampedDepth)
            {
                continue;
            }

            foreach (var (edgeRelation, targets) in view.Links)
            {
                if (relation is not null && !string.Equals(edgeRelation, relation, StringComparison.Ordinal))
                {
                    continue;
                }

                foreach (var target in targets)
                {
                    // Edges are validated on write, but a stored target could still be
                    // unparseable after a schema change; skip it rather than fail the walk.
                    if (!RepoContextKeys.TryParse(target, out _) || !visited.Add(target))
                    {
                        continue;
                    }

                    if (neighbors.Count >= clampedMax)
                    {
                        truncated = true;
                        break;
                    }

                    var neighbor = await RecallAsync(target, evaluateStaleness: true, cancellationToken).ConfigureAwait(false);
                    neighbors.Add(neighbor);
                    if (neighbor.Exists)
                    {
                        frontier.Enqueue((neighbor, currentDepth + 1));
                    }
                }

                if (truncated)
                {
                    break;
                }
            }
        }

        return new RepoContextNeighborsResult
        {
            Key = key,
            Exists = true,
            Neighbors = neighbors,
            Truncated = truncated,
        };
    }

    /// <summary>
    /// Returns one ordered, paged range of live entries under the scope's prefix.
    /// </summary>
    /// <param name="repoId">The repository to scan. Must be non-empty.</param>
    /// <param name="scope">The range to walk.</param>
    /// <param name="topic">The topic, required for <see cref="RepoContextScanScope.MemoryTopic"/>.</param>
    /// <param name="pathPrefix">An optional directory path prefix, honoured only for <see cref="RepoContextScanScope.Files"/>.</param>
    /// <param name="continuationToken">An opaque token from a prior page, or <see langword="null"/> to start.</param>
    /// <param name="pageSize">The maximum entries per page; clamped to [1, 500].</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>A page of projected entries with a continuation token.</returns>
    /// <exception cref="McpException">The repository id is empty, or the topic is missing for a topic scan.</exception>
    public async Task<RepoContextScanResult> ScanAsync(
        string repoId,
        RepoContextScanScope scope,
        string? topic,
        string? pathPrefix,
        string? continuationToken,
        int pageSize,
        CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");
        var (treeName, prefix) = ResolveScope(repoId, scope, topic, pathPrefix);
        var tree = Tree(treeName);
        var effectivePageSize = ClampPageSize(pageSize);

        var page = await RepoContextPortability
            .EnumerateAsync(tree, prefix, continuationToken, effectivePageSize, vectorExport: null, cancellationToken)
            .ConfigureAwait(false);

        var entries = new List<RepoContextEntryView>(page.Records.Count);
        foreach (var record in page.Records)
        {
            if (!RepoContextKeys.TryParse(record.Key, out var parsed))
            {
                continue;
            }

            // A bulk scan enumerates key+value bytes only; it cannot cheaply read
            // each entry's expiry, so it projects expiry as "not evaluated" (null)
            // rather than falsely asserting a durable entry. The enumerator still
            // yields only live (non-expired, non-tombstoned) entries.
            entries.Add(RepoContextEntryProjection.Project(
                parsed, record.Value, _serializer, life: null));
        }

        return new RepoContextScanResult
        {
            Entries = entries,
            ContinuationToken = page.ContinuationToken,
            HasMore = page.HasMore,
        };
    }

    /// <summary>
    /// Enumerates the distinct agent memory topics for a repository with their live
    /// entry counts, in ascending topic order.
    /// </summary>
    /// <param name="repoId">The repository whose topics to list. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>The distinct topics and their entry counts.</returns>
    /// <exception cref="McpException">The repository id is empty.</exception>
    public async Task<RepoContextTopicsResult> ListTopicsAsync(string repoId, CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");
        var tree = Tree(RepoContextTrees.Memory);
        var prefix = RepoContextKeys.MemoryPrefix(repoId);
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);

        string? token = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, MaxPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            foreach (var record in page.Records)
            {
                if (RepoContextKeys.TryParse(record.Key, out var parsed)
                    && parsed.Kind == RepoContextRecordKind.Memory
                    && parsed.Topic is { } topic)
                {
                    counts[topic] = counts.TryGetValue(topic, out var existing) ? existing + 1 : 1;
                }
            }

            if (!page.HasMore)
            {
                break;
            }

            token = page.ContinuationToken;
        }

        var topics = counts
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => new RepoContextTopicSummary { Topic = pair.Key, EntryCount = pair.Value })
            .ToList();

        return new RepoContextTopicsResult { RepoId = repoId, Topics = topics };
    }

    /// <summary>
    /// Creates or updates an agent memory entry, folding the supplied scalars and
    /// tags into any existing record at the same key through the record model's
    /// CRDT merge, and applying a time-to-live when supplied (or the per-repository
    /// default on creation).
    /// </summary>
    /// <param name="repoId">The repository the entry belongs to. Must be non-empty.</param>
    /// <param name="topic">The topic bucket. Must be non-empty.</param>
    /// <param name="id">The per-topic id, or <see langword="null"/> to generate one.</param>
    /// <param name="kind">The memory kind applied on creation.</param>
    /// <param name="title">An optional last-writer-wins title.</param>
    /// <param name="body">An optional last-writer-wins body.</param>
    /// <param name="author">An optional last-writer-wins author.</param>
    /// <param name="provenance">An optional last-writer-wins provenance descriptor.</param>
    /// <param name="tags">Optional tags to add to the entry's set.</param>
    /// <param name="addLinks">Optional knowledge-linking edges to add (relation to target keys).</param>
    /// <param name="removeLinks">Optional knowledge-linking edges to remove (relation to target keys).</param>
    /// <param name="ttlSeconds">An explicit time-to-live in seconds, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The write outcome.</returns>
    /// <exception cref="McpException">A required argument is empty, the TTL is not positive, or a link target is malformed.</exception>
    public Task<RepoContextRememberResult> RememberAsync(
        string repoId,
        string topic,
        string? id,
        MemoryKind kind,
        string? title,
        string? body,
        string? author,
        string? provenance,
        IReadOnlyList<string>? tags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        long? ttlSeconds,
        CancellationToken cancellationToken)
        => RememberAsync(
            repoId, topic, id, kind, title, body, author, provenance,
            tags, addLinks, removeLinks, ttlSeconds, fencingToken: null, cancellationToken);

    /// <summary>
    /// Creates or merges a memory entry, presenting <paramref name="fencingToken"/>
    /// as proof of the caller's claim on the entry.
    /// <para>
    /// The fencing check is applied to the stored record before the merge, so a
    /// caller whose claim has been superseded is refused here rather than winning a
    /// silent last-writer-wins race. A record nothing has claimed admits the write
    /// whether or not a token is presented, which is what leaves every unclaimed
    /// entry behaving exactly as it did before claims existed.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository identifier. Must be non-empty.</param>
    /// <param name="topic">The topic bucket. Must be non-empty.</param>
    /// <param name="id">The per-topic id, or <see langword="null"/> to generate one.</param>
    /// <param name="kind">The memory kind applied on creation.</param>
    /// <param name="title">An optional last-writer-wins title.</param>
    /// <param name="body">An optional last-writer-wins body.</param>
    /// <param name="author">An optional last-writer-wins author.</param>
    /// <param name="provenance">An optional last-writer-wins provenance descriptor.</param>
    /// <param name="tags">Optional tags to add to the entry's set.</param>
    /// <param name="addLinks">Optional knowledge-linking edges to add (relation to target keys).</param>
    /// <param name="removeLinks">Optional knowledge-linking edges to remove (relation to target keys).</param>
    /// <param name="ttlSeconds">An explicit time-to-live in seconds, or <see langword="null"/>.</param>
    /// <param name="fencingToken">The fencing token from <c>repocontext_claim</c>, or <see langword="null"/> to write unfenced.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The write outcome.</returns>
    /// <exception cref="McpException">A required argument is empty, the TTL is not positive, or a link target is malformed.</exception>
    /// <exception cref="RepoContextClaimConflictException">The entry is claimed and the presented token does not entitle this write.</exception>
    public async Task<RepoContextRememberResult> RememberAsync(
        string repoId,
        string topic,
        string? id,
        MemoryKind kind,
        string? title,
        string? body,
        string? author,
        string? provenance,
        IReadOnlyList<string>? tags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        long? ttlSeconds,
        long? fencingToken,
        CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");
        RequireNonEmpty(topic, "topic");

        var entryId = string.IsNullOrWhiteSpace(id) ? Guid.NewGuid().ToString("N") : id;
        var key = RepoContextKeys.Memory(repoId, topic, entryId);
        var tree = Tree(RepoContextTrees.Memory);
        var accessor = RepoContextMemoryCodec.Accessor(tree, key);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var existing = RepoContextMemoryCodec.Fold(
            await tree.GetAsync(key, cancellationToken).ConfigureAwait(false), _serializer, key);
        await EnforceFenceAsync(key, existing, fencingToken, cancellationToken).ConfigureAwait(false);
        var created = existing is null;

        var delta = new MemoryRecord
        {
            RepoId = repoId,
            Topic = topic,
            Id = entryId,
            Kind = created ? kind : MemoryKind.Unspecified,
        };

        if (title is not null) { delta = delta with { Title = RepoContextValues.Lww(title, clock) }; }
        if (body is not null) { delta = delta with { Body = RepoContextValues.Lww(body, clock) }; }
        if (author is not null) { delta = delta with { Author = RepoContextValues.Lww(author, clock) }; }
        if (provenance is not null) { delta = delta with { Provenance = RepoContextValues.Lww(provenance, clock) }; }
        if (created)
        {
            delta = delta with { CreatedAt = RepoContextValues.Lww(_timeProvider.GetUtcNow().UtcDateTime.Ticks, clock) };
        }

        var merged = created ? delta : MemoryRecord.Merge(delta, existing!);
        RepoContextRecordEditor.ApplyTags(merged.Tags, tags, removeTags: null);
        var (linksAdded, linksRemoved) = RepoContextRecordEditor.ApplyLinks(merged.Links, addLinks, removeLinks);
        var capturedDigests = await CaptureLinkDigestsAsync(addLinks, cancellationToken).ConfigureAwait(false);
        RepoContextRecordEditor.ApplyLinkDigests(merged.LinkDigests, capturedDigests, removeLinks, clock);
        var bytes = _serializer.SerializeToArray(merged);

        var ttl = ResolveTtl(repoId, ttlSeconds, created);
        if (ttl is { } window)
        {
            await accessor.SetAsync(_replicaId, bytes, window, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await accessor.SetAsync(_replicaId, bytes, cancellationToken).ConfigureAwait(false);
        }

        // POST-COMMIT BOUNDARY. The durable write above has landed. Nothing below may
        // propagate a fault, because a caller that sees this call throw cannot tell a
        // rejected write from a committed one, and the default id is a fresh GUID, so
        // the obvious retry writes a SECOND entry rather than converging on the first.
        var expiryTicks = await TryReadCommittedExpiryAsync(tree, key, cancellationToken).ConfigureAwait(false);
        await InvalidateMemoryVectorAsync(repoId, key, cancellationToken).ConfigureAwait(false);
        return new RepoContextRememberResult
        {
            Key = key,
            RepoId = repoId,
            Topic = topic,
            Id = entryId,
            Created = created,
            Expires = expiryTicks is { } ticks ? ticks != 0L : null,
            ExpiresAtUtc = expiryTicks is { } isoTicks ? ToExpiryIso(isoTicks) : null,
            LinksAdded = linksAdded,
            LinksRemoved = linksRemoved,
        };
    }

    /// <summary>
    /// Reads back the expiry of a key that was <b>just committed</b>, reporting
    /// <see langword="null"/> ("not evaluated") instead of propagating a fault.
    /// <para>
    /// This read is enrichment, not the operation. The caller's write is already
    /// durable by the time it runs, and it exists only to populate the expiry fields
    /// on the result. Letting it throw would discard the one fact the caller most
    /// needs - that the write landed - in exchange for two decorative fields, and the
    /// resulting error is indistinguishable from a write that never happened.
    /// </para>
    /// <para>
    /// Cancellation is absorbed here for the same reason and only here: cancelling a
    /// token cannot un-commit a durable write, so reporting the write is the honest
    /// answer even when the caller has stopped waiting. Every pre-commit path in this
    /// type still observes cancellation normally.
    /// </para>
    /// </summary>
    /// <param name="tree">The tree holding the committed key.</param>
    /// <param name="key">The key that was just written.</param>
    /// <param name="cancellationToken">Cancels the read-back only; it never un-commits the write.</param>
    /// <returns>The expiry in ticks (0 when the entry never expires), or <see langword="null"/> when the read-back could not be evaluated.</returns>
    private static async Task<long?> TryReadCommittedExpiryAsync(
        ILattice tree, string key, CancellationToken cancellationToken)
    {
        try
        {
            var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
            return versioned.ExpiresAtTicks;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// Retires the embedding of a memory entry that has just been written, so the
    /// vector plane never ranks an entry by text it no longer carries.
    /// <para>
    /// This is the change signal the reconcile cannot derive for itself. Memory is
    /// written through the tools rather than the repository walk, so no per-pass
    /// changed set reaches the ingestor and its back-fill - which embeds any entry
    /// with no live vector - would otherwise cover only brand-new entries. A
    /// REVISED entry already has a vector, so it would keep ranking on its
    /// pre-revision text indefinitely; a FORGOTTEN one would linger in the
    /// membership tally.
    /// </para>
    /// <para>
    /// Retiring on write closes both without a digest, a dirty-set, or any new
    /// persisted state: the entry simply looks un-embedded again, and the existing
    /// back-fill re-embeds it from its current text on the next reconcile (or, for
    /// a forget, finds no record and leaves it retired). The entry is briefly
    /// unreachable on the semantic path in between, which is the honest failure
    /// direction - a short absence that self-corrects, rather than a confident hit
    /// ranked on text the entry no longer has.
    /// </para>
    /// <para>
    /// Best-effort by design: retirement only deletes stored vector records, so it
    /// needs no embedder, and a failure here must not fail the caller's write. The
    /// capture is the durable act; the vector is a derived projection that the
    /// always-on sweep reconciles anyway.
    /// </para>
    /// <para>
    /// <b>Cancellation is absorbed too, and that is deliberate.</b> Every call site is
    /// past its durable commit, so an <see cref="OperationCanceledException"/> escaping
    /// here would fail a call whose write had already landed - reporting "nothing
    /// happened" about something that did. Cancelling a token cannot un-commit a write,
    /// so the honest answer post-commit is the result, not the fault. This is the only
    /// place in this type that absorbs cancellation; every pre-commit path still
    /// observes it normally.
    /// </para>
    /// </summary>
    private async Task InvalidateMemoryVectorAsync(
        string repoId, string key, CancellationToken cancellationToken)
    {
        try
        {
            await _vectorWriter.RetireAsync(repoId, key, cancellationToken).ConfigureAwait(false);
            await _vectorWriter.UnmarkMemoryEmbeddedAsync(repoId, key, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Swallowed deliberately, cancellation included: see the post-commit note above.
        }
    }

    /// <summary>
    /// Patches scalar fields and tags on an existing record through the record
    /// model's CRDT merge, preserving any remaining time-to-live the entry carried.
    /// </summary>
    /// <param name="key">The full repository-context key. Must address an existing record.</param>
    /// <param name="fields">The scalar field patches (field name to value), or <see langword="null"/>.</param>
    /// <param name="addTags">Tags to add, or <see langword="null"/>.</param>
    /// <param name="removeTags">Tags to remove, or <see langword="null"/>.</param>
    /// <param name="addLinks">Knowledge-linking edges to add (relation to target keys), or <see langword="null"/>. Memory records only.</param>
    /// <param name="removeLinks">Knowledge-linking edges to remove (relation to target keys), or <see langword="null"/>. Memory records only.</param>
    /// <param name="cancellationToken">Cancels the read-merge-write.</param>
    /// <returns>The patch outcome.</returns>
    /// <exception cref="McpException">The key is malformed, no record exists at it, a field is invalid, or a link target is malformed.</exception>
    public Task<RepoContextUpdateResult> UpdateAsync(
        string key,
        IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags,
        IReadOnlyList<string>? removeTags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        CancellationToken cancellationToken)
        => UpdateAsync(key, fields, addTags, removeTags, addLinks, removeLinks, fencingToken: null, cancellationToken);

    /// <summary>
    /// Patches a record while presenting <paramref name="fencingToken"/> as proof of
    /// the caller's claim on it.
    /// <para>
    /// This is the seam that makes a claim load-bearing rather than advisory: the
    /// stored record is checked against the presented token before the patch is
    /// merged, so a superseded holder is refused at the point of write. Only memory
    /// records can be claimed, so presenting a token for any other family is an
    /// error rather than a silently ignored argument.
    /// </para>
    /// </summary>
    /// <param name="key">The full repository-context key. Must address an existing record.</param>
    /// <param name="fields">The scalar field patches (field name to value), or <see langword="null"/>.</param>
    /// <param name="addTags">Tags to add, or <see langword="null"/>.</param>
    /// <param name="removeTags">Tags to remove, or <see langword="null"/>.</param>
    /// <param name="addLinks">Knowledge-linking edges to add (relation to target keys), or <see langword="null"/>. Memory records only.</param>
    /// <param name="removeLinks">Knowledge-linking edges to remove (relation to target keys), or <see langword="null"/>. Memory records only.</param>
    /// <param name="fencingToken">The fencing token from <c>repocontext_claim</c>, or <see langword="null"/> to write unfenced.</param>
    /// <param name="cancellationToken">Cancels the read-merge-write.</param>
    /// <returns>The patch outcome.</returns>
    /// <exception cref="McpException">The key is malformed, no record exists at it, a field is invalid, a link target is malformed, or a token was presented for a non-memory record.</exception>
    /// <exception cref="RepoContextClaimConflictException">The record is claimed and the presented token does not entitle this write.</exception>
    public async Task<RepoContextUpdateResult> UpdateAsync(
        string key,
        IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags,
        IReadOnlyList<string>? removeTags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        long? fencingToken,
        CancellationToken cancellationToken)
    {
        var parsed = ParseKey(key);
        RejectFenceOnNonMemory(key, parsed.Kind, fencingToken);
        var tree = Tree(RepoContextTrees.ForKind(parsed.Kind));

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        if (versioned.Value is not { } existing)
        {
            throw new McpException(
                $"No record exists at '{key}'. Use repocontext_remember or repocontext_bootstrap to create it first.");
        }

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var capturedDigests = await CaptureLinkDigestsAsync(addLinks, cancellationToken).ConfigureAwait(false);

        // For a memory record the stored value is an MvRegister blob whose concurrent
        // values are serialized MemoryRecords; fold them to a single record and hand
        // the re-serialized bytes to the patcher, which expects one record. Every
        // other family stores a single whole record, so its bytes patch directly.
        // The fold is also what the fencing check reads, so a claimed record is
        // judged against the same value the patch is about to merge into.
        byte[] patchInput;
        if (parsed.Kind == RepoContextRecordKind.Memory)
        {
            var folded = RepoContextMemoryCodec.Fold(existing, _serializer, key)!;
            await EnforceFenceAsync(key, folded, fencingToken, cancellationToken).ConfigureAwait(false);
            patchInput = _serializer.SerializeToArray(folded);
        }
        else
        {
            patchInput = existing;
        }

        var patch = RepoContextRecordEditor.Patch(
            parsed, patchInput, fields, addTags, removeTags, addLinks, removeLinks, clock, _serializer, capturedDigests);

        var remainingTtl = RemainingTtl(versioned.ExpiresAtTicks);
        if (parsed.Kind == RepoContextRecordKind.Memory)
        {
            // Author the merged record through the multi-value-register accessor so
            // the patch converges with any concurrent cross-cluster write instead of
            // overwriting it, preserving whatever remaining life the entry carried.
            var accessor = RepoContextMemoryCodec.Accessor(tree, key);
            if (remainingTtl is { } memoryWindow)
            {
                await accessor.SetAsync(_replicaId, patch.Merged, memoryWindow, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await accessor.SetAsync(_replicaId, patch.Merged, cancellationToken).ConfigureAwait(false);
            }
        }
        else if (remainingTtl is { } window)
        {
            await tree.SetAsync(key, patch.Merged, window, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await tree.SetAsync(key, patch.Merged, cancellationToken).ConfigureAwait(false);
        }

        await InvalidateMemoryVectorAsync(parsed.RepoId, key, cancellationToken).ConfigureAwait(false);
        return new RepoContextUpdateResult
        {
            Key = key,
            Kind = parsed.Kind.ToString(),
            FieldsUpdated = patch.FieldsUpdated,
            TagsAdded = patch.TagsAdded,
            TagsRemoved = patch.TagsRemoved,
            LinksAdded = patch.LinksAdded,
            LinksRemoved = patch.LinksRemoved,
        };
    }
    /// <summary>
    /// Forgets the entry at <paramref name="key"/>: a hard delete removes it
    /// immediately; a soft lapse re-writes it with a short time-to-live so it
    /// expires on its own.
    /// </summary>
    /// <param name="key">The full repository-context key. Must be well-formed.</param>
    /// <param name="lapse">When <see langword="true"/> soft-lapse; otherwise hard delete.</param>
    /// <param name="lapseSeconds">The lapse window in seconds, or <see langword="null"/> for the default.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The forget outcome.</returns>
    /// <exception cref="McpException">The key is malformed, or the lapse window is not positive.</exception>
    public Task<RepoContextForgetResult> ForgetAsync(
        string key,
        bool lapse,
        long? lapseSeconds,
        CancellationToken cancellationToken)
        => ForgetAsync(key, lapse, lapseSeconds, fencingToken: null, cancellationToken);

    /// <summary>
    /// Forgets the entry at <paramref name="key"/> while presenting
    /// <paramref name="fencingToken"/> as proof of the caller's claim on it. A
    /// forget is the most destructive write on the surface, so it is fenced exactly
    /// as a patch is: a claimed entry cannot be removed by a caller whose claim has
    /// been superseded, nor by one holding no claim at all.
    /// </summary>
    /// <param name="key">The full repository-context key. Must be well-formed.</param>
    /// <param name="lapse">When <see langword="true"/> soft-lapse; otherwise hard delete.</param>
    /// <param name="lapseSeconds">The lapse window in seconds, or <see langword="null"/> for the default.</param>
    /// <param name="fencingToken">The fencing token from <c>repocontext_claim</c>, or <see langword="null"/> to write unfenced.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The forget outcome.</returns>
    /// <exception cref="McpException">The key is malformed, the lapse window is not positive, or a token was presented for a non-memory record.</exception>
    /// <exception cref="RepoContextClaimConflictException">The entry is claimed and the presented token does not entitle this write.</exception>
    public async Task<RepoContextForgetResult> ForgetAsync(
        string key,
        bool lapse,
        long? lapseSeconds,
        long? fencingToken,
        CancellationToken cancellationToken)
    {
        var parsed = ParseKey(key);
        RejectFenceOnNonMemory(key, parsed.Kind, fencingToken);
        var tree = Tree(RepoContextTrees.ForKind(parsed.Kind));

        if (parsed.Kind == RepoContextRecordKind.Memory)
        {
            // A forget is fenced exactly as a patch is, but it is also the only
            // remedy for a record whose stored value cannot be decoded, so the fence
            // here must not be the thing that forecloses it. When the value folds,
            // the fence is resolved off the record as usual; when it does not, it is
            // resolved against the lock instead - which preserves the exclusion
            // invariant rather than relaxing it (see the fallback's remarks).
            var stored = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (RepoContextMemoryCodec.TryFold(stored, _serializer, key, out var existing))
            {
                await EnforceFenceAsync(key, existing, fencingToken, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await EnforceFenceOverUndecodableAsync(key, fencingToken, cancellationToken).ConfigureAwait(false);
            }
        }

        if (!lapse)
        {
            var deleted = await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            await InvalidateMemoryVectorAsync(parsed.RepoId, key, cancellationToken).ConfigureAwait(false);
            return new RepoContextForgetResult
            {
                Key = key,
                Mode = "delete",
                Existed = deleted,
                ExpiresAtUtc = null,
            };
        }

        var seconds = lapseSeconds ?? DefaultLapseSeconds;
        if (seconds <= 0L)
        {
            throw new McpException("The lapse window must be a positive number of seconds.");
        }

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        if (versioned.Value is not { } value)
        {
            return new RepoContextForgetResult
            {
                Key = key,
                Mode = "lapse",
                Existed = false,
                ExpiresAtUtc = null,
            };
        }

        var undecodable = false;
        if (parsed.Kind == RepoContextRecordKind.Memory)
        {
            // Lapse a memory record through the multi-value-register accessor so the
            // short time-to-live rides the CRDT-TTL join (max-absolute-ticks) and the
            // soft-delete converges across clusters instead of racing an LWW rewrite.
            // Fold first so the lapse re-authors the merged record, not one arm of a
            // conflict set.
            //
            // A malformed stored value must not foreclose the retirement. The
            // fallback below already anticipated "nothing folded, so lapse the stored
            // bytes as they are"; an undecodable value takes that same path rather
            // than throwing, because a record that cannot be read is exactly the
            // record that most needs retiring, and refusing here would leave a hard
            // delete as the only remedy - losing the entry rather than its formatting.
            // The tolerance is scoped to this path alone: every other read-modify-write
            // still fails loudly, so this cannot quietly absorb an unrelated decode
            // fault. It is reported on the result so the shedding is never silent.
            undecodable = !RepoContextMemoryCodec.TryFold(value, _serializer, key, out var folded);
            var lapseBytes = folded is null ? value : _serializer.SerializeToArray(folded);

            if (undecodable)
            {
                // The accessor's own read-modify-write would re-decode the same
                // malformed bytes, so the register path cannot carry this lapse.
                // A direct write of the stored bytes under the short time-to-live
                // retires the entry without ever decoding it.
                //
                // This bypass is a correctness requirement, not an optimisation, and
                // a perturbation arm establishes it rather than assuming it: routing
                // this write back through the accessor reddens the lapse fixtures
                // with a decode failure raised from the accessor itself. Do not
                // "simplify" the two branches back into one.
                await tree.SetAsync(key, lapseBytes, TimeSpan.FromSeconds(seconds), cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                var accessor = RepoContextMemoryCodec.Accessor(tree, key);
                await accessor.SetAsync(_replicaId, lapseBytes, TimeSpan.FromSeconds(seconds), cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        else
        {
            await tree.SetAsync(key, value, TimeSpan.FromSeconds(seconds), cancellationToken).ConfigureAwait(false);
        }

        // POST-COMMIT BOUNDARY: the lapse write above has landed. A fault in the
        // read-back below must degrade the reported expiry, never discard the lapse.
        // On a "lapse" result a null ExpiresAtUtc is unambiguous: a lapsed entry always
        // carries an expiry, so null here can only mean the read-back was not evaluated.
        var lapsedTicks = await TryReadCommittedExpiryAsync(tree, key, cancellationToken).ConfigureAwait(false);
        await InvalidateMemoryVectorAsync(parsed.RepoId, key, cancellationToken).ConfigureAwait(false);
        return new RepoContextForgetResult
        {
            Key = key,
            Mode = "lapse",
            Existed = true,
            ExpiresAtUtc = lapsedTicks is { } ticks ? ToExpiryIso(ticks) : null,
            Undecodable = undecodable,
        };
    }

    private ILattice Tree(string treeName) => _grainFactory.GetGrain<ILattice>(treeName);

    /// <summary>
    /// Lists every registered repository in ascending repository-id order, each
    /// with its last-ingested marker and recorded file count. The scan is
    /// proportional to the number of repositories, not the number of records: it
    /// reads the first key at or after a moving lower bound, extracts the
    /// repository id from that key, reads that repository's root marker, then
    /// advances the bound past the whole subtree.
    /// </summary>
    /// <param name="cancellationToken">Cancels the scan between repositories.</param>
    /// <returns>The registered repositories and their count.</returns>
    public async Task<RepoContextRepoListResult> ListReposAsync(CancellationToken cancellationToken)
    {
        var tree = Tree(RepoContextTrees.Structural);
        var repoIds = await ListRepoIdsAsync(cancellationToken).ConfigureAwait(false);
        var summaries = new List<RepoContextRepoSummary>(repoIds.Count);
        foreach (var repoId in repoIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            summaries.Add(await BuildRepoSummaryAsync(tree, repoId, cancellationToken).ConfigureAwait(false));
        }

        return new RepoContextRepoListResult
        {
            Repos = summaries,
            Count = summaries.Count,
        };
    }

    /// <summary>
    /// Walks the registered repository ids without building a per-repository
    /// summary.
    /// <para>
    /// A summary carries <c>embeddedVectorCount</c>, which is derived from the
    /// membership tree - the largest and slowest tree in the store. That read no longer
    /// blocks (issue 1992), but it still reads the repository root marker per repository
    /// and schedules an out-of-band membership walk when its memo is stale. A caller that
    /// only needs the ids (the retrieval warmup and the approximate-index sweep, which
    /// discard everything else) has no reason to pay either, least of all at startup
    /// while the vector trees are still replaying.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the scan between repositories.</param>
    /// <returns>The registered repository ids, in key order.</returns>
    internal async Task<IReadOnlyList<string>> ListRepoIdsAsync(CancellationToken cancellationToken)
    {
        var tree = Tree(RepoContextTrees.Structural);
        var namespacePrefix = RepoContextKeys.AllReposPrefix();
        var namespaceEnd = RepoContextPortability.PrefixUpperBound(namespacePrefix);
        var repoIds = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        var lower = namespacePrefix;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Resilient single-key probe: ScanKeysAsync reopens over the same
            // still-live range on a transient EnumerationAbortedException, so the
            // per-repository advance survives an enumerator reclaimed between the
            // open and the first read rather than aborting the whole listing.
            string? firstKey = null;
            await foreach (var key in tree
                .ScanKeysAsync(lower, namespaceEnd, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                firstKey = key;
                break;
            }

            if (firstKey is null)
            {
                break;
            }

            if (!RepoContextKeys.TryParse(firstKey, out var parsed))
            {
                // Defensive: a key that does not parse cannot yield a repository
                // id, so step just past it rather than looping forever on it.
                lower = firstKey + '\0';
                continue;
            }

            var repoId = parsed.RepoId;
            if (seen.Add(repoId))
            {
                repoIds.Add(repoId);
            }

            if (parsed.Kind == RepoContextRecordKind.Repo)
            {
                // The bare repo/{repoId} marker sorts before that repository's
                // repo/{repoId}/... subtree, but a sibling whose id extends this
                // one after a separator that orders below '/' (for example a
                // hyphen, so repo/svc-api sorts between repo/svc and repo/svc/)
                // lives in that gap. Stepping one key past the marker - rather
                // than jumping over the whole subtree - keeps such a sibling
                // visible; the subtree keys that follow parse back to an
                // already-seen id and are skipped cheaply below.
                lower = firstKey + '\0';
                continue;
            }

            var subtreeEnd = RepoContextPortability.PrefixUpperBound(RepoContextKeys.RepoScanPrefix(repoId));
            if (subtreeEnd is null)
            {
                break;
            }

            lower = subtreeEnd;
        }

        return repoIds;
    }

    /// <summary>
    /// Removes every record for a repository: the cancel/drain/clear preamble
    /// is shared with <see cref="ResetIndexAsync"/> via
    /// <see cref="TearDownIndexingControlAsync"/>, then a resilient range-delete
    /// drain tombstones each context tree's <c>repo/{repoId}/</c> subtree in
    /// bounded steps, reopening a fresh cursor across a transient enumerator loss
    /// so the whole subtree is drained rather than aborting part-way, then the
    /// bare <c>repo/{repoId}</c> root marker is deleted from the structural tree.
    /// Removing an absent repository is a no-op that reports zero deletions.
    /// </summary>
    /// <param name="repoId">The repository whose records to remove. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the removal between steps.</param>
    /// <returns>The repository id and the number of entries tombstoned.</returns>
    /// <exception cref="McpException">The repository id is empty.</exception>
    public async Task<RepoContextRepoRemovalResult> RemoveRepoAsync(string repoId, CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");

        await TearDownIndexingControlAsync(repoId).ConfigureAwait(false);

        var scanPrefix = RepoContextKeys.RepoScanPrefix(repoId);
        var end = RepoContextPortability.PrefixUpperBound(scanPrefix)
            ?? throw new McpException("The repository id produced an unbounded delete range.");

        long deleted = 0;
        // The local-derived trees are swept too: the approximate index is not
        // replicated, but it holds this repository's data under the same key
        // prefix, and leaving it behind would outlive the repository and be loaded
        // again by a later repository registered under the same id.
        foreach (var treeName in RepoContextTrees.AllIncludingLocalDerived)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // The resilient drain reopens a fresh delete-range cursor across a
            // transient enumerator loss (EnumerationAbortedException) so the whole
            // subtree is tombstoned rather than aborting part-way and orphaning
            // records after the control grains were already cleared above.
            deleted += await Tree(treeName)
                .DeleteRangeAsync(scanPrefix, end, DeleteStepSize, maxAttempts: null, cancellationToken)
                .ConfigureAwait(false);
        }

        // The root marker sits at repo/{repoId} with no trailing separator, so it
        // is outside the subtree range deleted above and is removed explicitly. It
        // is only ever written to the structural tree.
        var structural = Tree(RepoContextTrees.Structural);
        if (await structural.DeleteAsync(RepoContextKeys.Repo(repoId), cancellationToken).ConfigureAwait(false))
        {
            deleted++;
        }

        return new RepoContextRepoRemovalResult { RepoId = repoId, EntriesDeleted = checked((int)deleted) };
    }

    /// <summary>
    /// Drops a repository's code index and its derived planes but preserves its
    /// durable agent-memory records: the cancel/drain/clear preamble is shared
    /// with <see cref="RemoveRepoAsync"/>, then a resilient range-delete drain
    /// tombstones each code-index tree's <c>repo/{repoId}/</c> subtree in bounded
    /// steps. The bare <c>repo/{repoId}</c> root marker is deliberately
    /// <em>preserved</em> - it is what keeps the repository enumerable by
    /// <see cref="ListRepoIdsAsync"/> - but is rewritten with its index-derived
    /// fields (last-ingested marker, file count, and indexed commit) cleared, so
    /// the listing reports a repository with no index rather than quoting figures
    /// for one that was just deleted. Authored metadata (display name, default
    /// branch, and tags) is carried across unchanged. The
    /// <see cref="RepoContextTrees.Memory"/> tree is not
    /// touched, so every memory entry survives with its fields, tags, links, and
    /// remaining time-to-live intact. When the sweep found index records but no
    /// marker was present, a minimal marker is re-derived so the repository stays
    /// enumerable; resetting the index for an absent repository (one the sweep
    /// found nothing for) is a no-op that reports zero deletions and invents no
    /// marker.
    /// The set of trees to
    /// sweep is the local constant <see cref="RepoContextTrees.CodeIndexTrees"/>;
    /// see that member's remarks for why the vector payload tree is included
    /// here even though the auto-healer's allow-list excludes it, and why the
    /// vector-membership markers are dropped together with the payloads.
    /// </summary>
    /// <param name="repoId">The repository whose code index to reset. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the reset between steps.</param>
    /// <returns>The repository id and the number of code-index entries tombstoned.</returns>
    /// <exception cref="McpException">The repository id is empty.</exception>
    public async Task<RepoContextIndexResetResult> ResetIndexAsync(string repoId, CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");

        var resetStopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Collected, not thrown. TearDownIndexingControlAsync runs BEFORE any
        // sweeping, so until now a single unresponsive control-plane grain aborted
        // the reset before it deleted one record - and the grain most likely to be
        // unresponsive is the approximate-index build coordinator, whose slices
        // read the very vector trees the reset exists to drop. A damaged tree
        // therefore starved the stop call that was supposed to precede dropping
        // that tree, which is a deadlock in the recovery path: the worse the damage,
        // the less able the reset was to start. RemoveRepoAsync keeps the strict
        // behaviour (see the overload's remarks); only the recovery verb tolerates
        // a teardown fault, and it reports every one it tolerated.
        var teardownFailures = new List<string>();
        await TearDownIndexingControlAsync(repoId, teardownFailures).ConfigureAwait(false);

        // Mark the reset observable BEFORE any deletion. TearDownIndexingControlAsync
        // above cleared the job grain (it cancels and clears any in-flight index
        // run), so index_status would otherwise report None - indistinguishable
        // from a never-onboarded repository - for the whole of a sweep that can run
        // for minutes. BeginResetAsync re-populates that same surface with a
        // running teardown (status Running, phase Resetting), so a caller that
        // loses this call's response can still poll index_status and see the reset
        // in flight rather than nothing at all. The completion signal is written
        // only by CompleteResetAsync after the sweep finishes, never here.
        var jobGrain = _grainFactory.GetGrain<IRepoIndexJobGrain>(repoId);
        await jobGrain.BeginResetAsync().ConfigureAwait(false);

        var scanPrefix = RepoContextKeys.RepoScanPrefix(repoId);
        var end = RepoContextPortability.PrefixUpperBound(scanPrefix)
            ?? throw new McpException("The repository id produced an unbounded delete range.");

        var structural = Tree(RepoContextTrees.Structural);
        var markerKey = RepoContextKeys.Repo(repoId);

        // Clear the marker's index-derived registers BEFORE the sweep, not after.
        //
        // The sweep below can run for minutes on a large corpus, and for the whole
        // of that window list_repos is the surface an operator consults to ask
        // "did the reset work?". Clearing afterwards meant it answered that
        // question with the complete PRE-reset census - a stale lastIngested, a
        // stale fileCount, a stale indexedCommit - stated with full confidence and
        // indistinguishable from a healthy index. That does not read as "no
        // information yet"; it reads as "the reset did nothing", which is the one
        // conclusion that prompts an operator to run a destructive operation
        // AGAIN. The documented post-reset signature (three nulls) was implemented
        // correctly and simply could not be observed during the only window in
        // which anyone looks for it, so the documentation described an outcome
        // that never appeared - worse than describing none, because it licenses
        // trusting a field that is stale.
        //
        // Moving it is safe, and the ordering constraint that kept it here was
        // never real for THIS write. The marker sits at repo/{repoId} with no
        // trailing separator; the sweep is a range delete over repo/{repoId}/ and
        // its start bound sorts strictly after the marker key. The marker is
        // outside the swept range, so writing it first cannot be re-deleted. (The
        // re-derive branch after the sweep is a different case and genuinely must
        // stay there: it is conditioned on the deletion count, which is not known
        // until the sweep finishes.)
        //
        // Under a mid-sweep failure this also fails in the safer direction. The
        // marker then reports "registered, no index" while some index records
        // survive - understating coverage for a partially deleted index, which is
        // true and prompts a retry. The old order overstated it, claiming a full
        // census for an index that was being deleted underneath the claim.
        var markerBytes = await structural.GetAsync(markerKey, cancellationToken).ConfigureAwait(false);
        var censusCleared = false;
        if (markerBytes is not null)
        {
            // The three index-derived fields are cleared rather than carried across.
            // Keeping them would have list_repos report a file count and an ingest
            // timestamp for an index that no longer exists - a confident, precise
            // lie, which is worse than the absence it replaces. BuildRepoSummaryAsync
            // already tolerates unset registers and renders them as nulls, which is
            // exactly the "registered, no index" state a caller needs to distinguish
            // a just-reset repository from a never-onboarded one. Authored metadata
            // (display name, default branch, tags) is not index-derived, so it is
            // carried across untouched.
            var node = _serializer.Deserialize<RepoNode>(markerBytes) with
            {
                LastIngested = new BoundedRegister(),
                FileCount = new BoundedRegister(),
                IndexedCommit = new BoundedRegister(),
            };

            await structural.SetAsync(markerKey, _serializer.SerializeToArray(node), cancellationToken)
                .ConfigureAwait(false);
            censusCleared = true;
        }

        long deleted = 0;
        var treesSwept = 0;
        var sweptTrees = new List<string>();
        var sweepFailures = new List<string>();
        foreach (var treeName in RepoContextTrees.CodeIndexTrees)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Belt-and-braces: this iterates a local constant list, but the
            // fail-closed classification check guarantees the sweep can never
            // touch a tree that is not explicitly classified as a code-index
            // tree, even if the list is mis-edited to include Memory or a
            // future store-of-record tree name.
            if (!RepoContextTrees.IsCodeIndexTree(treeName))
            {
                throw new McpException(
                    "A code-only reset refused to sweep an unclassified tree: '" + treeName + "'.");
            }

            // One undrainable tree must not cost the other nine. The loop
            // previously let the first failure propagate, which sounds like the
            // safe default and is the opposite here: CodeIndexTrees is an ordered
            // constant, so a tree that cannot be drained permanently shadows every
            // tree after it, and the reset can never reach them however many times
            // it is retried. That was measured, not theorised - a ~170 MB leaf on
            // the symbol tree (position 2) blocked the vector-index tree (position
            // 9) which by itself held 99.2% of a 41 GB write-ahead log, so the one
            // verb that exists to reclaim that log could not reach the records
            // holding it open. Continuing past a failed tree is what lets the
            // whole-tree fallback in SweepTreeForResetAsync do its job on the
            // trees that CAN be recovered.
            //
            // The catch is deliberately narrow, and each omission is load-bearing:
            //
            //  - ILatticeLeafUnavailable and TimeoutException are the two shapes a
            //    tree that cannot be enumerated actually takes, and both are
            //    properties of the tree rather than of this request, so skipping
            //    to the next tree is the only way to make progress.
            //  - OperationCanceledException is NOT caught: a caller that cancelled
            //    must see the reset stop, not watch it grind through nine more
            //    trees pretending each one failed.
            //  - McpException is NOT caught: the refusals above it (an unclassified
            //    tree, the sole-repo gate inside the fallback) are deliberate
            //    fail-closed decisions, and swallowing one would turn a refusal to
            //    act into a silently skipped tree.
            //  - Nothing else is caught, so a genuine defect still surfaces as a
            //    failed reset rather than as a tree quietly recorded as damaged.
            //  - The structural tree is excluded from the catch entirely. Its
            //    fault is not "one tree of ten is damaged": it holds the
            //    repo/{repoId} marker that keeps the repository enumerable and the
            //    file nodes every other tree is keyed against, and
            //    SweepTreeForResetAsync refuses the whole-tree fallback on it
            //    unconditionally for that reason. A reset that cannot drain it has
            //    not partially succeeded, so it aborts with the original fault
            //    rather than recording a skipped tree.
            try
            {
                deleted += await SweepTreeForResetAsync(
                        treeName, repoId, scanPrefix, end, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (
                !string.Equals(treeName, RepoContextTrees.Structural, StringComparison.Ordinal)
                && IsUndrainableTreeFault(ex))
            {
                _logger.LogError(
                    ex,
                    "Reset of repository '{RepoId}' could not drain tree '{Tree}'; continuing with the remaining trees.",
                    repoId,
                    treeName);
                sweepFailures.Add(treeName + " (" + ex.GetType().Name + ": " + ex.Message + ")");
                continue;
            }

            // Report progress after each tree drains, so index_status shows the
            // teardown advancing (evidence of progress, not merely a "resetting"
            // flag). A wedged sweep stops advancing these counters, which is the
            // signal a caller needs; the completion marker is still withheld until
            // the whole loop finishes.
            treesSwept++;
            sweptTrees.Add(treeName);
            await jobGrain.ReportResetProgressAsync(treesSwept, checked((int)deleted)).ConfigureAwait(false);
        }

        // The root marker sits at repo/{repoId} with no trailing separator, so it
        // is outside the subtree range swept above. It is NOT deleted here, and
        // that is the whole point of this branch rather than an oversight:
        // ListRepoIdsAsync derives the listing by scanning the structural tree,
        // so once the subtree is swept the marker is the only key left that keeps
        // the repository enumerable. Delete it and a reset repository vanishes
        // from list_repos while its deliberately-preserved memory survives
        // underneath, reachable only by an agent that already knows the id -
        // which defeats the reason this verb exists.
        //
        // A reset must not invent a registration for a repository that was never
        // onboarded. The condition that establishes "never onboarded" is that the
        // sweep above found NOTHING - not that the marker happens to be absent.
        // The two are different, and conflating them left the original #2168
        // failure mode alive in a corner: a repository holding index records but
        // no marker is enumerable before a reset (its subtree keys carry the
        // listing) and not enumerable after it (subtree swept, no marker to
        // preserve), which is exactly the disappearance this verb exists to
        // prevent. Such a state is reachable through the portability seam, whose
        // ExportAsync is bounded by an arbitrary prefix - RepoScanPrefix covers
        // repo/{repoId}/ and so excludes the separator-free marker key - and
        // whose ImportAsync applies records one key at a time without atomicity.
        //
        // So the deletion count, not the marker's presence, is what separates the
        // two: it is direct evidence this repository had a code index a moment
        // ago. A zero-deletion reset still writes nothing, which keeps the
        // never-onboarded guarantee exactly as strong as it was.
        if (markerBytes is null && deleted > 0)
        {
            // Re-derived, not invented: every index-derived register is left unset,
            // which is the same "registered, no index" shape the preserve branch
            // above produces. There is no authored metadata to carry across - the
            // marker that would have held it is the one that is missing - so the
            // node is the bare key-derived identity and nothing more.
            await structural
                .SetAsync(markerKey, _serializer.SerializeToArray(new RepoNode { RepoId = repoId }), cancellationToken)
                .ConfigureAwait(false);
        }

        resetStopwatch.Stop();

        // A reset that could not drain every tree is NOT complete, and must not
        // say it is. CompleteResetAsync is the sole completion signal and its
        // contract is that an interrupted reset never reaches it, so a partial
        // sweep reports failure on the job surface and raises - it does not return
        // a result with a quietly shorter TreesSwept list, which a caller reading
        // a success response has no reason to inspect.
        //
        // Everything the sweep DID drain is already durable and is not rolled
        // back, so the message states it: the operator needs to know both that the
        // reset was partial and that it made real progress, because the correct
        // response to "eight of ten trees drained, two are damaged" is to
        // investigate those two, not to assume nothing happened and retry blindly.
        if (sweepFailures.Count > 0 || teardownFailures.Count > 0)
        {
            var detail = new System.Text.StringBuilder()
                .Append("The reset of repository '").Append(repoId)
                .Append("' was partial. Drained ").Append(treesSwept).Append(" of ")
                .Append(RepoContextTrees.CodeIndexTrees.Count).Append(" trees and deleted ")
                .Append(deleted).Append(" entries; that work is durable and is not rolled back.");

            if (sweepFailures.Count > 0)
            {
                detail.Append(" Trees that could not be drained: ")
                    .Append(string.Join("; ", sweepFailures)).Append('.');
            }

            if (teardownFailures.Count > 0)
            {
                detail.Append(" Control-plane teardown steps that did not complete: ")
                    .Append(string.Join("; ", teardownFailures))
                    .Append(". A writer may still be live for this repository.");
            }

            var message = detail.ToString();
            await jobGrain.FailAsync(message).ConfigureAwait(false);
            throw new McpException(message);
        }

        // The sole completion signal, written only now the sweep has finished.
        // Everything above this line ran with the job surface reporting a running
        // teardown; this is what flips it to Completed, so a caller can distinguish
        // "reset in progress" from "reset done" and, critically, an interrupted
        // reset (which never reaches this line) never reports itself complete.
        await jobGrain
            .CompleteResetAsync(resetStopwatch.ElapsedMilliseconds, treesSwept, checked((int)deleted))
            .ConfigureAwait(false);

        return new RepoContextIndexResetResult
        {
            RepoId = repoId,
            EntriesDeleted = checked((int)deleted),
            ElapsedMilliseconds = resetStopwatch.ElapsedMilliseconds,
            // The trees actually swept, not the constant list. TreesSwept is
            // documented as "named rather than counted so the caller can see
            // exactly what was dropped instead of trusting that the sweep covered
            // what it should have" - assigning CodeIndexTrees unconditionally
            // asserted precisely the thing the caller was told not to trust. The
            // two agreed only because the loop had no way to skip a tree; now that
            // it has, reporting the constant would be a straightforward lie.
            TreesSwept = sweptTrees,
            MemoryPreserved = true,
            CensusCleared = censusCleared,
        };
    }

    /// <summary>
    /// Whether a fault means a tree cannot be drained right now, as opposed to
    /// the reset being refused or a defect having surfaced. Shared by the sweep
    /// loop and the control-plane teardown so the two cannot drift apart on what
    /// counts as recoverable.
    /// <para>
    /// <see cref="ILatticeLeafUnavailable"/> covers both core exceptions that mean
    /// a leaf cannot be activated; <see cref="TimeoutException"/> covers an
    /// Orleans response timeout on the tree grain and, because both derive from
    /// it, the core's own stalled-scan and shard-activation timeouts. Neither is a
    /// property of this request, so retrying it unchanged repeats the same
    /// failure while moving to the next tree makes progress.
    /// </para>
    /// </summary>
    private static bool IsUndrainableTreeFault(Exception ex) =>
        ex is ILatticeLeafUnavailable or TimeoutException;

    /// <summary>
    /// Sweeps one code-index tree's <c>repo/{repoId}/</c> subtree for
    /// <see cref="ResetIndexAsync"/>, falling back to a whole-tree drop when the
    /// subtree cannot be walked at all.
    /// <para>
    /// The normal path is the resilient range-delete every other sweep uses. It
    /// enumerates, and enumeration activates leaves - so on a tree holding a leaf
    /// that is terminally un-activatable (its durable projection checkpoint was
    /// trimmed with no covering snapshot, or it cannot be materialised within the
    /// memory available) the reset cannot make progress at all. That is the exact
    /// state an operator invokes a reset to escape, so the one verb that exists to
    /// recover a damaged index is disabled by the damage. Both shapes carry
    /// <see cref="ILatticeLeafUnavailable"/>, which is what lets this package catch
    /// the condition at all - one of the two concrete types is internal to the
    /// core, so before that marker existed only one of the two was reachable here
    /// and the memory-exhaustion shape fell straight through the fallback.
    /// </para>
    /// <para>
    /// <see cref="ILattice.DeleteTreeAsync"/> is the one public primitive that
    /// makes progress there: it marks every shard root deleted through shard-root
    /// state alone and never activates the throwing leaf. It drops the
    /// <b>whole</b> tree, though, and these trees are shared by every repository,
    /// so it is only equivalent to the requested subtree delete when this
    /// repository is the only registered one. When another repository shares the
    /// tree the fault is rethrown rather than silently widened into someone else's
    /// data - the same fail-closed discipline
    /// <see cref="RepoContextTrees.IsRebuildableVectorTree"/> applies to the
    /// self-healer.
    /// </para>
    /// <para>
    /// <see cref="RepoContextTrees.Structural"/> is excluded from the fallback
    /// unconditionally. It carries the separator-free <c>repo/{repoId}</c> marker
    /// that <see cref="ResetIndexAsync"/> deliberately preserves to keep the
    /// repository enumerable, and that marker is outside the swept range precisely
    /// so the sweep cannot take it. A whole-tree drop is not bounded by the range
    /// and would delete it, turning a reset back into the disappearance the
    /// preserve branch exists to prevent.
    /// </para>
    /// </summary>
    /// <param name="treeName">The code-index tree to sweep.</param>
    /// <param name="repoId">The repository being reset.</param>
    /// <param name="scanPrefix">The inclusive start of the repository's subtree.</param>
    /// <param name="end">The exclusive upper bound of the repository's subtree.</param>
    /// <param name="cancellationToken">Cancels the sweep between steps.</param>
    /// <returns>The number of entries tombstoned, or zero when the tree was dropped whole.</returns>
    private async Task<long> SweepTreeForResetAsync(
        string treeName,
        string repoId,
        string scanPrefix,
        string end,
        CancellationToken cancellationToken)
    {
        var tree = Tree(treeName);
        try
        {
            return await tree
                .DeleteRangeAsync(scanPrefix, end, DeleteStepSize, maxAttempts: null, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception unavailable) when (unavailable is ILatticeLeafUnavailable)
        {
            if (string.Equals(treeName, RepoContextTrees.Structural, StringComparison.Ordinal))
            {
                throw;
            }

            // Only equivalent to the requested subtree delete when nothing else
            // lives in the tree. Read the registration census rather than assume:
            // a shared tree must keep the fault.
            var repoIds = await ListRepoIdsAsync(cancellationToken).ConfigureAwait(false);
            var soleRegisteredRepo = repoIds.Count <= 1
                && (repoIds.Count == 0 || string.Equals(repoIds[0], repoId, StringComparison.Ordinal));
            if (!soleRegisteredRepo)
            {
                throw new McpException(
                    $"The code-index tree '{treeName}' holds a leaf that cannot be activated, so the "
                    + $"repository subtree for '{repoId}' cannot be enumerated to delete it. The whole-tree "
                    + "drop that would recover it was refused because "
                    + $"{repoIds.Count} repositories share this tree and dropping it would delete their "
                    + "index records too. Remove or reset the other repositories first, or remove this one "
                    + "with repocontext_remove_repo.",
                    unavailable);
            }

            // Infrastructure-authored maintenance with no user identity behind it,
            // exactly as RepoContextVectorPlaneReDeriver documents for the same
            // primitive. The bypass is bounded the same way: it is entered only
            // after the fail-closed code-index classification accepted the tree,
            // the tree name is a local layout constant rather than any wire- or
            // exception-derived value, the scope contains nothing but the
            // delete/purge of that one tree, and it is lexical and disposed on
            // every path.
            using var systemOrigin = LatticeSystemOrigin.Enter();

            await tree.DeleteTreeAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await tree.PurgeTreeAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception purgeUnavailable) when (purgeUnavailable is ILatticeLeafUnavailable)
            {
                // Best-effort, matching the self-healer: the soft-delete has already
                // unblocked the terminal state and registered a reminder-driven
                // purge that completes the reclaim out of band.
                _ = purgeUnavailable;
            }

            // A whole-tree drop tombstones no individual entries, so it contributes
            // nothing to the deletion count. That is deliberate rather than a lost
            // figure: the count feeds the "was anything here?" test that decides
            // whether to re-derive an absent marker, and a dropped tree is not
            // evidence about THIS repository's records specifically.
            return 0;
        }
    }

    /// <summary>
    /// Cancels any in-flight indexing run for a repository, clears the job
    /// grain's durable state and its resume reminder, stops the always-on
    /// self-index scan, and disarms the approximate-index build coordinator.
    /// Shared by <see cref="RemoveRepoAsync"/> and
    /// <see cref="ResetIndexAsync"/> so both take the same control-plane
    /// teardown - a second copy would drift from the load-bearing comment
    /// below.
    /// </summary>
    /// <param name="repoId">The repository whose control plane is torn down.</param>
    /// <param name="faults">
    /// When <see langword="null"/> (the <see cref="RemoveRepoAsync"/> path) every
    /// step must succeed and the first failure propagates, because a removal
    /// deletes the repository's memory as well as its index and must not run with
    /// a writer still live. When non-<see langword="null"/> (the
    /// <see cref="ResetIndexAsync"/> path) a recoverable fault in one step is
    /// appended here and the remaining steps still run, because a reset is the
    /// verb invoked precisely when the deployment is already damaged - refusing to
    /// start until the damaged deployment answers promptly is how the recovery
    /// path deadlocks against the damage. The caller reports every collected fault
    /// and never claims the reset completed.
    /// </param>
    private async Task TearDownIndexingControlAsync(string repoId, ICollection<string>? faults = null)
    {
        // Stop any in-flight indexing run and drain it to a full halt BEFORE
        // deleting a single record. CancelAndWaitAsync cancels the run and awaits
        // its termination, so no concurrent structural write from the indexer can
        // race the range-delete below - a race that otherwise surfaces as an
        // Orleans state version conflict on a leaf shared by both writers. The job
        // grain then unregisters its resume reminder and clears its durable state,
        // so a removed repository leaves no reminder firing forever and no job
        // state for a later start to resume. Doing this first (rather than last)
        // also means an error in the delete pass can no longer skip the cleanup.
        await RunTeardownStepAsync(
            "cancel the in-flight indexing run",
            () => _indexRunner.CancelAndWaitAsync(repoId),
            repoId,
            faults).ConfigureAwait(false);
        await RunTeardownStepAsync(
            "clear the index job grain",
            () => _grainFactory.GetGrain<IRepoIndexJobGrain>(repoId).CancelAndClearAsync(),
            repoId,
            faults).ConfigureAwait(false);

        // Tear down the repository's always-on self-index scan so a removed
        // repository leaves no keep-alive reminder firing and no checkpoint behind.
        await RunTeardownStepAsync(
            "stop the self-index scan",
            () => _grainFactory.GetGrain<IRepoContextSelfIndexGrain>(repoId).StopAsync(),
            repoId,
            faults).ConfigureAwait(false);

        // Disarm the approximate-index build coordinator. This is the FOURTH
        // reminder-anchored writer for a repository, and until now teardown covered
        // only three - so a removed or reset repository left a durable keep-alive
        // registered for a build over records that were about to be deleted.
        //
        // Two consequences, both observed. The coordinator reactivates on that
        // reminder after every restart and re-opens the index into the process,
        // which is resident memory held for a repository that may no longer exist;
        // and it is a live writer to the vector-index tree racing the range-delete
        // below, which is precisely the state the CancelAndWaitAsync comment above
        // exists to prevent for the other three.
        //
        // Null when the approximate retrieval plane is not composed into this host,
        // in which case no coordinator was ever armed. The stop is not gated on the
        // scheduling switch: see RepoContextAnnIndexScheduler.TryStopAsync for why
        // the switch being off is the state that most needs stopping.
        //
        // This is the step measured to time out against a damaged index: the
        // coordinator is non-reentrant and its build slice, though budgeted at five
        // seconds of its own work, blocks indefinitely inside a single vector read
        // against an unresponsive tree, so the stop queues behind a turn that never
        // ends. On the reset path that fault is now collected rather than fatal.
        if (_annScheduler is not null)
        {
            await RunTeardownStepAsync(
                "disarm the approximate-index build coordinator",
                () => _annScheduler.TryStopAsync(repoId, CancellationToken.None),
                repoId,
                faults).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs one control-plane teardown step, either strictly or fault-collecting
    /// according to whether <paramref name="faults"/> was supplied. Only the same
    /// undrainable-tree shapes the sweep loop tolerates are collected - a
    /// cancellation, a deliberate refusal, or any other exception still propagates,
    /// so tolerating a wedged grain never becomes tolerating a defect.
    /// </summary>
    private async Task RunTeardownStepAsync(
        string description, Func<Task> step, string repoId, ICollection<string>? faults)
    {
        try
        {
            await step().ConfigureAwait(false);
        }
        catch (Exception ex) when (faults is not null && IsUndrainableTreeFault(ex))
        {
            _logger.LogError(
                ex,
                "Reset of repository '{RepoId}' could not {Step}; continuing so the sweep can still run.",
                repoId,
                description);
            faults.Add(description + " (" + ex.GetType().Name + ": " + ex.Message + ")");
        }
    }

    private async Task<RepoContextRepoSummary> BuildRepoSummaryAsync(
        ILattice structural, string repoId, CancellationToken cancellationToken)
    {
        string? lastIngested = null;
        long? fileCount = null;
        string? indexedCommit = null;

        var markerBytes = await structural
            .GetAsync(RepoContextKeys.Repo(repoId), cancellationToken)
            .ConfigureAwait(false);
        if (markerBytes is not null)
        {
            var node = _serializer.Deserialize<RepoNode>(markerBytes);
            lastIngested = RepoContextValues.ReadString(node.LastIngested);
            fileCount = RepoContextValues.ReadInt64(node.FileCount);
            indexedCommit = RepoContextValues.ReadString(node.IndexedCommit);
        }

        var embeddedVectorCount = await ReadEmbeddedVectorCountAsync(repoId, cancellationToken)
            .ConfigureAwait(false);

        // The indexed root is read from the durable index request rather than the marker
        // node, because the request is the authority for how the repository was actually
        // walked - the structural records are addressed relative to it. The marker node
        // never carried it, which is precisely why this listing could report a healthy
        // repository under an id that described a different tree (issue #2617): the root
        // was known to the drift report, which refuses a path "outside the indexed root
        // of repository '<root>'", and to nothing a caller reads first.
        var indexedRoot = await ReadIndexedRootAsync(repoId).ConfigureAwait(false);

        return new RepoContextRepoSummary
        {
            RepoId = repoId,
            IndexedRoot = indexedRoot,
            LastIngested = lastIngested,
            FileCount = fileCount,
            EmbeddedVectorCount = embeddedVectorCount.Count,
            EmbeddedVectorCountPending = embeddedVectorCount.Pending,
            IndexedCommit = indexedCommit,
        };
    }

    /// <summary>
    /// Reads the resolved root a repository was last indexed from, or
    /// <see langword="null"/> when it has no persisted index request - a repository that
    /// was never onboarded, or one whose index was reset (the reset path clears the
    /// request, so a null root is the same "no information yet" answer the reset's other
    /// three nulls carry, not a failure to read one).
    /// </summary>
    private async Task<string?> ReadIndexedRootAsync(string repoId)
    {
        var request = await _grainFactory
            .GetGrain<IRepoIndexJobGrain>(repoId).GetRequestAsync().ConfigureAwait(false);

        return string.IsNullOrWhiteSpace(request?.RepoRoot) ? null : request.RepoRoot;
    }

    /// <summary>
    /// Reads the durable count of sources with a live embedding for a repository: the
    /// number of live presence keys in the vector-membership tree that the vector
    /// writer maintains as embeddings land. A source is a file or a captured symbol, so
    /// this counts embedded files plus embedded symbols. It is read from the store of
    /// record (the vector-membership tree), never from a run's in-flight progress, so
    /// it is a restart-durable diagnostic.
    /// <para>
    /// The read never blocks on that walk. It serves the last completed count and lets
    /// the writer refresh out of band, reporting whether the value is current, because
    /// an active ingest invalidates the exactness key on every write and an exact-only
    /// contract turned this diagnostic into a whole-tree scan per <c>list_repos</c> call
    /// (issue 1992). A not-yet-measured repository reports <see langword="null"/>, which
    /// is a different answer from <c>0</c>.
    /// </para>
    /// </summary>
    private Task<RepoContextEmbeddedCount> ReadEmbeddedVectorCountAsync(
        string repoId, CancellationToken cancellationToken) =>
        _vectorWriter.CountEmbeddedAsync(repoId, cancellationToken);

    private TimeSpan? ResolveTtl(string repoId, long? ttlSeconds, bool created)
    {
        if (ttlSeconds is { } explicitSeconds)
        {
            if (explicitSeconds <= 0L)
            {
                throw new McpException("The 'ttlSeconds' parameter must be a positive number of seconds when supplied.");
            }

            return TimeSpan.FromSeconds(explicitSeconds);
        }

        if (created && _ttlOptions.Get(repoId).DefaultMemoryTtl is { } window)
        {
            return window;
        }

        return null;
    }

    private TimeSpan? RemainingTtl(long expiresAtTicks)
    {
        if (expiresAtTicks == 0L)
        {
            return null;
        }

        var remaining = expiresAtTicks - _timeProvider.GetUtcNow().UtcDateTime.Ticks;
        return remaining > 0L ? TimeSpan.FromTicks(remaining) : null;
    }

    /// <summary>
    /// Formats an absolute UTC expiry tick as an ISO-8601 UTC timestamp (round-trip
    /// "O" format), or <see langword="null"/> when the entry never expires. The
    /// string form keeps the value within the safe integer range of JSON consumers
    /// that would otherwise parse a raw <see cref="DateTime.Ticks"/> count as an
    /// out-of-range BigInt.
    /// </summary>
    private static string? ToExpiryIso(long expiresAtTicks) =>
        expiresAtTicks == 0L
            ? null
            : new DateTime(expiresAtTicks, DateTimeKind.Utc).ToString("O");

    private static (string TreeName, string Prefix) ResolveScope(
        string repoId, RepoContextScanScope scope, string? topic, string? pathPrefix)
    {
        switch (scope)
        {
            case RepoContextScanScope.Files:
                return (RepoContextTrees.Structural,
                    string.IsNullOrEmpty(pathPrefix)
                        ? RepoContextKeys.FilesPrefix(repoId)
                        : RepoContextKeys.FilesUnderPrefix(repoId, pathPrefix));
            case RepoContextScanScope.Packages:
                RequireNoPathPrefix(pathPrefix, scope);
                return (RepoContextTrees.Structural, RepoContextKeys.PackagesPrefix(repoId));
            case RepoContextScanScope.Symbols:
                RequireNoPathPrefix(pathPrefix, scope);
                return (RepoContextTrees.Symbol, RepoContextKeys.SymbolsPrefix(repoId));
            case RepoContextScanScope.Memory:
                RequireNoPathPrefix(pathPrefix, scope);
                return (RepoContextTrees.Memory, RepoContextKeys.MemoryPrefix(repoId));
            case RepoContextScanScope.MemoryTopic:
                RequireNoPathPrefix(pathPrefix, scope);
                RequireNonEmpty(topic, "topic");
                return (RepoContextTrees.Memory, RepoContextKeys.MemoryTopicPrefix(repoId, topic!));
            default:
                throw new McpException($"Unknown scan scope '{scope}'.");
        }
    }

    private static void RequireNoPathPrefix(string? pathPrefix, RepoContextScanScope scope)
    {
        if (!string.IsNullOrEmpty(pathPrefix))
        {
            throw new McpException($"A path prefix is only supported for the Files scope, not {scope}.");
        }
    }

    private static int ClampPageSize(int pageSize)
        => pageSize <= 0 ? DefaultPageSize : Math.Min(pageSize, MaxPageSize);

    private static RepoContextKey ParseKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key) || !RepoContextKeys.TryParse(key, out var parsed))
        {
            throw new McpException(
                $"The key '{key}' is not a well-formed repository-context key (expected 'repo/{{repoId}}/...').");
        }

        return parsed;
    }

    private static void RequireNonEmpty(string? value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new McpException($"The '{parameterName}' parameter is required and must be non-empty.");
        }
    }
}
