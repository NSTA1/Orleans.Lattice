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
internal sealed class RepoContextStore
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

    /// <summary>Creates the capture/maintenance adapter.</summary>
    /// <param name="grainFactory">The grain factory used to reach the named Lattice trees. Must not be <see langword="null"/>.</param>
    /// <param name="indexRunner">The indexing runner, used to drain an in-flight index to a halt before a repository's records are removed. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode and re-encode records. Must not be <see langword="null"/>.</param>
    /// <param name="vectorWriter">The vector writer that owns the membership layout, used to read the durable embedded-source count. Must not be <see langword="null"/>.</param>
    /// <param name="ttlOptions">The per-repository TTL policy. Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock used to project remaining life. Must not be <see langword="null"/>.</param>
    public RepoContextStore(
        IGrainFactory grainFactory,
        IRepoIndexRunner indexRunner,
        Serializer serializer,
        RepoContextVectorWriter vectorWriter,
        IOptionsMonitor<RepoContextTtlOptions> ttlOptions,
        TimeProvider timeProvider)
    {
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
    public async Task<RepoContextEntryView> RecallAsync(string key, CancellationToken cancellationToken)
    {
        var parsed = ParseKey(key);
        var tree = Tree(RepoContextTrees.ForKind(parsed.Kind));

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        var life = RepoContextRemainingLife.FromVersionedValue(versioned, _timeProvider.GetUtcNow().UtcDateTime);
        return RepoContextEntryProjection.Project(parsed, versioned.Value, _serializer, life);
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

                    var neighbor = await RecallAsync(target, cancellationToken).ConfigureAwait(false);
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

            entries.Add(RepoContextEntryProjection.Project(
                parsed, record.Value, _serializer, RepoContextRemainingLife.NeverExpires));
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
        CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");
        RequireNonEmpty(topic, "topic");

        var entryId = string.IsNullOrWhiteSpace(id) ? Guid.NewGuid().ToString("N") : id;
        var key = RepoContextKeys.Memory(repoId, topic, entryId);
        var tree = Tree(RepoContextTrees.Memory);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var existing = await tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
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

        var merged = created ? delta : MemoryRecord.Merge(delta, _serializer.Deserialize<MemoryRecord>(existing!));
        RepoContextRecordEditor.ApplyTags(merged.Tags, tags, removeTags: null);
        var (linksAdded, linksRemoved) = RepoContextRecordEditor.ApplyLinks(merged.Links, addLinks, removeLinks);
        var bytes = _serializer.SerializeToArray(merged);

        var ttl = ResolveTtl(repoId, ttlSeconds, created);
        if (ttl is { } window)
        {
            await tree.SetAsync(key, bytes, window, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await tree.SetAsync(key, bytes, cancellationToken).ConfigureAwait(false);
        }

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        return new RepoContextRememberResult
        {
            Key = key,
            RepoId = repoId,
            Topic = topic,
            Id = entryId,
            Created = created,
            Expires = versioned.ExpiresAtTicks != 0L,
            ExpiresAtTicks = versioned.ExpiresAtTicks,
            LinksAdded = linksAdded,
            LinksRemoved = linksRemoved,
        };
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
    public async Task<RepoContextUpdateResult> UpdateAsync(
        string key,
        IReadOnlyDictionary<string, string>? fields,
        IReadOnlyList<string>? addTags,
        IReadOnlyList<string>? removeTags,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? addLinks,
        IReadOnlyDictionary<string, IReadOnlyList<string>>? removeLinks,
        CancellationToken cancellationToken)
    {
        var parsed = ParseKey(key);
        var tree = Tree(RepoContextTrees.ForKind(parsed.Kind));

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        if (versioned.Value is not { } existing)
        {
            throw new McpException(
                $"No record exists at '{key}'. Use repocontext_remember or repocontext_bootstrap to create it first.");
        }

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var patch = RepoContextRecordEditor.Patch(
            parsed, existing, fields, addTags, removeTags, addLinks, removeLinks, clock, _serializer);

        var remainingTtl = RemainingTtl(versioned.ExpiresAtTicks);
        if (remainingTtl is { } window)
        {
            await tree.SetAsync(key, patch.Merged, window, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await tree.SetAsync(key, patch.Merged, cancellationToken).ConfigureAwait(false);
        }

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
    public async Task<RepoContextForgetResult> ForgetAsync(
        string key,
        bool lapse,
        long? lapseSeconds,
        CancellationToken cancellationToken)
    {
        var parsed = ParseKey(key);
        var tree = Tree(RepoContextTrees.ForKind(parsed.Kind));

        if (!lapse)
        {
            var deleted = await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            return new RepoContextForgetResult
            {
                Key = key,
                Mode = "delete",
                Existed = deleted,
                ExpiresAtTicks = 0L,
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
                ExpiresAtTicks = 0L,
            };
        }

        await tree.SetAsync(key, value, TimeSpan.FromSeconds(seconds), cancellationToken).ConfigureAwait(false);
        var lapsed = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        return new RepoContextForgetResult
        {
            Key = key,
            Mode = "lapse",
            Existed = true,
            ExpiresAtTicks = lapsed.ExpiresAtTicks,
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
        var namespacePrefix = RepoContextKeys.AllReposPrefix();
        var namespaceEnd = RepoContextPortability.PrefixUpperBound(namespacePrefix);
        var summaries = new List<RepoContextRepoSummary>();
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
                summaries.Add(await BuildRepoSummaryAsync(tree, repoId, cancellationToken).ConfigureAwait(false));
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

        return new RepoContextRepoListResult { Repos = summaries, Count = summaries.Count };
    }

    /// <summary>
    /// Removes every record for a repository: a resilient range-delete drain
    /// tombstones each context tree's <c>repo/{repoId}/</c> subtree in bounded
    /// steps, reopening a fresh cursor across a transient enumerator loss so the
    /// whole subtree is drained rather than aborting part-way, then the bare
    /// <c>repo/{repoId}</c> root marker is deleted from the structural tree.
    /// Removing an absent repository is a no-op that reports zero deletions.
    /// </summary>
    /// <param name="repoId">The repository whose records to remove. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the removal between steps.</param>
    /// <returns>The repository id and the number of entries tombstoned.</returns>
    /// <exception cref="McpException">The repository id is empty.</exception>
    public async Task<RepoContextRepoRemovalResult> RemoveRepoAsync(string repoId, CancellationToken cancellationToken)
    {
        RequireNonEmpty(repoId, "repoId");

        // Stop any in-flight indexing run and drain it to a full halt BEFORE
        // deleting a single record. CancelAndWaitAsync cancels the run and awaits
        // its termination, so no concurrent structural write from the indexer can
        // race the range-delete below - a race that otherwise surfaces as an
        // Orleans state version conflict on a leaf shared by both writers. The job
        // grain then unregisters its resume reminder and clears its durable state,
        // so a removed repository leaves no reminder firing forever and no job
        // state for a later start to resume. Doing this first (rather than last)
        // also means an error in the delete pass can no longer skip the cleanup.
        await _indexRunner.CancelAndWaitAsync(repoId).ConfigureAwait(false);
        await _grainFactory.GetGrain<IRepoIndexJobGrain>(repoId)
            .CancelAndClearAsync()
            .ConfigureAwait(false);

        // Tear down the repository's always-on self-index scan so a removed
        // repository leaves no keep-alive reminder firing and no checkpoint behind.
        await _grainFactory.GetGrain<IRepoContextSelfIndexGrain>(repoId)
            .StopAsync()
            .ConfigureAwait(false);

        var scanPrefix = RepoContextKeys.RepoScanPrefix(repoId);
        var end = RepoContextPortability.PrefixUpperBound(scanPrefix)
            ?? throw new McpException("The repository id produced an unbounded delete range.");

        long deleted = 0;
        foreach (var treeName in RepoContextTrees.All)
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

    private async Task<RepoContextRepoSummary> BuildRepoSummaryAsync(
        ILattice structural, string repoId, CancellationToken cancellationToken)
    {
        string? lastIngested = null;
        long? fileCount = null;

        var markerBytes = await structural
            .GetAsync(RepoContextKeys.Repo(repoId), cancellationToken)
            .ConfigureAwait(false);
        if (markerBytes is not null)
        {
            var node = _serializer.Deserialize<RepoNode>(markerBytes);
            lastIngested = RepoContextValues.ReadString(node.LastIngested);
            fileCount = RepoContextValues.ReadInt64(node.FileCount);
        }

        var embeddedVectorCount = await ReadEmbeddedVectorCountAsync(repoId, cancellationToken)
            .ConfigureAwait(false);

        return new RepoContextRepoSummary
        {
            RepoId = repoId,
            LastIngested = lastIngested,
            FileCount = fileCount,
            EmbeddedVectorCount = embeddedVectorCount,
        };
    }

    /// <summary>
    /// Reads the durable count of sources with a live embedding for a repository: the
    /// number of live presence keys in the vector-membership tree that the vector
    /// writer maintains as embeddings land. A source is a file or a captured symbol, so
    /// this counts embedded files plus embedded symbols. It is read from the store of
    /// record (the vector-membership tree), never from a run's in-flight progress, so
    /// it is a restart-durable diagnostic. Returns <c>0</c> when nothing has been
    /// embedded yet.
    /// </summary>
    private Task<long> ReadEmbeddedVectorCountAsync(string repoId, CancellationToken cancellationToken) =>
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
