using System.IO;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The real bootstrap-time vectorisation seam: the
/// <see cref="IRepoContextVectorIngestor"/> that embeds the files a bootstrap run
/// added or updated - and the per-symbol records the reconcile captured - and
/// stores their vectors on the reserved vector trees. It replaces the default
/// <see cref="NoOpRepoContextVectorIngestor"/> so a run wires straight through from
/// the structural walk to a searchable semantic index, with no change to the tool.
/// <para>
/// <b>Chunked and symbol-granular.</b> A file is embedded as several overlapping
/// windows (see <see cref="RepoContextTextChunker"/>) rather than one leading-window
/// vector, so content deep in a large file is searchable; each window is a passage
/// whose canonical record is the file, so a hit hydrates and de-duplicates to the
/// file. A symbol is embedded as its own single passage (kind, name, and signature)
/// so a symbol-level query lands on the declaring symbol.
/// </para>
/// <para>
/// <b>Fail-closed and honest.</b> When no <see cref="IEmbeddingProvider"/> is
/// configured, the provider is unreachable
/// (<see cref="IEmbeddingProvider.IsAvailableAsync"/> is false), or an embed call
/// returns an unsuccessful <see cref="EmbeddingResult"/>, the ingestor simply
/// records nothing and returns - it never throws out of a bootstrap run and never
/// stores an unembedded or wrong-space vector. Search then degrades to structural
/// or keyword recall over the records the structural walk already captured.
/// </para>
/// </summary>
internal sealed class EmbeddingRepoContextVectorIngestor : IRepoContextVectorIngestor
{
    /// <summary>
    /// The maximum number of characters of a file's content that are read for
    /// embedding. A file longer than this is truncated to its leading window before
    /// chunking, which bounds the memory a single very large file uses; the
    /// chunker's own per-file window cap bounds how many of those characters are
    /// actually embedded.
    /// </summary>
    internal const int MaxEmbedChars = 64 * 1024;

    /// <summary>
    /// The maximum number of passages embedded in a single request to the provider.
    /// A bootstrap run over a real repository has thousands of passages once files
    /// are chunked and symbols are embedded; sending them all in one call builds a
    /// multi-megabyte request that can exceed the provider's HTTP timeout and
    /// fail-close the whole run. Batching bounds each request's size and duration
    /// and lets vectors land incrementally, so a slow or partial provider still
    /// yields a searchable index instead of nothing.
    /// </summary>
    internal const int EmbedBatchSize = 32;

    private readonly RepoContextVectorWriter _writer;
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly ILogger<EmbeddingRepoContextVectorIngestor> _logger;

    /// <summary>Creates the embedding vector ingestor.</summary>
    /// <param name="writer">The writer that persists vectors onto the reserved trees. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to enumerate the symbol tree for symbol embedding. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode symbol records during symbol embedding. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger used to record fail-closed fallbacks. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (search then degrades to keyword recall).</param>
    /// <exception cref="ArgumentNullException"><paramref name="writer"/>, <paramref name="grainFactory"/>, <paramref name="serializer"/>, or <paramref name="logger"/> is null.</exception>
    public EmbeddingRepoContextVectorIngestor(
        RepoContextVectorWriter writer,
        IGrainFactory grainFactory,
        Serializer serializer,
        ILogger<EmbeddingRepoContextVectorIngestor> logger,
        IEmbeddingProvider? embeddingProvider = null)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);
        _writer = writer;
        _grainFactory = grainFactory;
        _serializer = serializer;
        _logger = logger;
        _embeddingProvider = embeddingProvider;
    }

    /// <inheritdoc />
    public async ValueTask<int> IngestAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> changedFiles,
        IReadOnlyList<RepoFileEntry> unchangedFiles,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(repoRoot);
        ArgumentNullException.ThrowIfNull(changedFiles);
        ArgumentNullException.ThrowIfNull(unchangedFiles);

        // Gate on the provider before any membership read: with no provider there
        // is nothing to embed and no reason to load the membership set, so an
        // unchanged file is never falsely flagged as missing an embedding.
        if (_embeddingProvider is null || (changedFiles.Count == 0 && unchangedFiles.Count == 0))
        {
            return 0;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: the embedding provider is unavailable. Search will use keyword recall.",
                repoId);
            return 0;
        }

        var toEmbed = await SelectFilesToEmbedAsync(repoId, changedFiles, unchangedFiles, cancellationToken)
            .ConfigureAwait(false);
        if (toEmbed.Count == 0)
        {
            return 0;
        }

        var sources = new List<EmbeddingSource>(toEmbed.Count);
        foreach (var file in toEmbed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var text = await ReadContentAsync(repoRoot, file.RelativePath, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(text))
            {
                // Skip a file that could not be read (null) or that carries no
                // embeddable content. A contentless file still has its structural
                // record from the walk, so keyword recall keeps covering it.
                continue;
            }

            var windows = RepoContextTextChunker.Chunk(text);
            if (windows.Count == 0)
            {
                continue;
            }

            sources.Add(new EmbeddingSource(RepoContextKeys.File(repoId, file.RelativePath), windows));
        }

        var embedded = await EmbedAndStoreAsync(repoId, sources, onProgress, cancellationToken)
            .ConfigureAwait(false);

        if (embedded == 0 && sources.Count > 0)
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: no embedding batch succeeded. Search will use keyword recall.",
                repoId);
        }

        return embedded;
    }

    /// <inheritdoc />
    public async Task<int> IngestSymbolsAsync(
        string repoId,
        IReadOnlyCollection<string> changedSymbolKeys,
        IReadOnlyCollection<string> prunedSymbolKeys,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(changedSymbolKeys);
        ArgumentNullException.ThrowIfNull(prunedSymbolKeys);

        // Retire a pruned symbol's embedding regardless of the provider: a symbol
        // the reconcile removed must drop its vector, or the membership count drifts
        // high. Retirement only deletes stored records, so it needs no embedder.
        foreach (var key in prunedSymbolKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _writer.RetireAsync(repoId, key, cancellationToken).ConfigureAwait(false);
        }

        if (_embeddingProvider is null)
        {
            return 0;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping symbol vectorisation for repository {RepoId}: the embedding provider is unavailable. Search will use keyword recall.",
                repoId);
            return 0;
        }

        // A symbol is (re-)embedded when its declaration changed this pass or when
        // it has no live embedding yet (a new symbol, or a back-fill of symbols
        // captured before symbol embedding existed). Presence is judged from the
        // add-wins membership set, probed in memory, so an already-embedded,
        // unchanged symbol is skipped without a read per symbol.
        var changed = new HashSet<string>(changedSymbolKeys, StringComparer.Ordinal);
        var embeddedMembers = await _writer.LoadEmbeddedMembersAsync(repoId, cancellationToken).ConfigureAwait(false);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var prefix = RepoContextKeys.SymbolsPrefix(repoId);
        var sources = new List<EmbeddingSource>();

        string? token = null;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            foreach (var record in page.Records)
            {
                if (record.Value is null)
                {
                    continue;
                }

                var sourceKey = record.Key;
                if (!changed.Contains(sourceKey) && embeddedMembers.Contains(VectorCodec.SourceId(sourceKey)))
                {
                    continue;
                }

                var text = BuildSymbolText(_serializer.Deserialize<SymbolRecord>(record.Value));
                if (string.IsNullOrWhiteSpace(text))
                {
                    continue;
                }

                sources.Add(new EmbeddingSource(sourceKey, new[] { text }));
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        return await EmbedAndStoreAsync(repoId, sources, onProgress: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Embeds every source's passages in bounded batches and stores each source's
    /// vectors as a unit once all of its passages have landed. Batching is flat
    /// across sources, so a call that mixes many small files with a few large ones
    /// still packs full requests; a source whose passages span a failed and a
    /// succeeded batch is left incomplete and re-embedded on the next pass, so a
    /// stored source always carries its whole current passage set. Membership is
    /// recorded after each batch, so an interruption leaves at most one batch of
    /// vectors unrecorded while presence still implies a durable vector.
    /// </summary>
    private async Task<int> EmbedAndStoreAsync(
        string repoId,
        IReadOnlyList<EmbeddingSource> sources,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken)
    {
        if (sources.Count == 0)
        {
            return 0;
        }

        // Flatten every source's passages into one unit list, remembering each
        // unit's owning source and slot, so a source's vectors can be reassembled
        // in order once its units land - even across batch boundaries.
        var unitTexts = new List<string>();
        var unitOwner = new List<int>();
        var unitSlot = new List<int>();
        var slots = new ReadOnlyMemory<float>[sources.Count][];
        var filled = new int[sources.Count];
        var spaces = new EmbeddingSpace?[sources.Count];
        for (var s = 0; s < sources.Count; s++)
        {
            var units = sources[s].Units;
            slots[s] = new ReadOnlyMemory<float>[units.Count];
            for (var u = 0; u < units.Count; u++)
            {
                unitTexts.Add(units[u]);
                unitOwner.Add(s);
                unitSlot.Add(u);
            }
        }

        var embedded = 0;
        var pendingMembers = new List<string>();
        for (var start = 0; start < unitTexts.Count; start += EmbedBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(EmbedBatchSize, unitTexts.Count - start);
            var batchTexts = unitTexts.GetRange(start, count);

            var result = await _embeddingProvider!
                .EmbedAsync(batchTexts, EmbeddingTextType.Passage, cancellationToken)
                .ConfigureAwait(false);
            if (!result.Succeeded || result.Vectors.Count != count)
            {
                _logger.LogInformation(
                    "Bootstrap vectorisation for repository {RepoId} skipped a batch of {Count} passage(s): the embedding call did not succeed ({Error}). Those sources fall back to keyword recall.",
                    repoId,
                    count,
                    result.Error ?? "no vectors returned");
                continue;
            }

            var completed = new List<int>();
            for (var i = 0; i < count; i++)
            {
                var owner = unitOwner[start + i];
                slots[owner][unitSlot[start + i]] = result.Vectors[i];
                spaces[owner] = result.Space;
                if (++filled[owner] == slots[owner].Length)
                {
                    completed.Add(owner);
                }
            }

            foreach (var owner in completed)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await _writer
                    .StoreAsync(repoId, sources[owner].SourceKey, spaces[owner]!, slots[owner], cancellationToken)
                    .ConfigureAwait(false);
                pendingMembers.Add(sources[owner].SourceKey);
                embedded++;
            }

            if (pendingMembers.Count > 0)
            {
                // Record membership for the sources completed in this batch in one
                // read-modify-write, after their vectors have landed.
                await _writer.AddMembersAsync(repoId, pendingMembers, cancellationToken).ConfigureAwait(false);
                pendingMembers.Clear();
            }

            // Surface incremental progress after each batch lands, so a long
            // vectorisation pass reports a rising count instead of appearing frozen.
            if (onProgress is not null)
            {
                await onProgress(embedded, cancellationToken).ConfigureAwait(false);
            }
        }

        return embedded;
    }

    /// <summary>
    /// Builds the passage text for a symbol: its kind, fully-qualified name, and -
    /// when present - its declaration signature. The name and signature carry the
    /// symbol's meaning for retrieval; the kind disambiguates a type from a member
    /// of the same name.
    /// </summary>
    private static string BuildSymbolText(SymbolRecord record)
    {
        var kind = record.Kind == SymbolKind.Unspecified
            ? "symbol"
            : record.Kind.ToString();
        var signature = RepoContextValues.ReadString(record.Signature);
        return string.IsNullOrWhiteSpace(signature)
            ? $"{kind} {record.FullyQualifiedName}"
            : $"{kind} {record.FullyQualifiedName}\n{signature}";
    }

    private static async Task<string?> ReadContentAsync(
        string repoRoot, string relativePath, CancellationToken cancellationToken)
    {
        var fullPath = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        try
        {
            var content = await File.ReadAllTextAsync(fullPath, cancellationToken).ConfigureAwait(false);
            return content.Length > MaxEmbedChars ? content[..MaxEmbedChars] : content;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <summary>
    /// Builds the list of files to embed: every changed file (its content moved,
    /// so any prior vector is stale) plus every unchanged file that has no live
    /// embedding yet. The unchanged set heals a vectorise a prior run left
    /// incomplete - the structural digest was committed but the embedding never
    /// landed - without re-embedding the files that already have a vector.
    /// <para>
    /// Presence is judged from the add-wins membership set, which holds only
    /// 16-character source identifiers and never the embeddings themselves, loaded
    /// once for the repository and probed in memory with a single reused buffer.
    /// That avoids both an existence round-trip per unchanged file and pulling any
    /// vector payload back across the grain boundary.
    /// </para>
    /// </summary>
    private async Task<List<RepoFileEntry>> SelectFilesToEmbedAsync(
        string repoId,
        IReadOnlyList<RepoFileEntry> changedFiles,
        IReadOnlyList<RepoFileEntry> unchangedFiles,
        CancellationToken cancellationToken)
    {
        var toEmbed = new List<RepoFileEntry>(changedFiles.Count + unchangedFiles.Count);
        toEmbed.AddRange(changedFiles);

        if (unchangedFiles.Count == 0)
        {
            return toEmbed;
        }

        var embedded = await _writer.LoadEmbeddedMembersAsync(repoId, cancellationToken).ConfigureAwait(false);
        foreach (var file in unchangedFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sourceId = VectorCodec.SourceId(RepoContextKeys.File(repoId, file.RelativePath));
            if (!embedded.Contains(sourceId))
            {
                toEmbed.Add(file);
            }
        }

        return toEmbed;
    }

    /// <inheritdoc />
    public async Task RetireAsync(
        string repoId,
        IReadOnlyList<string> removedPaths,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(removedPaths);

        // Retirement only deletes stored records, so it runs regardless of the
        // embedding provider: a file removed while the provider is down must still
        // drop its vector, or the membership count would drift high.
        foreach (var path in removedPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _writer
                .RetireAsync(repoId, RepoContextKeys.File(repoId, path), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A source to embed: the canonical record key its vectors hydrate to, and the
    /// ordered passages (a file's overlapping windows, or a symbol's single
    /// passage) that become its unit vectors.
    /// </summary>
    private readonly record struct EmbeddingSource(string SourceKey, IReadOnlyList<string> Units);
}
