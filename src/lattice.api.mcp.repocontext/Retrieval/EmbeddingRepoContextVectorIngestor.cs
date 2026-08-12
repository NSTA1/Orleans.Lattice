using System.IO;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The real bootstrap-time vectorisation seam: the
/// <see cref="IRepoContextVectorIngestor"/> that embeds the files a bootstrap run
/// added or updated and stores their vectors on the reserved vector trees. It
/// replaces the default <see cref="NoOpRepoContextVectorIngestor"/> so a run wires
/// straight through from the structural walk to a searchable semantic index, with
/// no change to the bootstrap coordinator or the tool.
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
    /// The maximum number of characters of a file's content that are embedded. A
    /// file longer than this is truncated to its leading window, which bounds the
    /// per-embed request size and keeps a single large file from dominating a run.
    /// </summary>
    internal const int MaxEmbedChars = 8192;

    /// <summary>
    /// The maximum number of files embedded in a single request to the provider.
    /// A bootstrap run over a real repository has hundreds or thousands of files;
    /// embedding them all in one call builds a multi-megabyte request that can
    /// exceed the provider's HTTP timeout and fail-close the whole run. Chunking
    /// bounds each request's size and duration and lets vectors land
    /// incrementally, so a slow or partial provider still yields a searchable
    /// index instead of nothing.
    /// </summary>
    internal const int EmbedBatchSize = 32;

    private readonly RepoContextVectorWriter _writer;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly ILogger<EmbeddingRepoContextVectorIngestor> _logger;

    /// <summary>Creates the embedding vector ingestor.</summary>
    /// <param name="writer">The writer that persists vectors onto the reserved trees. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger used to record fail-closed fallbacks. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (search then degrades to keyword recall).</param>
    /// <exception cref="ArgumentNullException"><paramref name="writer"/> or <paramref name="logger"/> is null.</exception>
    public EmbeddingRepoContextVectorIngestor(
        RepoContextVectorWriter writer,
        ILogger<EmbeddingRepoContextVectorIngestor> logger,
        IEmbeddingProvider? embeddingProvider = null)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(logger);
        _writer = writer;
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

        var sourceKeys = new List<string>(toEmbed.Count);
        var texts = new List<string>(toEmbed.Count);
        foreach (var file in toEmbed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var text = await ReadContentAsync(repoRoot, file.RelativePath, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(text))
            {
                // Skip a file that could not be read (null) or that carries no
                // embeddable content (empty or whitespace-only). An empty string
                // is not merely useless to embed: the embedding server rejects a
                // batch that contains one, which would fail-close the whole run's
                // vectorisation. A contentless file still has its structural
                // record from the walk, so keyword recall keeps covering it.
                continue;
            }

            sourceKeys.Add(RepoContextKeys.File(repoId, file.RelativePath));
            texts.Add(text);
        }

        if (texts.Count == 0)
        {
            return 0;
        }

        // Embed and store in bounded chunks. Each chunk is an independent request,
        // so a large repository never builds one oversized, slow call that trips
        // the provider's HTTP timeout, and vectors from earlier chunks survive
        // even if a later chunk's embed fails - search then covers whatever landed
        // and degrades to keyword recall only for the remainder.
        var embedded = 0;
        for (var start = 0; start < texts.Count; start += EmbedBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(EmbedBatchSize, texts.Count - start);
            var batchTexts = texts.GetRange(start, count);

            var result = await _embeddingProvider
                .EmbedAsync(batchTexts, EmbeddingTextType.Passage, cancellationToken)
                .ConfigureAwait(false);
            if (!result.Succeeded || result.Vectors.Count != batchTexts.Count)
            {
                _logger.LogInformation(
                    "Bootstrap vectorisation for repository {RepoId} skipped a batch of {Count} file(s): the embedding call did not succeed ({Error}). Those files fall back to keyword recall.",
                    repoId,
                    count,
                    result.Error ?? "no vectors returned");
                continue;
            }

            for (var i = 0; i < count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await _writer
                    .StoreAsync(repoId, sourceKeys[start + i], result.Space, result.Vectors[i], cancellationToken)
                    .ConfigureAwait(false);
            }

            // Record membership for the whole batch in one read-modify-write after
            // its vectors have landed, rather than folding presence per file. This
            // flush is per batch, not once at the end, so an interruption leaves at
            // most this batch's vectors unrecorded - they are simply re-embedded on
            // the next pass - while presence still implies a durable vector.
            await _writer
                .AddMembersAsync(repoId, sourceKeys.GetRange(start, count), cancellationToken)
                .ConfigureAwait(false);

            embedded += count;

            // Surface incremental progress after each batch lands, so a long
            // vectorisation pass (hundreds or thousands of files, embedded on CPU)
            // reports a rising count instead of appearing frozen until it finishes.
            if (onProgress is not null)
            {
                await onProgress(embedded, cancellationToken).ConfigureAwait(false);
            }
        }

        if (embedded == 0)
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: no embedding batch succeeded. Search will use keyword recall.",
                repoId);
        }

        return embedded;
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
        var probe = new byte[VectorCodec.SourceIdByteLength];
        foreach (var file in unchangedFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sourceId = VectorCodec.SourceId(RepoContextKeys.File(repoId, file.RelativePath));
            System.Text.Encoding.UTF8.GetBytes(sourceId, probe);
            if (!embedded.Contains(probe))
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
}
