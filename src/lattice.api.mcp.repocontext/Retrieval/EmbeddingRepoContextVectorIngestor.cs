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
    public async ValueTask IngestAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> changedFiles,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(repoRoot);
        ArgumentNullException.ThrowIfNull(changedFiles);

        if (_embeddingProvider is null || changedFiles.Count == 0)
        {
            return;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: the embedding provider is unavailable. Search will use keyword recall.",
                repoId);
            return;
        }

        var sourceKeys = new List<string>(changedFiles.Count);
        var texts = new List<string>(changedFiles.Count);
        foreach (var file in changedFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var text = await ReadContentAsync(repoRoot, file.RelativePath, cancellationToken).ConfigureAwait(false);
            if (text is null)
            {
                continue;
            }

            sourceKeys.Add(RepoContextKeys.File(repoId, file.RelativePath));
            texts.Add(text);
        }

        if (texts.Count == 0)
        {
            return;
        }

        var result = await _embeddingProvider
            .EmbedAsync(texts, EmbeddingTextType.Passage, cancellationToken)
            .ConfigureAwait(false);
        if (!result.Succeeded || result.Vectors.Count != texts.Count)
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: the embedding call did not succeed ({Error}). Search will use keyword recall.",
                repoId,
                result.Error ?? "no vectors returned");
            return;
        }

        for (var i = 0; i < sourceKeys.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _writer
                .StoreAsync(repoId, sourceKeys[i], result.Space, result.Vectors[i], cancellationToken)
                .ConfigureAwait(false);
        }
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
}
