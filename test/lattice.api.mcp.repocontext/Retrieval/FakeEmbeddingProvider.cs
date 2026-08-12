namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// A deterministic in-memory <see cref="IEmbeddingProvider"/> test double so
/// ranking, ingestion, and search tests run without the Onyx model-server
/// container. It embeds text with a stable bag-of-tokens hash into a fixed number
/// of dimensions and L2-normalizes the result, so two texts that share tokens have
/// a high cosine similarity - which lets a semantic-search test assert meaningful
/// ranking. Availability and per-call success are switchable so a test can drive
/// the fail-closed degradation path.
/// </summary>
internal sealed class FakeEmbeddingProvider : IEmbeddingProvider
{
    private readonly int _dimension;

    /// <summary>Creates the fake provider.</summary>
    /// <param name="dimension">The vector dimension. Defaults to 16.</param>
    /// <param name="modelId">The model id stamped on the space. Defaults to a fixed test id.</param>
    public FakeEmbeddingProvider(int dimension = 16, string modelId = "fake-embed-v1")
    {
        _dimension = dimension;
        Space = new EmbeddingSpace(modelId, dimension, normalized: true);
    }

    /// <inheritdoc />
    public EmbeddingSpace Space { get; }

    /// <summary>Whether <see cref="IsAvailableAsync"/> reports the provider ready.</summary>
    public bool Available { get; set; } = true;

    /// <summary>When true, every <see cref="EmbedAsync"/> call returns a fail-closed unsuccessful result.</summary>
    public bool FailEmbeds { get; set; }

    /// <inheritdoc />
    public Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(Available);

    /// <inheritdoc />
    public Task<EmbeddingResult> EmbedAsync(
        IReadOnlyList<string> texts, EmbeddingTextType textType, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(texts);
        if (FailEmbeds)
        {
            return Task.FromResult(EmbeddingResult.Failure(Space, "The fake embedder was configured to fail."));
        }

        var vectors = new List<ReadOnlyMemory<float>>(texts.Count);
        foreach (var text in texts)
        {
            vectors.Add(Embed(text));
        }

        return Task.FromResult(EmbeddingResult.Success(Space, vectors));
    }

    /// <summary>Deterministically embeds one text (exposed for direct ranking assertions).</summary>
    /// <param name="text">The text to embed.</param>
    /// <returns>The L2-normalized embedding.</returns>
    public float[] Embed(string text)
    {
        var vector = new float[_dimension];
        foreach (var token in Tokenize(text))
        {
            var bucket = (int)(Fnv1a(token) % (uint)_dimension);
            vector[bucket] += 1f;
        }

        Normalize(vector);
        return vector;
    }

    private static IEnumerable<string> Tokenize(string text)
    {
        var start = -1;
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsLetterOrDigit(text[i]))
            {
                if (start < 0)
                {
                    start = i;
                }
            }
            else if (start >= 0)
            {
                yield return text[start..i].ToLowerInvariant();
                start = -1;
            }
        }

        if (start >= 0)
        {
            yield return text[start..].ToLowerInvariant();
        }
    }

    private static void Normalize(float[] vector)
    {
        double magnitude = 0;
        foreach (var component in vector)
        {
            magnitude += (double)component * component;
        }

        if (magnitude == 0)
        {
            return;
        }

        var scale = 1.0 / Math.Sqrt(magnitude);
        for (var i = 0; i < vector.Length; i++)
        {
            vector[i] = (float)(vector[i] * scale);
        }
    }

    private static uint Fnv1a(string token)
    {
        uint hash = 2166136261;
        foreach (var ch in token)
        {
            hash ^= ch;
            hash *= 16777619;
        }

        return hash;
    }
}
