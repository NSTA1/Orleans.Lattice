namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of an <see cref="IEmbeddingProvider.EmbedAsync"/> call. The seam
/// is deliberately fail-closed and honest: on any failure - an unreachable
/// provider, a transport error, a non-success response, or a response whose shape
/// does not match the provider's declared <see cref="EmbeddingSpace"/> - it
/// returns an unsuccessful result carrying a clear <see cref="Error"/> and
/// <b>no</b> vectors, so a caller can fall back to structural or keyword recall
/// rather than silently indexing or searching with unembedded or wrong-space
/// vectors.
/// </summary>
public sealed record EmbeddingResult
{
    private EmbeddingResult(
        bool succeeded,
        EmbeddingSpace space,
        IReadOnlyList<ReadOnlyMemory<float>> vectors,
        string? error)
    {
        Succeeded = succeeded;
        Space = space;
        Vectors = vectors;
        Error = error;
    }

    /// <summary>
    /// Whether the embedding call succeeded. When <see langword="false"/> the
    /// caller must treat <see cref="Vectors"/> as empty and fall back; the reason
    /// is in <see cref="Error"/>.
    /// </summary>
    public bool Succeeded { get; }

    /// <summary>
    /// The embedding space the produced vectors belong to (the same space the
    /// provider advertises via <see cref="IEmbeddingProvider.Space"/>). Present on
    /// both success and failure so a caller always knows which space was attempted.
    /// </summary>
    public EmbeddingSpace Space { get; }

    /// <summary>
    /// The produced vectors, one per input text in input order, each of length
    /// <see cref="EmbeddingSpace.Dimension"/>. Empty when <see cref="Succeeded"/>
    /// is <see langword="false"/>.
    /// </summary>
    public IReadOnlyList<ReadOnlyMemory<float>> Vectors { get; }

    /// <summary>
    /// A human-readable description of why the call failed, or
    /// <see langword="null"/> when <see cref="Succeeded"/> is
    /// <see langword="true"/>. Never carries secret material.
    /// </summary>
    public string? Error { get; }

    /// <summary>
    /// Creates a successful result carrying the produced vectors.
    /// </summary>
    /// <param name="space">The space the vectors belong to.</param>
    /// <param name="vectors">The produced vectors, one per input text.</param>
    /// <returns>A successful <see cref="EmbeddingResult"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="space"/> or
    /// <paramref name="vectors"/> is null.</exception>
    public static EmbeddingResult Success(
        EmbeddingSpace space, IReadOnlyList<ReadOnlyMemory<float>> vectors)
    {
        ArgumentNullException.ThrowIfNull(space);
        ArgumentNullException.ThrowIfNull(vectors);
        return new EmbeddingResult(succeeded: true, space, vectors, error: null);
    }

    /// <summary>
    /// Creates a failed, fail-closed result carrying no vectors and a clear error.
    /// </summary>
    /// <param name="space">The space that was attempted.</param>
    /// <param name="error">A human-readable reason for the failure.</param>
    /// <returns>An unsuccessful <see cref="EmbeddingResult"/> with an empty
    /// <see cref="Vectors"/> list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="space"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="error"/> is null or
    /// whitespace.</exception>
    public static EmbeddingResult Failure(EmbeddingSpace space, string error)
    {
        ArgumentNullException.ThrowIfNull(space);
        if (string.IsNullOrWhiteSpace(error))
        {
            throw new ArgumentException(
                "A failed embedding result must carry a non-empty error.", nameof(error));
        }

        return new EmbeddingResult(
            succeeded: false, space, Array.Empty<ReadOnlyMemory<float>>(), error);
    }
}
