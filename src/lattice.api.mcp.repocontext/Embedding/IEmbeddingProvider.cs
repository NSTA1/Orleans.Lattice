namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The pluggable embedding seam for the repository-context surface: text in,
/// vectors out, alongside the <see cref="EmbeddingSpace"/> the provider operates
/// in. A host binds the shipped default (a thin client for the companion Onyx
/// model-server container) or swaps in its own implementation (OpenAI, Azure
/// OpenAI, a self-hosted endpoint) via configuration, without any other part of
/// the repository-context surface changing.
/// </summary>
/// <remarks>
/// <para>
/// The contract is <b>fail-closed and honest</b>: <see cref="EmbedAsync"/> never
/// throws for an unreachable or misbehaving provider and never returns unembedded
/// or wrong-space vectors. It returns an unsuccessful <see cref="EmbeddingResult"/>
/// so a caller (bootstrap vectorisation, semantic search) can fall back to the
/// structural/keyword recall path rather than corrupting the index or a query.
/// </para>
/// <para>
/// Implementations should be safe to call concurrently: the shipped default
/// batches texts into a single request and the Onyx model server already serves
/// concurrent embed calls.
/// </para>
/// </remarks>
public interface IEmbeddingProvider
{
    /// <summary>
    /// The embedding space this provider produces vectors in - the model id,
    /// dimension, and normalization that later work stamps onto every stored
    /// vector. Stable for the lifetime of the provider.
    /// </summary>
    EmbeddingSpace Space { get; }

    /// <summary>
    /// Probes whether the provider is currently reachable and ready to serve
    /// embeddings, so a caller can decide up front between the embedding path and
    /// the fallback recall path. Fail-closed: any error or unreachable endpoint
    /// resolves to <see langword="false"/> rather than throwing.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><see langword="true"/> only when the provider answered a health
    /// probe successfully; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Embeds a batch of texts, returning one vector per input in input order.
    /// </summary>
    /// <param name="texts">The texts to embed, in the order the vectors are wanted
    /// back. May be empty (an empty batch yields an empty successful result).</param>
    /// <param name="textType">Whether the texts are stored passages or search
    /// queries, so the model applies the correct asymmetric prefix.</param>
    /// <param name="cancellationToken">Cancels the embedding call.</param>
    /// <returns>A successful <see cref="EmbeddingResult"/> carrying the vectors, or
    /// a fail-closed unsuccessful result carrying a clear error and no vectors.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="texts"/> is null.</exception>
    Task<EmbeddingResult> EmbedAsync(
        IReadOnlyList<string> texts,
        EmbeddingTextType textType,
        CancellationToken cancellationToken = default);
}
