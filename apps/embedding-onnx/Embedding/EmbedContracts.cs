using System.Text.Json.Serialization;

namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The embed request body, matching the Onyx model server's internal
/// <c>EmbedRequest</c> shape field for field so this server is a drop-in for the
/// <c>OnyxEmbeddingProvider</c> client without changing a line of that client.
/// </summary>
/// <remarks>
/// <c>model_name</c> and <c>provider_type</c> are accepted and ignored: this
/// server hosts exactly one baked local model, so there is nothing to select and
/// no remote provider to route to. Accepting them keeps the wire contract
/// identical rather than rejecting a request the Onyx image would have served.
/// </remarks>
internal sealed record EmbedRequest
{
    /// <summary>The texts to embed, in the order the vectors are wanted back.</summary>
    [JsonPropertyName("texts")]
    public IReadOnlyList<string>? Texts { get; init; }

    /// <summary>Accepted and ignored; the image hosts a single baked model.</summary>
    [JsonPropertyName("model_name")]
    public string? ModelName { get; init; }

    /// <summary>The requested token ceiling, clamped to the server's own maximum.</summary>
    [JsonPropertyName("max_context_length")]
    public int MaxContextLength { get; init; }

    /// <summary>Whether to L2-normalize the returned vectors.</summary>
    [JsonPropertyName("normalize_embeddings")]
    public bool NormalizeEmbeddings { get; init; }

    /// <summary>
    /// The text role (<c>passage</c> or <c>query</c>). Accepted for wire
    /// compatibility; no asymmetric prefix is applied, which matches the Onyx
    /// server's behaviour for this client (see the README).
    /// </summary>
    [JsonPropertyName("text_type")]
    public string? TextType { get; init; }

    /// <summary>Accepted and ignored; this endpoint serves the local model only.</summary>
    [JsonPropertyName("provider_type")]
    public string? ProviderType { get; init; }
}

/// <summary>
/// The embed response body: one vector per input text, in input order.
/// </summary>
/// <param name="Embeddings">The embedding vectors.</param>
internal sealed record EmbedResponse(
    [property: JsonPropertyName("embeddings")] IReadOnlyList<float[]> Embeddings);

/// <summary>
/// The health probe response. The <c>status</c> field mirrors the Onyx server's
/// health shape; <c>provider</c> and <c>model</c> are additive diagnostics that
/// let an operator confirm which accelerator actually bound without reading
/// logs.
/// </summary>
/// <param name="Status">Literal <c>ok</c> when the model is loaded and serving.</param>
/// <param name="Provider">The ONNX Runtime execution provider in use.</param>
/// <param name="Model">The file name of the loaded ONNX model.</param>
/// <param name="Dimension">The vector dimension the loaded model produces.</param>
internal sealed record HealthResponse(
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("provider")] string Provider,
    [property: JsonPropertyName("model")] string Model,
    [property: JsonPropertyName("dimension")] int Dimension);
