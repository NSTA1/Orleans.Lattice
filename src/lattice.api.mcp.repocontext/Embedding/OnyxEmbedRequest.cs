using System.Text.Json.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The request body for Onyx's model-server embed endpoint
/// (<c>POST /encoder/bi-encoder-embed</c>). This mirrors the model server's
/// internal, versioned contract - not a stable public API - so it is wrapped
/// behind <see cref="OnyxEmbeddingProvider"/> and pinned to the image tag; a
/// schema change on the server is a single-file update here.
/// </summary>
/// <remarks>
/// <para><c>provider_type</c> is deliberately always null: this endpoint is for
/// the server's <b>local</b> model, and a non-null provider makes the server
/// reject the request. No <c>api_key</c> is ever sent - the local model needs
/// none, and the field is omitted so no secret can leak onto the wire.</para>
/// </remarks>
internal sealed record OnyxEmbedRequest
{
    /// <summary>The texts to embed, in the order the vectors are wanted back.</summary>
    [JsonPropertyName("texts")]
    public required IReadOnlyList<string> Texts { get; init; }

    /// <summary>The HuggingFace model id the server should embed with.</summary>
    [JsonPropertyName("model_name")]
    public required string ModelName { get; init; }

    /// <summary>The maximum context length (tokens) the server applies.</summary>
    [JsonPropertyName("max_context_length")]
    public required int MaxContextLength { get; init; }

    /// <summary>Whether the server should L2-normalize the returned vectors.</summary>
    [JsonPropertyName("normalize_embeddings")]
    public required bool NormalizeEmbeddings { get; init; }

    /// <summary>
    /// The text role - <c>passage</c> for stored chunks, <c>query</c> for search
    /// vectors - so the server applies the correct asymmetric prefix.
    /// </summary>
    [JsonPropertyName("text_type")]
    public required string TextType { get; init; }

    /// <summary>
    /// Always <see langword="null"/>: this endpoint serves the local model only.
    /// Serialized so the field is explicit on the wire.
    /// </summary>
    [JsonPropertyName("provider_type")]
    public string? ProviderType { get; init; }
}
