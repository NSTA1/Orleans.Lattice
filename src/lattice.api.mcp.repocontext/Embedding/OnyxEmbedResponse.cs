using System.Text.Json.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The response body from Onyx's model-server embed endpoint
/// (<c>POST /encoder/bi-encoder-embed</c>): one float vector per input text, in
/// input order. Wrapped behind <see cref="OnyxEmbeddingProvider"/>, which
/// validates each vector's length against the configured
/// <see cref="EmbeddingSpace.Dimension"/> before surfacing it.
/// </summary>
internal sealed record OnyxEmbedResponse
{
    /// <summary>
    /// The produced vectors, one per input text. May be <see langword="null"/> if
    /// the server returns a malformed body; the provider treats that as a
    /// fail-closed failure.
    /// </summary>
    [JsonPropertyName("embeddings")]
    public float[][]? Embeddings { get; init; }
}
