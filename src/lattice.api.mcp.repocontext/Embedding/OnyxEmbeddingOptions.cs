namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Configuration for the default <see cref="OnyxEmbeddingProvider"/>: the client
/// for the companion Onyx model-server embedding container. A host populates
/// these through
/// <see cref="LatticeMcpRepoContextEmbeddingServiceCollectionExtensions.AddOnyxEmbeddingProvider"/>;
/// the provider reads them once when it constructs its
/// <see cref="EmbeddingSpace"/> and on every embed call.
/// </summary>
/// <remarks>
/// The defaults match the model and endpoint baked into the shipped
/// <c>apps/embedding</c> image (Onyx's <c>nomic-ai/nomic-embed-text-v1</c>
/// default document encoder, 768-dimensional, 512-token context, L2-normalized).
/// Changing <see cref="ModelName"/> or <see cref="Dimension"/> selects a new
/// embedding space and must be paired with the matching model in the container.
/// </remarks>
public sealed class OnyxEmbeddingOptions
{
    /// <summary>
    /// The default model id baked into the companion image: Onyx's default
    /// document encoder, <c>nomic-ai/nomic-embed-text-v1</c>.
    /// </summary>
    public const string DefaultModelName = "nomic-ai/nomic-embed-text-v1";

    /// <summary>
    /// The default vector dimension of <see cref="DefaultModelName"/> (<c>768</c>).
    /// </summary>
    public const int DefaultDimension = 768;

    /// <summary>
    /// The default maximum context length (in tokens) of
    /// <see cref="DefaultModelName"/> (<c>512</c>); longer inputs are truncated by
    /// the model server.
    /// </summary>
    public const int DefaultMaxContextLength = 512;

    /// <summary>
    /// The model server's default listen address on the compose network:
    /// <c>http://localhost:9000</c>.
    /// </summary>
    public const string DefaultBaseAddress = "http://localhost:9000";

    /// <summary>
    /// Base address of the companion model-server container. The provider issues
    /// the health probe against <c>api/health</c> and embeds against
    /// <c>encoder/bi-encoder-embed</c> relative to this address. Defaults to
    /// <see cref="DefaultBaseAddress"/>; point it at the managed or external
    /// endpoint in a cloud deployment.
    /// </summary>
    public Uri BaseAddress { get; set; } = new(DefaultBaseAddress);

    /// <summary>
    /// The HuggingFace model id the server should embed with. Must match a model
    /// baked into (or mounted into) the container. Defaults to
    /// <see cref="DefaultModelName"/>.
    /// </summary>
    public string ModelName { get; set; } = DefaultModelName;

    /// <summary>
    /// The dimension of the vectors <see cref="ModelName"/> produces, used to
    /// build the provider's <see cref="EmbeddingSpace"/> and to fail-closed-reject
    /// any response whose vectors are a different length. Defaults to
    /// <see cref="DefaultDimension"/>.
    /// </summary>
    public int Dimension { get; set; } = DefaultDimension;

    /// <summary>
    /// The maximum context length (in tokens) sent with each embed request.
    /// Defaults to <see cref="DefaultMaxContextLength"/>.
    /// </summary>
    public int MaxContextLength { get; set; } = DefaultMaxContextLength;

    /// <summary>
    /// Whether the server should L2-normalize the returned vectors. Reflected in
    /// the provider's <see cref="EmbeddingSpace.Normalized"/>. Defaults to
    /// <see langword="true"/>.
    /// </summary>
    public bool NormalizeEmbeddings { get; set; } = true;

    /// <summary>
    /// Optional per-request timeout applied to the underlying HTTP client. When
    /// <see langword="null"/> the ambient <see cref="System.Net.Http.HttpClient"/>
    /// default is used. A timeout elapsing is a fail-closed failure, not an
    /// exception surfaced to the caller.
    /// </summary>
    public TimeSpan? RequestTimeout { get; set; }
}
