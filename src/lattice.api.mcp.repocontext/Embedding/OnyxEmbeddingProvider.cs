using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IEmbeddingProvider"/>: a thin HTTP client for the
/// companion Onyx model-server embedding container. It does <b>not</b> embed
/// in-process - every call is ordinary egress to the configured
/// <see cref="OnyxEmbeddingOptions.BaseAddress"/> - so the repository-context MCP
/// host keeps its single, MCP-only listener.
/// </summary>
/// <remarks>
/// <para>
/// The provider is fail-closed and honest: a transport error, a timeout, a
/// non-success status, a malformed body, or a response whose vector count or
/// length disagrees with the configured <see cref="EmbeddingSpace"/> all resolve
/// to an unsuccessful <see cref="EmbeddingResult"/> with a clear error and no
/// vectors. It never throws for an unreachable or misbehaving server, and it
/// never surfaces a vector of the wrong dimension, so a caller can safely fall
/// back to structural/keyword recall.
/// </para>
/// <para>
/// It wraps the server's internal, versioned request/response contract
/// (<see cref="OnyxEmbedRequest"/> / <see cref="OnyxEmbedResponse"/>) behind this
/// single seam. That contract is served by both shipped companion images -
/// <c>apps/embedding-onnx</c> (the sample's default) and <c>apps/embedding</c>
/// (the reference implementation it is pinned against) - on the same port and
/// with numerically identical vectors, so this client is unaware of which one is
/// running.
/// </para>
/// </remarks>
internal sealed class OnyxEmbeddingProvider : IEmbeddingProvider
{
    /// <summary>
    /// The name of the <see cref="HttpClient"/> the provider resolves from
    /// <see cref="IHttpClientFactory"/>, so a host can configure the handler,
    /// resilience, or proxy for embedding egress in isolation.
    /// </summary>
    public const string HttpClientName = "Orleans.Lattice.RepoContext.OnyxEmbedding";

    private const string HealthPath = "api/health";
    private const string EmbedPath = "encoder/bi-encoder-embed";

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly OnyxEmbeddingOptions _options;
    private readonly ILogger<OnyxEmbeddingProvider> _logger;
    private readonly Uri _baseAddress;

    /// <summary>
    /// Constructs the provider from the HTTP client factory and options.
    /// </summary>
    /// <param name="httpClientFactory">Factory for the named embedding
    /// <see cref="HttpClient"/>.</param>
    /// <param name="options">The Onyx endpoint and model configuration.</param>
    /// <param name="logger">Logger for the cold fail-closed diagnostic path.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public OnyxEmbeddingProvider(
        IHttpClientFactory httpClientFactory,
        IOptions<OnyxEmbeddingOptions> options,
        ILogger<OnyxEmbeddingProvider> logger)
    {
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        ArgumentNullException.ThrowIfNull(_options);
        _logger = logger;
        _baseAddress = NormalizeBaseAddress(_options.BaseAddress);
        Space = new EmbeddingSpace(_options.ModelName, _options.Dimension, _options.NormalizeEmbeddings);
    }

    /// <inheritdoc />
    public EmbeddingSpace Space { get; }

    /// <inheritdoc />
    public async Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var client = CreateClient();
            using var response = await client.GetAsync(HealthPath, cancellationToken)
                .ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException
            or InvalidOperationException or UriFormatException)
        {
            _logger.LogDebug(ex, "Onyx embedding health probe to {BaseAddress} failed.", _baseAddress);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<EmbeddingResult> EmbedAsync(
        IReadOnlyList<string> texts,
        EmbeddingTextType textType,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(texts);

        if (texts.Count == 0)
        {
            return EmbeddingResult.Success(Space, Array.Empty<ReadOnlyMemory<float>>());
        }

        var request = new OnyxEmbedRequest
        {
            Texts = texts,
            ModelName = _options.ModelName,
            MaxContextLength = _options.MaxContextLength,
            NormalizeEmbeddings = _options.NormalizeEmbeddings,
            TextType = ToWireTextType(textType),
            ProviderType = null,
        };

        OnyxEmbedResponse? payload;
        try
        {
            using var client = CreateClient();
            using var response = await client
                .PostAsJsonAsync(EmbedPath, request, SerializerOptions, cancellationToken)
                .ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                return Fail(
                    $"The Onyx model server at {_baseAddress} returned {(int)response.StatusCode} "
                    + $"({response.ReasonPhrase}) for an embedding request.");
            }

            payload = await response.Content
                .ReadFromJsonAsync<OnyxEmbedResponse>(SerializerOptions, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException
            or InvalidOperationException or UriFormatException)
        {
            return Fail(
                $"The Onyx model server at {_baseAddress} was unreachable or returned an unreadable "
                + $"response: {ex.Message}");
        }

        return Project(payload, texts.Count);
    }

    private EmbeddingResult Project(OnyxEmbedResponse? payload, int expectedCount)
    {
        var embeddings = payload?.Embeddings;
        if (embeddings is null)
        {
            return Fail($"The Onyx model server at {_baseAddress} returned a body with no embeddings.");
        }

        if (embeddings.Length != expectedCount)
        {
            return Fail(
                $"The Onyx model server returned {embeddings.Length} vectors for {expectedCount} "
                + "input texts.");
        }

        var vectors = new ReadOnlyMemory<float>[embeddings.Length];
        for (var i = 0; i < embeddings.Length; i++)
        {
            var vector = embeddings[i];
            if (vector is null || vector.Length != Space.Dimension)
            {
                return Fail(
                    $"The Onyx model server returned a vector of length {vector?.Length ?? 0} at index "
                    + $"{i}, but the configured embedding space '{Space.ModelId}' is {Space.Dimension}"
                    + "-dimensional.");
            }

            vectors[i] = vector;
        }

        return EmbeddingResult.Success(Space, vectors);
    }

    private EmbeddingResult Fail(string error)
    {
        _logger.LogWarning(
            "Embedding via the Onyx model server failed (fail-closed, caller falls back): {Error}", error);
        return EmbeddingResult.Failure(Space, error);
    }

    private HttpClient CreateClient()
    {
        var client = _httpClientFactory.CreateClient(HttpClientName);
        client.BaseAddress ??= _baseAddress;
        if (_options.RequestTimeout is { } timeout)
        {
            client.Timeout = timeout;
        }

        return client;
    }

    private static string ToWireTextType(EmbeddingTextType textType) => textType switch
    {
        EmbeddingTextType.Passage => "passage",
        EmbeddingTextType.Query => "query",
        _ => throw new ArgumentOutOfRangeException(nameof(textType), textType, "Unknown embedding text type."),
    };

    private static Uri NormalizeBaseAddress(Uri baseAddress)
    {
        ArgumentNullException.ThrowIfNull(baseAddress);
        if (baseAddress.AbsoluteUri.EndsWith('/'))
        {
            return baseAddress;
        }

        return new Uri(baseAddress.AbsoluteUri + "/");
    }
}
