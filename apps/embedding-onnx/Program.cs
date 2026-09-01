using System.Text.Json.Serialization;
using Orleans.Lattice.Embedding.Onnx;

// ---------------------------------------------------------------------------
// The repository-context embedding companion, ONNX Runtime edition.
//
// One job: text -> vector, over the same HTTP contract the Onyx companion image
// serves (GET /api/health, POST /encoder/bi-encoder-embed on port 9000), so it
// is a drop-in for that image behind the unchanged OnyxEmbeddingProvider client.
//
// The accelerator is chosen at runtime by EMBED_PROVIDER, with the CPU as the
// always-available fallback, so one image serves both the CPU default and an
// NVIDIA host by configuration alone.
// ---------------------------------------------------------------------------

// Exec-form container health probe. The runtime image is chiseled and has no
// shell, so a shell-form HEALTHCHECK cannot work; the image instead re-invokes
// this same assembly with --healthcheck, which needs no shell and no extra
// tooling in the image. Handled before any host is built so the probe stays a
// cheap, short-lived process rather than loading the model.
if (args.Contains("--healthcheck", StringComparer.Ordinal))
{
    return await HealthProbe.RunAsync(
        EmbedServerOptions.ParsePositivePort(
            Environment.GetEnvironmentVariable("EMBED_PORT"))).ConfigureAwait(false);
}

var options = EmbedServerOptions.FromEnvironment(Environment.GetEnvironmentVariable);

// Must run before any ONNX Runtime type is touched: the native load happens in
// that assembly's static constructor, and a failed static constructor is cached
// for the life of the process.
OnnxNativeLibraryResolver.Install();

var builder = WebApplication.CreateSlimBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{options.Port}");
builder.Services.ConfigureHttpJsonOptions(json =>
    json.SerializerOptions.TypeInfoResolverChain.Insert(0, EmbedJsonContext.Default));

// Loading the model at startup (not on first request) means the container is
// unhealthy until it can actually serve, so an orchestrator's readiness gate -
// the sample compose's `depends_on: service_healthy` - means what it says.
var embedder = new OnnxEmbedder(options);
builder.Services.AddSingleton(embedder);

var app = builder.Build();
app.Lifetime.ApplicationStopped.Register(embedder.Dispose);

app.MapGet("/api/health", (OnnxEmbedder engine) => Results.Ok(
    new HealthResponse("ok", engine.ActiveProvider, engine.ModelName, engine.Dimension)));

app.MapPost("/encoder/bi-encoder-embed", (EmbedRequest request, OnnxEmbedder engine) =>
{
    var texts = request.Texts;
    if (texts is null || texts.Count == 0)
    {
        return Results.Ok(new EmbedResponse([]));
    }

    var vectors = engine.Embed(texts, request.MaxContextLength, request.NormalizeEmbeddings);
    return Results.Ok(new EmbedResponse(vectors));
});

app.Logger.LogInformation(
    "Embedding server listening on port {Port} using {Provider} with model {Model} ({Dimension}-dim).",
    options.Port,
    embedder.ActiveProvider,
    embedder.ModelName,
    embedder.Dimension);

await app.RunAsync().ConfigureAwait(false);
return 0;

/// <summary>
/// The container health probe: a short-lived HTTP GET against the server's own
/// health endpoint on localhost, used by the image's exec-form
/// <c>HEALTHCHECK</c>.
/// </summary>
internal static class HealthProbe
{
    /// <summary>
    /// Probes the local health endpoint.
    /// </summary>
    /// <param name="port">The port the server listens on.</param>
    /// <returns><c>0</c> when the endpoint answers successfully, otherwise
    /// <c>1</c>.</returns>
    public static async Task<int> RunAsync(int port)
    {
        try
        {
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            using var response = await client
                .GetAsync(new Uri($"http://localhost:{port}/api/health"))
                .ConfigureAwait(false);
            return response.IsSuccessStatusCode ? 0 : 1;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return 1;
        }
    }
}

/// <summary>
/// The source-generated JSON context for the wire types, so the server runs
/// without reflection-based serialization under the slim hosting model.
/// </summary>
[JsonSerializable(typeof(EmbedRequest))]
[JsonSerializable(typeof(EmbedResponse))]
[JsonSerializable(typeof(HealthResponse))]
internal sealed partial class EmbedJsonContext : JsonSerializerContext;
