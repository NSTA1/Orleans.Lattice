using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for <see cref="OnyxEmbeddingProvider"/> driven against a fake HTTP
/// endpoint. They cover the happy path (batching, dimension and normalization
/// surfaced into the embedding-space tag), the request wire contract, the
/// fail-closed paths (unreachable, non-success status, count/dimension mismatch,
/// malformed body), and the health probe. No real network or container is used.
/// </summary>
[TestFixture]
public sealed class OnyxEmbeddingProviderTests
{
    private static readonly Uri BaseAddress = new("http://onyx-embedder:9000");

    private static OnyxEmbeddingProvider CreateProvider(
        StubHttpMessageHandler handler, Action<OnyxEmbeddingOptions>? configure = null)
    {
        var options = new OnyxEmbeddingOptions();
        options.BaseAddress = BaseAddress;
        configure?.Invoke(options);

        var factory = new StubHttpClientFactory(handler);
        return new OnyxEmbeddingProvider(
            factory,
            Options.Create(options),
            NullLogger<OnyxEmbeddingProvider>.Instance);
    }

    private static HttpResponseMessage EmbeddingsResponse(params float[][] vectors)
        => new(HttpStatusCode.OK)
        {
            Content = JsonContent.Create(new { embeddings = vectors }),
        };

    [Test]
    public void Space_reflects_the_configured_model_dimension_and_normalization()
    {
        using var handler = new StubHttpMessageHandler(_ => EmbeddingsResponse());
        var provider = CreateProvider(handler, o =>
        {
            o.ModelName = "acme/tiny-embed";
            o.Dimension = 3;
            o.NormalizeEmbeddings = false;
        });

        Assert.Multiple(() =>
        {
            Assert.That(provider.Space.ModelId, Is.EqualTo("acme/tiny-embed"));
            Assert.That(provider.Space.Dimension, Is.EqualTo(3));
            Assert.That(provider.Space.Normalized, Is.False);
        });
    }

    [Test]
    public void Space_defaults_match_the_baked_onyx_nomic_model()
    {
        using var handler = new StubHttpMessageHandler(_ => EmbeddingsResponse());
        var provider = CreateProvider(handler);

        Assert.Multiple(() =>
        {
            Assert.That(provider.Space.ModelId, Is.EqualTo("nomic-ai/nomic-embed-text-v1"));
            Assert.That(provider.Space.Dimension, Is.EqualTo(768));
            Assert.That(provider.Space.Normalized, Is.True);
        });
    }

    [Test]
    public async Task EmbedAsync_returns_one_vector_per_input_in_order()
    {
        using var handler = new StubHttpMessageHandler(_ => EmbeddingsResponse(
            new[] { 1f, 0f, 0f }, new[] { 0f, 1f, 0f }));
        var provider = CreateProvider(handler, o => o.Dimension = 3);

        var result = await provider.EmbedAsync(
            new[] { "first", "second" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.True);
            Assert.That(result.Error, Is.Null);
            Assert.That(result.Vectors, Has.Count.EqualTo(2));
            Assert.That(result.Vectors[0].ToArray(), Is.EqualTo(new[] { 1f, 0f, 0f }));
            Assert.That(result.Vectors[1].ToArray(), Is.EqualTo(new[] { 0f, 1f, 0f }));
            Assert.That(result.Space.Dimension, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task EmbedAsync_sends_the_full_batch_in_a_single_request()
    {
        var requests = new List<HttpRequestMessage>();
        using var handler = new StubHttpMessageHandler(req =>
        {
            requests.Add(req);
            return EmbeddingsResponse(new[] { 0f }, new[] { 0f }, new[] { 0f });
        });
        var provider = CreateProvider(handler, o => o.Dimension = 1);

        await provider.EmbedAsync(new[] { "a", "b", "c" }, EmbeddingTextType.Passage);

        Assert.That(requests, Has.Count.EqualTo(1), "the whole batch is one request");
    }

    [Test]
    public async Task EmbedAsync_posts_the_pinned_wire_contract_to_the_embed_path()
    {
        JsonElement body = default;
        Uri? requestUri = null;
        using var handler = new StubHttpMessageHandler(req =>
        {
            requestUri = req.RequestUri;
            body = ReadBody(req);
            return EmbeddingsResponse(new[] { 0f, 0f });
        });
        var provider = CreateProvider(handler, o =>
        {
            o.ModelName = "acme/tiny-embed";
            o.Dimension = 2;
            o.MaxContextLength = 256;
            o.NormalizeEmbeddings = true;
        });

        await provider.EmbedAsync(new[] { "chunk" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(requestUri, Is.EqualTo(new Uri("http://onyx-embedder:9000/encoder/bi-encoder-embed")));
            Assert.That(body.GetProperty("model_name").GetString(), Is.EqualTo("acme/tiny-embed"));
            Assert.That(body.GetProperty("max_context_length").GetInt32(), Is.EqualTo(256));
            Assert.That(body.GetProperty("normalize_embeddings").GetBoolean(), Is.True);
            Assert.That(body.GetProperty("text_type").GetString(), Is.EqualTo("passage"));
            Assert.That(body.GetProperty("provider_type").ValueKind, Is.EqualTo(JsonValueKind.Null));
            Assert.That(body.GetProperty("texts").EnumerateArray().Single().GetString(), Is.EqualTo("chunk"));
        });
    }

    [Test]
    public async Task EmbedAsync_maps_the_query_role_to_the_query_prefix()
    {
        JsonElement body = default;
        using var handler = new StubHttpMessageHandler(req =>
        {
            body = ReadBody(req);
            return EmbeddingsResponse(new[] { 0f });
        });
        var provider = CreateProvider(handler, o => o.Dimension = 1);

        await provider.EmbedAsync(new[] { "how do I" }, EmbeddingTextType.Query);

        Assert.That(body.GetProperty("text_type").GetString(), Is.EqualTo("query"));
    }

    [Test]
    public async Task EmbedAsync_with_no_texts_returns_an_empty_success_without_calling_the_server()
    {
        var called = false;
        using var handler = new StubHttpMessageHandler(_ =>
        {
            called = true;
            return EmbeddingsResponse();
        });
        var provider = CreateProvider(handler);

        var result = await provider.EmbedAsync(Array.Empty<string>(), EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.True);
            Assert.That(result.Vectors, Is.Empty);
            Assert.That(called, Is.False);
        });
    }

    [Test]
    public void EmbedAsync_rejects_a_null_text_list()
    {
        using var handler = new StubHttpMessageHandler(_ => EmbeddingsResponse());
        var provider = CreateProvider(handler);

        Assert.ThrowsAsync<ArgumentNullException>(
            () => provider.EmbedAsync(null!, EmbeddingTextType.Passage));
    }

    [Test]
    public async Task EmbedAsync_is_fail_closed_when_the_server_is_unreachable()
    {
        using var handler = new StubHttpMessageHandler(
            _ => throw new HttpRequestException("connection refused"));
        var provider = CreateProvider(handler);

        var result = await provider.EmbedAsync(new[] { "x" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(result.Vectors, Is.Empty);
            Assert.That(result.Error, Does.Contain("unreachable").IgnoreCase);
        });
    }

    [Test]
    public async Task EmbedAsync_is_fail_closed_on_a_non_success_status()
    {
        using var handler = new StubHttpMessageHandler(
            _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
        var provider = CreateProvider(handler);

        var result = await provider.EmbedAsync(new[] { "x" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(result.Error, Does.Contain("503"));
        });
    }

    [Test]
    public async Task EmbedAsync_is_fail_closed_when_the_vector_dimension_is_wrong()
    {
        using var handler = new StubHttpMessageHandler(
            _ => EmbeddingsResponse(new[] { 1f, 2f, 3f, 4f }));
        var provider = CreateProvider(handler, o => o.Dimension = 3);

        var result = await provider.EmbedAsync(new[] { "x" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(result.Vectors, Is.Empty);
            Assert.That(result.Error, Does.Contain("dimension").Or.Contain("dimensional"));
        });
    }

    [Test]
    public async Task EmbedAsync_is_fail_closed_when_the_vector_count_disagrees()
    {
        using var handler = new StubHttpMessageHandler(
            _ => EmbeddingsResponse(new[] { 1f, 2f }));
        var provider = CreateProvider(handler, o => o.Dimension = 2);

        var result = await provider.EmbedAsync(new[] { "a", "b" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(result.Error, Does.Contain("1 vectors for 2"));
        });
    }

    [Test]
    public async Task EmbedAsync_is_fail_closed_on_a_body_with_no_embeddings()
    {
        using var handler = new StubHttpMessageHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = JsonContent.Create(new { unrelated = true }),
        });
        var provider = CreateProvider(handler);

        var result = await provider.EmbedAsync(new[] { "x" }, EmbeddingTextType.Passage);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(result.Error, Does.Contain("no embeddings"));
        });
    }

    [Test]
    public async Task IsAvailableAsync_is_true_when_the_health_probe_succeeds()
    {
        Uri? probed = null;
        using var handler = new StubHttpMessageHandler(req =>
        {
            probed = req.RequestUri;
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        var provider = CreateProvider(handler);

        var available = await provider.IsAvailableAsync();

        Assert.Multiple(() =>
        {
            Assert.That(available, Is.True);
            Assert.That(probed, Is.EqualTo(new Uri("http://onyx-embedder:9000/api/health")));
        });
    }

    [Test]
    public async Task IsAvailableAsync_is_false_when_the_probe_fails_or_throws()
    {
        using var down = new StubHttpMessageHandler(
            _ => new HttpResponseMessage(HttpStatusCode.InternalServerError));
        using var thrown = new StubHttpMessageHandler(
            _ => throw new HttpRequestException("no route to host"));

        var downProvider = CreateProvider(down);
        var thrownProvider = CreateProvider(thrown);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await downProvider.IsAvailableAsync(), Is.False);
            Assert.That(await thrownProvider.IsAvailableAsync(), Is.False);
        });
    }

    private static JsonElement ReadBody(HttpRequestMessage request)
    {
        var json = request.Content!.ReadAsStringAsync().GetAwaiter().GetResult();
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}
