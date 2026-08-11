using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Registration and configuration tests for
/// <see cref="LatticeMcpRepoContextEmbeddingServiceCollectionExtensions.AddOnyxEmbeddingProvider"/>.
/// Proves the default provider is resolvable, that the baked model is overridable
/// through options (so a swapped-in container model reshapes the embedding space),
/// that the seam is swappable (a host-bound provider wins), and that arguments
/// are validated.
/// </summary>
[TestFixture]
public sealed class AddOnyxEmbeddingProviderTests
{
    [Test]
    public void AddOnyxEmbeddingProvider_registers_the_default_onyx_provider()
    {
        var services = new ServiceCollection();
        services.AddOnyxEmbeddingProvider();

        using var provider = services.BuildServiceProvider();
        var embedding = provider.GetService<IEmbeddingProvider>();

        Assert.Multiple(() =>
        {
            Assert.That(embedding, Is.InstanceOf<OnyxEmbeddingProvider>());
            Assert.That(embedding!.Space.ModelId, Is.EqualTo(OnyxEmbeddingOptions.DefaultModelName));
            Assert.That(embedding.Space.Dimension, Is.EqualTo(OnyxEmbeddingOptions.DefaultDimension));
        });
    }

    [Test]
    public void AddOnyxEmbeddingProvider_applies_a_model_override_to_the_embedding_space()
    {
        var services = new ServiceCollection();
        services.AddOnyxEmbeddingProvider(o =>
        {
            o.ModelName = "acme/large-embed";
            o.Dimension = 1024;
            o.NormalizeEmbeddings = false;
        });

        using var provider = services.BuildServiceProvider();
        var embedding = provider.GetRequiredService<IEmbeddingProvider>();

        Assert.Multiple(() =>
        {
            Assert.That(embedding.Space.ModelId, Is.EqualTo("acme/large-embed"));
            Assert.That(embedding.Space.Dimension, Is.EqualTo(1024));
            Assert.That(embedding.Space.Normalized, Is.False);
        });
    }

    [Test]
    public void AddOnyxEmbeddingProvider_registers_the_named_http_client()
    {
        var services = new ServiceCollection();
        services.AddOnyxEmbeddingProvider();

        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IHttpClientFactory>();
        using var client = factory.CreateClient(OnyxEmbeddingProvider.HttpClientName);

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void AddOnyxEmbeddingProvider_leaves_a_host_bound_provider_in_place()
    {
        var custom = new FixedEmbeddingProvider(new EmbeddingSpace("host/custom", 16, true));
        var services = new ServiceCollection();
        services.AddSingleton<IEmbeddingProvider>(custom);

        services.AddOnyxEmbeddingProvider();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IEmbeddingProvider>(), Is.SameAs(custom));
    }

    [Test]
    public void AddOnyxEmbeddingProvider_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();
        Assert.That(services.AddOnyxEmbeddingProvider(), Is.SameAs(services));
    }

    [Test]
    public void AddOnyxEmbeddingProvider_rejects_a_null_service_collection()
        => Assert.Throws<ArgumentNullException>(
            () => LatticeMcpRepoContextEmbeddingServiceCollectionExtensions.AddOnyxEmbeddingProvider(null!));

    private sealed class FixedEmbeddingProvider(EmbeddingSpace space) : IEmbeddingProvider
    {
        public EmbeddingSpace Space { get; } = space;

        public Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task<EmbeddingResult> EmbedAsync(
            IReadOnlyList<string> texts,
            EmbeddingTextType textType,
            CancellationToken cancellationToken = default)
            => Task.FromResult(EmbeddingResult.Success(Space, Array.Empty<ReadOnlyMemory<float>>()));
    }
}
