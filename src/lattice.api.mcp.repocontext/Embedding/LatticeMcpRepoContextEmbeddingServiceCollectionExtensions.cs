using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// DI extensions that bind the default <see cref="IEmbeddingProvider"/> for the
/// repository-context surface: the thin client for the companion Onyx
/// model-server embedding container.
/// </summary>
public static class LatticeMcpRepoContextEmbeddingServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="OnyxEmbeddingProvider"/> as the singleton
    /// <see cref="IEmbeddingProvider"/> and its named
    /// <see cref="System.Net.Http.HttpClient"/>. The seam is fully swappable: this
    /// registration is <c>TryAdd</c>, so a host that has already bound its own
    /// <see cref="IEmbeddingProvider"/> (OpenAI, Azure OpenAI, a self-hosted
    /// endpoint) keeps it, and a host that wants a different provider simply does
    /// not call this method.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">Optional callback to populate
    /// <see cref="OnyxEmbeddingOptions"/> (endpoint base address, model, dimension).
    /// When omitted, the defaults target the model and endpoint baked into the
    /// shipped <c>apps/embedding</c> image.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    public static IServiceCollection AddOnyxEmbeddingProvider(
        this IServiceCollection services,
        Action<OnyxEmbeddingOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var optionsBuilder = services.AddOptions<OnyxEmbeddingOptions>();
        if (configure is not null)
        {
            optionsBuilder.Configure(configure);
        }

        services.AddHttpClient(OnyxEmbeddingProvider.HttpClientName);
        services.TryAddSingleton<IEmbeddingProvider, OnyxEmbeddingProvider>();

        return services;
    }
}
