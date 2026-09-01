using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The default <see cref="IGrainKeySourceResolver"/>, which looks a key source
/// up as a keyed singleton registered under the index's name.
/// </summary>
/// <remarks>
/// An index with no registered source resolves to <c>null</c> rather than
/// throwing. That is deliberate: declaring an index without one is a supported
/// configuration - the activation path still enrols every grain that is used -
/// and only the background crawl over dormant grains is unavailable.
/// </remarks>
internal sealed class GrainKeySourceResolver : IGrainKeySourceResolver
{
    private readonly IServiceProvider _services;

    /// <summary>Initialises the resolver.</summary>
    /// <param name="services">The container holding the keyed registrations. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public GrainKeySourceResolver(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
    }

    /// <inheritdoc />
    public IGrainKeySource? Resolve(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return _services.GetKeyedService<IGrainKeySource>(indexName);
    }
}
