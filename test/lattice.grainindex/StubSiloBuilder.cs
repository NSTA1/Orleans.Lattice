using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// A minimal <see cref="ISiloBuilder"/> that exposes a real
/// <see cref="IServiceCollection"/> and nothing else.
/// <para>
/// The registration surface under test only ever touches
/// <see cref="ISiloBuilder.Services"/>, so the declaration path can be exercised
/// with plain dependency injection instead of a cluster - which keeps these
/// tests unit tests with no host, no timing, and no ordering dependence.
/// </para>
/// </summary>
internal sealed class StubSiloBuilder : ISiloBuilder
{
    /// <inheritdoc />
    public IServiceCollection Services { get; } = new ServiceCollection();

    /// <inheritdoc />
    public IConfiguration Configuration =>
        throw new NotSupportedException(
            "The grain-index registration surface does not read configuration; "
            + "a test reaching this has drifted from what it means to cover.");

    /// <summary>
    /// Builds a provider over whatever the declaration calls registered, so a
    /// test can resolve the options it seeded.
    /// </summary>
    public ServiceProvider BuildServiceProvider() => Services.BuildServiceProvider();
}
