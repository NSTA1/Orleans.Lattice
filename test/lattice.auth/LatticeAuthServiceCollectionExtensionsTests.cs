using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAuthServiceCollectionExtensions"/> that do not
/// require a live silo: the ordering guard (auth must follow the core
/// registration) and the null-argument guards. Happy-path wiring is covered by
/// the policy-store integration tests.
/// </summary>
[TestFixture]
public class LatticeAuthServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeAuth_before_AddLattice_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeAuth(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeAuth_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeAuth(), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeAuth_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).ConfigureLatticeAuth(_ => { }), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeAuth_with_null_configure_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.ConfigureLatticeAuth(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeAuth_layers_the_options_delegate()
    {
        var builder = new FakeSiloBuilder();
        builder.ConfigureLatticeAuth(o => o.EnableDurableHistoryView = false);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeAuthOptions>>()
            .Value;

        Assert.That(options.EnableDurableHistoryView, Is.False);
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
