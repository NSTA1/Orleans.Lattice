using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupServiceCollectionExtensions"/> that do
/// not require a live silo: the ordering guard (backup must follow the core
/// registration), the null-argument guards, and idempotent re-registration.
/// Happy-path wiring is covered by the sink and catalog integration tests.
/// </summary>
[TestFixture]
public sealed class LatticeBackupServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeBackup_before_AddLattice_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeBackup(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeBackup_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeBackup(), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackup_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).ConfigureLatticeBackup(_ => { }), Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeBackup_with_null_configure_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.ConfigureLatticeBackup(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeBackup_after_core_wires_the_backup_services_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>>(
            new PassthroughLatticeOptionsValidator());

        builder.AddLatticeBackup();
        builder.AddLatticeBackup();

        var sinkRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeBackupSink));
        var catalogRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeBackupCatalogStore));
        Assert.That(sinkRegistrations, Is.EqualTo(1));
        Assert.That(catalogRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void ConfigureLatticeBackup_layers_the_options_delegate()
    {
        var builder = new FakeSiloBuilder();
        builder.ConfigureLatticeBackup(o => o.EnableDurableHistoryView = false);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeBackupOptions>>()
            .Value;

        Assert.That(options.EnableDurableHistoryView, Is.False);
    }

    private sealed class PassthroughLatticeOptionsValidator : IValidateOptions<LatticeOptions>
    {
        public ValidateOptionsResult Validate(string? name, LatticeOptions options) =>
            ValidateOptionsResult.Success;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
