using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiBackupServiceCollectionExtensions"/> that
/// do not require a live silo: the ordering guard (the control API must follow
/// the backup engine registration), the null-argument guard, idempotent
/// re-registration, and the layering of the options delegate. Happy-path wiring
/// is covered by the control-facade integration tests.
/// </summary>
[TestFixture]
public sealed class LatticeApiBackupServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeBackupApi_before_AddLatticeBackup_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeBackupApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeBackupApi_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeBackupApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeBackupApi_after_engine_wires_the_control_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeBackupCaptureService>());

        builder.AddLatticeBackupApi();
        builder.AddLatticeBackupApi();

        var controlRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeBackupControl));
        Assert.That(controlRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeBackupApi_layers_the_options_delegate()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeBackupCaptureService>());

        builder.AddLatticeBackupApi(o => o.DefaultListPageSize = 7);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeApiBackupOptions>>()
            .Value;

        Assert.That(options.DefaultListPageSize, Is.EqualTo(7));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
