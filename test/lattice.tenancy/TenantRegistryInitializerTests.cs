using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the constructor argument guards on
/// <see cref="TenantRegistryInitializer"/>. The bootstrap body itself (history
/// retention, view creation, default-tenant seeding) needs a live silo and is
/// covered by the integration convergence fixture.
/// </summary>
[TestFixture]
public sealed class TenantRegistryInitializerTests
{
    private static IGrainFactory GrainFactory => Substitute.For<IGrainFactory>();

    private static IServiceProvider Services => new ServiceCollection().BuildServiceProvider();

    private static IOptionsMonitor<LatticeTenancyOptions> OptionsMonitor
    {
        get
        {
            var options = Substitute.For<IOptionsMonitor<LatticeTenancyOptions>>();
            options.CurrentValue.Returns(new LatticeTenancyOptions());
            return options;
        }
    }

    private static IOptions<ClusterOptions> Cluster => Options.Create(new ClusterOptions { ClusterId = "test" });

    private static OrleansLatticeSerializer<TenantRecord> Serializer => TestSerializers.TenantRecords;

    [Test]
    public void Ctor_null_grain_factory_throws()
    {
        Assert.That(
            () => new TenantRegistryInitializer(null!, Services, OptionsMonitor, Cluster, Serializer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_services_throws()
    {
        Assert.That(
            () => new TenantRegistryInitializer(GrainFactory, null!, OptionsMonitor, Cluster, Serializer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_options_throws()
    {
        Assert.That(
            () => new TenantRegistryInitializer(GrainFactory, Services, null!, Cluster, Serializer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_cluster_options_throws()
    {
        Assert.That(
            () => new TenantRegistryInitializer(GrainFactory, Services, OptionsMonitor, null!, Serializer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_serializer_throws()
    {
        Assert.That(
            () => new TenantRegistryInitializer(GrainFactory, Services, OptionsMonitor, Cluster, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_with_valid_arguments_succeeds()
    {
        Assert.That(
            () => new TenantRegistryInitializer(GrainFactory, Services, OptionsMonitor, Cluster, Serializer),
            Throws.Nothing);
    }

    [Test]
    public async Task EnsureInitializedAsync_skips_seeding_when_the_default_tenant_already_exists()
    {
        // Covers line 127: SeedDefaultTenantAsync returns early when the registry
        // already contains the default tenant record instead of writing a new one.
        var grainFactory = Substitute.For<IGrainFactory>();
        var serializer = TestSerializers.TenantRecords;
        var options = Substitute.For<IOptionsMonitor<LatticeTenancyOptions>>();
        options.CurrentValue.Returns(new LatticeTenancyOptions { SeedDefaultTenant = true });

        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);

        // Return a serialized TenantRecord for the default-tenant key so the
        // initializer finds an existing entry and returns at line 127.
        var existing = TenantRecord.Create(
            TenantId.Default,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            "seed");
        lattice.GetAsync(TenantId.Default.Value!, Arg.Any<CancellationToken>())
            .Returns(serializer.Serialize(existing));

        var initializer = new TenantRegistryInitializer(
            grainFactory, Services, options, Cluster, serializer);

        await initializer.EnsureInitializedAsync();

        await lattice.DidNotReceive().SetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }
}
