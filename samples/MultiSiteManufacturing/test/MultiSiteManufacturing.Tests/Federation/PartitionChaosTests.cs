using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// Covers the <see cref="ChaosPreset.ClusterSplit"/> router filter:
/// when the partition flag is set, each silo inside a cluster accepts
/// only the half of the serial-hash space it owns; ClearAll heals.
/// </summary>
[TestFixture]
public class PartitionChaosTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private FederationRouter BuildRouter(bool isPrimary)
    {
        var baseline = _fixture.NewBaselineBackend();
        var lattice = _fixture.NewLatticeBackend();
        return new FederationRouter(
            [baseline, lattice],
            _fixture.GrainFactory,
            NullLogger<FederationRouter>.Instance,
            new SiloIdentity(isPrimary ? "a" : "b", isPrimary, ClusterName: "us"));
    }

    [Test]
    public async Task EmitAsync_passes_when_partition_not_active()
    {
        var router = BuildRouter(isPrimary: true);
        // Clean slate — ensure the partition flag is cleared from any
        // previous test.
        await router.ConfigurePartitionAsync(false);

        var fact = Step(new PartSerialNumber("HPT-PART-2028-93000"), 1, ProcessStage.Forge, ProcessSite.OhioForge);
        var forwarded = await router.EmitAsync(fact);

        Assert.That(forwarded, Is.True);
    }

    [Test]
    public async Task Partition_splits_serials_between_silos_and_ClearAll_heals()
    {
        var siloA = BuildRouter(isPrimary: true);
        var siloB = BuildRouter(isPrimary: false);

        await siloA.ConfigurePartitionAsync(true);

        // Two serials whose FNV-1a hashes land in different buckets.
        // We detect which silo owns each by probing both routers; the
        // invariant we assert is that every serial is accepted by
        // exactly one silo during partition and by both after heal.
        var serials = Enumerable.Range(93100, 20)
            .Select(i => new PartSerialNumber($"HPT-PART-2028-{i}"))
            .ToArray();

        var acceptedByA = new List<PartSerialNumber>();
        var acceptedByB = new List<PartSerialNumber>();

        foreach (var serial in serials)
        {
            var factA = Step(serial, 1, ProcessStage.Forge, ProcessSite.OhioForge);
            var factB = Step(serial, 2, ProcessStage.Forge, ProcessSite.OhioForge);
            if (await siloA.EmitAsync(factA)) acceptedByA.Add(serial);
            if (await siloB.EmitAsync(factB)) acceptedByB.Add(serial);
        }

        Assert.Multiple(() =>
        {
            Assert.That(acceptedByA, Is.Not.Empty, "silo A should own some serials");
            Assert.That(acceptedByB, Is.Not.Empty, "silo B should own some serials");
            Assert.That(acceptedByA.Intersect(acceptedByB), Is.Empty,
                "no serial should be accepted by both silos during partition");
            Assert.That(acceptedByA.Count + acceptedByB.Count, Is.EqualTo(serials.Length),
                "every serial should be accepted by exactly one silo");
        });

        // Heal: ClearAll flips the partition flag off via
        // ApplyPresetAsync. Afterwards both silos accept every serial.
        await siloA.ApplyPresetAsync(ChaosPreset.ClearAll);

        foreach (var serial in serials.Take(3))
        {
            var healedA = Step(serial, 10, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat);
            var healedB = Step(serial, 11, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat);
            Assert.That(await siloA.EmitAsync(healedA), Is.True, "silo A should accept after heal");
            Assert.That(await siloB.EmitAsync(healedB), Is.True, "silo B should accept after heal");
        }
    }

    [Test]
    public async Task ClusterSplit_preset_sets_partition_flag()
    {
        var router = BuildRouter(isPrimary: true);
        await router.ConfigurePartitionAsync(false);

        await router.ApplyPresetAsync(ChaosPreset.ClusterSplit);

        Assert.That(await router.IsPartitionedAsync(), Is.True);

        // Leave the flag cleared so subsequent tests aren't poisoned.
        await router.ConfigurePartitionAsync(false);
    }

    [Test]
    public async Task ReplicationDisconnect_preset_sets_disconnect_flag()
    {
        var router = BuildRouter(isPrimary: true);
        var grain = _fixture.GrainFactory
            .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey);
        await grain.SetDisconnectedAsync(false);

        await router.ApplyPresetAsync(ChaosPreset.ReplicationDisconnect);

        Assert.That(await grain.IsDisconnectedAsync(), Is.True);

        // ClearAll heals both flags.
        await router.ApplyPresetAsync(ChaosPreset.ClearAll);
        Assert.That(await grain.IsDisconnectedAsync(), Is.False);
    }
}

