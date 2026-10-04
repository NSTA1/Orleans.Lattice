using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// #4464 durable causal-apply buffer on the real site-B tree. A drained entry
/// is removed durably only after its apply, so a crash in between re-applies it
/// on the next drain; and a parked entry releases its shadow-forward
/// reservation, so a late re-delivery after the drain reaches the applier
/// again. Both must be harmless: no double count, and no regression of a newer
/// value.
/// </summary>
public partial class ReplicationApplyIntegrationTests
{
    private const string DependencyOrigin = "site-c";

    private static IOptionsMonitor<LatticeReplicationOptions> SiteBMonitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions { ClusterId = TwoSiteClusterFixture.SiteBClusterId };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);
        return monitor;
    }

    private static VersionVector DependsOnSiteC(long ticks)
    {
        var vc = new VersionVector();
        vc.Entries[DependencyOrigin] = Hlc(ticks);
        return vc;
    }

    private static WalRecord PnDelta(string tree, string key, long increment, long decrement, HybridLogicalClock ts)
    {
        var counter = new PnCounter();
        counter.Increment("site-a", increment);
        counter.Decrement("site-a", decrement);
        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["site-a"] = increment },
            Decrements = new Dictionary<string, long> { ["site-a"] = decrement },
        };
        return new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = JsonLatticeSerializer<PnCounter>.Default.Serialize(counter),
            Delta = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(delta),
            Timestamp = ts,
            Mode = LatticeMergeMode.PnCounter,
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        };
    }

    private async Task SatisfySiteCDependencyAsync(string tree, ReplicationApplier applier, LatticeMergeMode mode)
    {
        var dependency = mode == LatticeMergeMode.PnCounter
            ? PnDelta(tree, "dep", 1, 0, Hlc(50)) with { OriginClusterId = DependencyOrigin }
            : LwwSet(tree, "dep", new byte[] { 9 }, Hlc(50)) with { OriginClusterId = DependencyOrigin };
        Assert.That((await applier.ApplyAsync(dependency)).Applied, Is.True);
    }

    [Test]
    public async Task Lww_entry_re_applied_after_a_crash_between_drain_apply_and_removal_has_no_double_effect()
    {
        const string tree = "ri-causal-crash-lww";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var state = new FakePersistentState<CausalApplyBufferState>();
        var grain = CausalBufferTestWiring.Create(_fixture.SiteB.Client, applier, SiteBMonitor(), tree, state);
        var parked = LwwSet(tree, "k", new byte[] { 1 }, Hlc(100)) with { VectorClock = DependsOnSiteC(50) };

        Assert.That(await grain.ParkAsync(parked), Is.EqualTo(1), "Dependency unmet: parked.");
        await SatisfySiteCDependencyAsync(tree, applier, LatticeMergeMode.LwwRegister);

        state.ThrowOnWrite = new InvalidOperationException("crash before the removal is durable");
        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DrainAsync());
        Assert.That((await lattice.GetWithVersionAsync("k")).Value, Is.EqualTo(new byte[] { 1 }), "Applied once.");

        // A newer write lands, then the reactivated buffer re-applies the
        // entry it still holds.
        await applier.ApplyAsync(LwwSet(tree, "k", new byte[] { 2 }, Hlc(200)));
        var reactivated = CausalBufferTestWiring.Create(_fixture.SiteB.Client, applier, SiteBMonitor(), tree, state);
        Assert.That(await reactivated.DrainAsync(), Is.Zero);

        var k = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(k.Value, Is.EqualTo(new byte[] { 2 }), "The re-apply must not regress the newer row.");
            Assert.That(k.Version, Is.EqualTo(Hlc(200)));
            Assert.That(state.State.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task Pn_counter_delta_re_applied_after_a_crash_between_drain_apply_and_removal_is_not_double_counted()
    {
        const string tree = "ri-causal-crash-pn";
        TwoSiteClusterFixture.TreeModeOverrides[tree] = LatticeMergeMode.PnCounter;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var state = new FakePersistentState<CausalApplyBufferState>();
        var grain = CausalBufferTestWiring.Create(_fixture.SiteB.Client, applier, SiteBMonitor(), tree, state);
        var parked = PnDelta(tree, "k", 5, 1, Hlc(100)) with { VectorClock = DependsOnSiteC(50) };

        Assert.That(await grain.ParkAsync(parked), Is.EqualTo(1));
        await SatisfySiteCDependencyAsync(tree, applier, LatticeMergeMode.PnCounter);

        state.ThrowOnWrite = new InvalidOperationException("crash before the removal is durable");
        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DrainAsync());
        Assert.That((await lattice.PnCounter("k").GetAsync()).Value, Is.EqualTo(4), "Applied once.");

        var reactivated = CausalBufferTestWiring.Create(_fixture.SiteB.Client, applier, SiteBMonitor(), tree, state);
        Assert.That(await reactivated.DrainAsync(), Is.Zero);

        Assert.That((await lattice.PnCounter("k").GetAsync()).Value, Is.EqualTo(4), "The re-apply must not double count.");
    }

    [Test]
    public async Task Late_redelivery_of_a_drained_lww_entry_does_not_regress_a_newer_row()
    {
        const string tree = "ri-causal-late-lww";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var parked = LwwSet(tree, "k", new byte[] { 1 }, Hlc(100)) with { VectorClock = DependsOnSiteC(50) };

        // Parks in the silo's durable buffer grain, then drains when the
        // dependency applies.
        Assert.That((await applier.ApplyAsync(parked)).Applied, Is.False);
        await SatisfySiteCDependencyAsync(tree, applier, LatticeMergeMode.LwwRegister);
        Assert.That((await lattice.GetWithVersionAsync("k")).Value, Is.EqualTo(new byte[] { 1 }), "Drained.");
        Assert.That(await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync(), Is.Zero);

        await applier.ApplyAsync(LwwSet(tree, "k", new byte[] { 2 }, Hlc(200)));

        // Neither the cache (released at park) nor the buffer (drained) holds
        // it any more, so the late re-delivery reaches the apply.
        await applier.ApplyAsync(parked);

        var k = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(k.Value, Is.EqualTo(new byte[] { 2 }));
            Assert.That(k.Version, Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task Late_redelivery_of_a_drained_pn_counter_delta_is_not_double_counted()
    {
        const string tree = "ri-causal-late-pn";
        TwoSiteClusterFixture.TreeModeOverrides[tree] = LatticeMergeMode.PnCounter;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var parked = PnDelta(tree, "k", 5, 1, Hlc(100)) with { VectorClock = DependsOnSiteC(50) };

        Assert.That((await applier.ApplyAsync(parked)).Applied, Is.False);
        await SatisfySiteCDependencyAsync(tree, applier, LatticeMergeMode.PnCounter);
        Assert.That((await lattice.PnCounter("k").GetAsync()).Value, Is.EqualTo(4), "Drained.");

        await applier.ApplyAsync(parked);
        await CreateSiteBApplier().ApplyAsync(parked);

        Assert.That((await lattice.PnCounter("k").GetAsync()).Value, Is.EqualTo(4));
    }
}
