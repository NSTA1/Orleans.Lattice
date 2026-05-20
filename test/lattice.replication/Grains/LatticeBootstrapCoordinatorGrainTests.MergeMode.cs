using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the per-tree <see cref="LatticeMergeMode"/> stamping in
/// <see cref="LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync"/>.
/// The drain previously hardcoded
/// <see cref="LatticeMergeMode.LwwRegister"/> on every emitted
/// <see cref="WalRecord"/>; the receiver-side per-tree merge mode now
/// flows from the resolver so an OrSet- or PnCounter-mode tree merges
/// bootstrap-arrived entries under the correct CRDT semantics rather
/// than silently degrading to LWW.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public async Task Drain_stamps_OrSet_mode_when_resolver_returns_OrSet()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)LatticeMergeMode.OrSet);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake, mergeResolver: resolver);
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(50) };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(123), new VersionVector(), Stream(entry))));

        await grain.ProcessNextPhaseAsync();

        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "k", Hlc(50), SourceCluster, LatticeMergeMode.OrSet)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Drain_stamps_PnCounter_mode_when_resolver_returns_PnCounter()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)LatticeMergeMode.PnCounter);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake, mergeResolver: resolver);
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 1, 2 }, Timestamp = Hlc(7) };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entry))));

        await grain.ProcessNextPhaseAsync();

        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "k", Hlc(7), SourceCluster, LatticeMergeMode.PnCounter)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Drain_falls_back_to_LwwRegister_when_resolver_returns_null()
    {
        // Pins the contract that an unconfigured tree (resolver returns
        // null, the "not enumerated in ReplicatedTrees" signal)
        // preserves the historical LwwRegister default.
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)null);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake, mergeResolver: resolver);
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 9 }, Timestamp = Hlc(11) };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(20), new VersionVector(), Stream(entry))));

        await grain.ProcessNextPhaseAsync();

        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "k", Hlc(11), SourceCluster, LatticeMergeMode.LwwRegister)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Drain_explicit_LwwRegister_resolver_stamps_LwwRegister()
    {
        // Explicit LwwRegister (resolver returns a value) must behave
        // identically to the null-fallback case so a host that opts a
        // tree in with `ReplicatedTrees[tree] = LwwRegister` gets the
        // expected behaviour rather than silently differing from the
        // unconfigured-tree path.
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)LatticeMergeMode.LwwRegister);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, apply, _, _) = Create(fake, mergeResolver: resolver);
        var entry = new SnapshotEntry { Key = "k", Value = new byte[] { 0 }, Timestamp = Hlc(3) };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(5), new VersionVector(), Stream(entry))));

        await grain.ProcessNextPhaseAsync();

        await apply.Received(1).ApplyAsync(
            Arg.Is<WalRecord>(r => IsBootstrapSet(r, "k", Hlc(3), SourceCluster, LatticeMergeMode.LwwRegister)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Drain_resolves_merge_mode_exactly_once_per_drain_regardless_of_entry_count()
    {
        // Hot-path invariant: the merge mode is invariant for the
        // lifetime of a drain, so the resolver must be hit exactly once
        // up-front rather than once per entry. A regression that moved
        // the Resolve call inside the await-foreach would slip past
        // every other test in this fixture because they all use a
        // single-entry snapshot. Three entries here would catch a
        // per-entry call.
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns((LatticeMergeMode?)LatticeMergeMode.OrSet);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, _, _, _) = Create(fake, mergeResolver: resolver);
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = Hlc(2) },
            new SnapshotEntry { Key = "c", Value = new byte[] { 3 }, Timestamp = Hlc(3) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        resolver.Received(1).Resolve(Tree);
    }

    [Test]
    public async Task Drain_resolver_is_keyed_by_tree_name_not_source_cluster()
    {
        // Pins the contract that the resolver is keyed by tree name
        // (the receiver-side tree id) rather than the source cluster
        // id, which would be incorrect: the merge mode is a per-tree
        // semantic property, not a per-peer one.
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns((LatticeMergeMode?)null);
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _, _, _, _) = Create(fake, mergeResolver: resolver);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(1), new VersionVector(), Stream())));

        await grain.ProcessNextPhaseAsync();

        resolver.Received(1).Resolve(Tree);
        resolver.DidNotReceive().Resolve(SourceCluster);
    }
}
