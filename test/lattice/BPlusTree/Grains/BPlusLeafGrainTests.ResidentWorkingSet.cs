using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Wiring tests for the per-silo resident leaf working set on the leaf's
/// activation and deactivation hooks (issue #2767).
/// <para>
/// <see cref="LeafResidentWorkingSetTests"/> pins the shedding policy in
/// isolation, by handing the ledger a class flag directly. That is necessary and
/// not sufficient: a policy that orders two classes correctly proves nothing if
/// the production wiring only ever registers one of them. These tests therefore
/// drive the <b>real</b> condition through a full activation - a leaf with no
/// snapshot comes up cold, a leaf whose snapshot outruns its checkpoint comes up
/// warm - rather than passing the flag in, because a test that passed the flag
/// in would only prove the flag was passed to itself.
/// </para>
/// </summary>
[TestFixture]
public class BPlusLeafGrainResidentWorkingSetTests
{
    private static string UniqueResidencyTree()
        => $"tree-residency-{Guid.NewGuid():N}";

    /// <summary>
    /// Builds a leaf wired to an explicit resident working set. Injected through
    /// activation services rather than the process-wide instance, so
    /// concurrently running fixtures cannot see each other's budget and the
    /// budget itself is deterministic rather than a function of the machine.
    /// </summary>
    private static (BPlusLeafGrain Grain, IGrainContext Context, FakePersistentState<LeafNodeState> State) CreateLeaf(
        LeafResidentWorkingSet workingSet,
        string treeId,
        LeafSnapshotBlob? preloadedSnapshot,
        long persistedCheckpoint,
        long walHead)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(preloadedSnapshot));

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(walHead));
        coord.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(workingSet);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                WalPartitions = 1,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, context, state);
    }

    private static LeafSnapshotBlob NewBlob(long offset)
        => new()
        {
            SnapshotOffset = offset,
            Rows = new List<LeafSnapshotRow>
            {
                new("a", new LwwValue<byte[]> { Value = [1], Timestamp = HybridLogicalClock.Zero }),
                new("b", new LwwValue<byte[]> { Value = [2], Timestamp = HybridLogicalClock.Zero }),
            },
            CapturedAtTicks = 1L,
        };

    [Test]
    public async Task A_cold_activation_is_accounted_even_though_it_holds_no_snapshot()
    {
        // The clause under test. A cold leaf holds decoded rows and no frame, so
        // an implementation that only accounted snapshot-backed activations
        // would leave this class entirely unbounded while still reporting the
        // silo as within budget - and would make the banked-before-unbanked
        // ordering vacuous, since every registration would then be banked.
        var workingSet = new LeafResidentWorkingSet(64L * 1024 * 1024);
        var (grain, _, _) = CreateLeaf(
            workingSet,
            UniqueResidencyTree(),
            preloadedSnapshot: null,
            persistedCheckpoint: 5L,
            walHead: 5L);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(
            workingSet.RegisteredCount,
            Is.EqualTo(1),
            "a cold activation must be accounted against the resident working set");
    }

    [Test]
    public async Task A_snapshot_backed_activation_is_accounted()
    {
        var workingSet = new LeafResidentWorkingSet(64L * 1024 * 1024);
        var (grain, _, _) = CreateLeaf(
            workingSet,
            UniqueResidencyTree(),
            preloadedSnapshot: NewBlob(offset: 50L),
            persistedCheckpoint: 10L,
            walHead: 50L);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Proves the rehydrate really happened, so this is the banked class
            // by the production condition and not by an inert code path.
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }));
            Assert.That(workingSet.RegisteredCount, Is.EqualTo(1));
            Assert.That(workingSet.ResidentBytes, Is.GreaterThan(0L));
        });
    }

    [Test]
    public async Task Deactivation_returns_the_activation_bytes_to_the_working_set()
    {
        var workingSet = new LeafResidentWorkingSet(64L * 1024 * 1024);
        var (grain, _, _) = CreateLeaf(
            workingSet,
            UniqueResidencyTree(),
            preloadedSnapshot: NewBlob(offset: 50L),
            persistedCheckpoint: 10L,
            walHead: 50L);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        Assert.That(workingSet.RegisteredCount, Is.EqualTo(1), "precondition");

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(workingSet.RegisteredCount, Is.Zero);
            Assert.That(
                workingSet.ResidentBytes,
                Is.Zero,
                "a leaked registration is permanent and drives the silo to shed leaves that are in use");
        });
    }

    [Test]
    public async Task An_over_budget_admission_deactivates_an_older_leaf_gracefully()
    {
        // One byte of budget, so the second activation is necessarily over it
        // and the first is the only candidate.
        var workingSet = new LeafResidentWorkingSet(1L);
        var tree = UniqueResidencyTree();

        var (first, firstContext, _) = CreateLeaf(
            workingSet, tree, NewBlob(offset: 50L), persistedCheckpoint: 10L, walHead: 50L);
        await ((IGrainBase)first).OnActivateAsync(CancellationToken.None);

        firstContext.DidNotReceive().Deactivate(Arg.Any<DeactivationReason>(), Arg.Any<CancellationToken>());

        var (second, _, _) = CreateLeaf(
            workingSet, tree, NewBlob(offset: 50L), persistedCheckpoint: 10L, walHead: 50L);
        await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);

        firstContext.Received(1).Deactivate(
            Arg.Is<DeactivationReason>(r => r.ReasonCode == DeactivationReasonCode.ApplicationRequested),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_leaf_that_is_mid_split_is_not_shed_by_another_leafs_admission()
    {
        // The wiring counterpart to the ledger-level pin tests. It proves the
        // grain supplies a predicate keyed to the *persisted* split state, and
        // that the predicate is evaluated at selection time rather than at
        // registration: the leaf below registers unpinned and only then enters
        // a split, which is the real sequence.
        var workingSet = new LeafResidentWorkingSet(1L);
        var tree = UniqueResidencyTree();

        var (first, firstContext, firstState) = CreateLeaf(
            workingSet, tree, NewBlob(offset: 50L), persistedCheckpoint: 10L, walHead: 50L);
        await ((IGrainBase)first).OnActivateAsync(CancellationToken.None);

        firstState.State.SplitState = SplitState.SplitInProgress;
        firstState.State.SplitKey = "k";

        var (second, _, _) = CreateLeaf(
            workingSet, tree, NewBlob(offset: 50L), persistedCheckpoint: 10L, walHead: 50L);
        await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);

        firstContext.DidNotReceive().Deactivate(
            Arg.Any<DeactivationReason>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_leaf_with_no_tree_id_is_not_accounted()
    {
        var workingSet = new LeafResidentWorkingSet(64L * 1024 * 1024);
        var (grain, _, _) = CreateLeaf(
            workingSet,
            treeId: string.Empty,
            preloadedSnapshot: null,
            persistedCheckpoint: 0L,
            walHead: 0L);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(
            workingSet.RegisteredCount,
            Is.Zero,
            "a leaf with no tree id retains nothing and would only add an unsheddable entry");
    }
}
