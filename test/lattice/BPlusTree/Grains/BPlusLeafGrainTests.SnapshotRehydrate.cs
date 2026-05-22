using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the activation-time snapshot rehydration seam
/// added in R-120 step 7.4. On activation, the leaf consults the
/// dedicated snapshot storage grain; when a blob exists whose
/// <c>SnapshotOffset</c> exceeds the persisted
/// <c>ProjectionCheckpointOffset</c>, the leaf rebuilds the
/// in-memory entry cache from the canonical byte rows and advances
/// the persisted checkpoint to the snapshot's offset before driving
/// the existing WAL tail replay.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain SnapshotStub, ILeafReplayCoordinatorGrain Coordinator) CreateGrainWithSnapshotAndCoordinator(
        LeafSnapshotBlob? preloadedSnapshot,
        long persistedCheckpoint,
        long walHead)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(preloadedSnapshot));

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
        var services = sc.BuildServiceProvider();

        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-rehydrate";
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var baseOptions = new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state, snapshotStub, coord);
    }

    private static LeafSnapshotBlob NewSnapshotBlob(long offset, params (string key, byte[] value)[] rows)
    {
        var list = new List<LeafSnapshotRow>(rows.Length);
        foreach (var (k, v) in rows)
        {
            list.Add(new LeafSnapshotRow(k, new LwwValue<byte[]>
            {
                Value = v,
                Timestamp = HybridLogicalClock.Zero,
            }));
        }
        return new LeafSnapshotBlob
        {
            SnapshotOffset = offset,
            Rows = list,
            CapturedAtTicks = 1L,
        };
    }

    [Test]
    public async Task Activation_rehydrates_cache_from_snapshot_when_offset_exceeds_checkpoint()
    {
        var blob = NewSnapshotBlob(
            offset: 50,
            ("a", new byte[] { 1 }),
            ("b", new byte[] { 2 }));
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 10,
            walHead: 50);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(50L));
        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }));
        Assert.That(state.State.ProjectionHash, Is.Null,
            "Digest must be invalidated so the lazy backfill re-folds the canonical full-walk hash.");
    }

    [Test]
    public async Task Activation_ignores_snapshot_older_than_persisted_checkpoint()
    {
        var blob = NewSnapshotBlob(offset: 5, ("a", new byte[] { 1 }));
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 20,
            walHead: 20);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Checkpoint unchanged (snapshot was older), and the cache must
        // not have been hydrated from the blob's rows.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(20L));
        Assert.That(grain.EntriesForTest, Is.Empty);
    }

    [Test]
    public async Task Activation_ignores_snapshot_at_equal_offset()
    {
        // Equal offset == "we have already absorbed everything the
        // snapshot contains via the WAL"; ignore to avoid the
        // pointless cache replace.
        var blob = NewSnapshotBlob(offset: 30, ("a", new byte[] { 1 }));
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 30,
            walHead: 30);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(30L));
        Assert.That(grain.EntriesForTest, Is.Empty);
    }

    [Test]
    public async Task Activation_no_op_when_no_snapshot_present()
    {
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null,
            persistedCheckpoint: 5,
            walHead: 5);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L));
    }

    [Test]
    public async Task Activation_handles_blob_with_null_rows_collection_defensively()
    {
        // LeafSnapshotBlob.Rows is documented as never-null and
        // defaults to Array.Empty, but the setter is public and a
        // partially-deserialised blob (or a future format-rev) could
        // surface null. The rehydrate path must treat null Rows as
        // an empty row set rather than NPE-ing on the foreach.
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 25,
            Rows = null!,
            CapturedAtTicks = 1L,
        };
        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: blob,
            persistedCheckpoint: 10,
            walHead: 25);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Checkpoint still advances to the snapshot offset; the cache
        // is empty (no rows were carried) and the digest is invalidated
        // so the lazy backfill recomputes from the empty cache.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(25L));
        Assert.That(grain.EntriesForTest, Is.Empty);
        Assert.That(state.State.ProjectionHash, Is.Null);
    }

    [Test]
    public async Task Activation_snapshot_load_failure_falls_through_to_WAL_replay()
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<LeafSnapshotBlob?>(new InvalidOperationException("storage transient")));

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(10L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-rehydrate";
        state.State.ProjectionCheckpointOffset = 5;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        // Must not throw - snapshot load failures are best-effort and
        // the leaf falls through to the WAL tail-replay path.
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L));
        await coord.Received().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
    }
}