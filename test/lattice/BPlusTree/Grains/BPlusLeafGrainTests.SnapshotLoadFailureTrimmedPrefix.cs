using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4450: a snapshot that fails to load must not
/// send the leaf down the cold replay.
/// <para>
/// Under coverage-gated trim the steady state of a snapshot covering
/// <c>[0, C]</c> is a WAL tail of <c>C + 1</c>. A failed load used to read as
/// "no snapshot", so the activation took the <c>-1</c> cold override and replayed
/// the readable WAL. The cold-path fall-off guard fires only on
/// <c>tail &gt; persisted + 1</c>, which passes at <c>tail == C + 1</c>, so the
/// leaf came up holding only <c>(C, head)</c> - silently missing acknowledged
/// writes - and the cold flag then authorised a deactivation capture that wrote
/// that lossy cache over the good blob. Found by the WAL durability TLA+ model
/// (epic #4430, mutation <c>ReadPositionHonestLoadFailureColdReplays</c>).
/// </para>
/// <para>
/// A failed load now fails the replay closed whatever the tail reads, and the
/// barrier re-arm retries it. Gating the cold path on an intact tail is not
/// enough: the model found the GC trimming under the failed snapshot's
/// unlowerable pin while the rebuild ran.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot, ILeafReplayCoordinatorGrain Coordinator)
        CreateTrimmedPrefixLeaf(long persistedCheckpoint, long head, long tail, IReadOnlyList<CommitLogSliceEntry> entries)
    {
        var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LeafSnapshotSaveOutcome.Kept));

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
        coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(tail));
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(ReplaySliceStub.Unfiltered(
                entries, call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2))));
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(ReplaySliceStub.Filtered(
                entries, call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), call.ArgAt<WalKeyFilter>(3))));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshot);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ICommitLogReader>());
        services.AddSingleton(Substitute.For<ILeafCursorReporter>());

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = MaterialiserTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        state.State.ProjectionCheckpointOffsetAssigned = true;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.FromHours(1),
                MaterialiserCheckpointEntries = 1_000_000,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state, snapshot, coordinator);
    }

    private static CommitLogSliceEntry TrimmedPrefixEntry(long offset) =>
        new(offset, BuildCommittedSet($"k{offset}", Encoding.UTF8.GetBytes($"v{offset}")));

    private static LeafSnapshotBlob TrimmedPrefixSnapshot(long coveredOffset)
    {
        var rows = new List<LeafSnapshotRow>();
        for (var offset = 0L; offset <= coveredOffset; offset++)
        {
            rows.Add(new LeafSnapshotRow($"k{offset}", new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes($"v{offset}"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
            }));
        }

        return new LeafSnapshotBlob
        {
            SnapshotOffset = coveredOffset,
            Rows = rows,
            CapturedAtTicks = 1L,
            SnapshotOffsetsByPartition = new[] { coveredOffset },
        };
    }

    private static async Task<Exception?> ActivateCapturingFaultAsync(BPlusLeafGrain grain)
    {
        try
        {
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Test]
    public async Task Failed_snapshot_load_over_a_trimmed_wal_prefix_fails_the_replay_instead_of_coming_up_from_the_suffix()
    {
        // The WAL was trimmed through offset 5 under a snapshot covering [0, 5]
        // (pin = min(persisted 5, covered 5)), so the tail is 6. The snapshot then
        // fails to load with an ordinary storage fault.
        var (grain, state, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: [TrimmedPrefixEntry(6), TrimmedPrefixEntry(7)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<LeafSnapshotBlob?>(new InvalidOperationException("storage transient")));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
                "The only durable copy of [0, 5] is the snapshot that failed to load, so the replay must fail "
                + "closed rather than come up from the surviving suffix. It came up holding: "
                + $"[{string.Join(", ", grain.EntriesForTest.Keys)}].");
            Assert.That(fault?.InnerException?.Message, Is.EqualTo("storage transient"),
                "The storage fault that caused the decline must travel with it.");
            Assert.That(grain.EntriesForTest, Is.Empty,
                "Nothing may be replayed into the cache from the suffix.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L),
                "The persisted checkpoint must be untouched, so the retry sees exactly what this attempt saw.");
        });
    }

    [Test]
    public async Task Failed_snapshot_load_over_a_trimmed_prefix_at_checkpoint_zero_fails_the_replay()
    {
        // The cold-path fall-off guard is gated on persistedCheckpoint > 0, so a
        // snapshot covering only offset 0 with the WAL trimmed to tail 1 was
        // invisible to it as well as to the tail > persisted + 1 test.
        var (grain, _, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 0, head: 3, tail: 1, entries: [TrimmedPrefixEntry(1), TrimmedPrefixEntry(2)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<LeafSnapshotBlob?>(new InvalidOperationException("storage transient")));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
            $"Came up holding [{string.Join(", ", grain.EntriesForTest.Keys)}] without k0.");
    }

    [Test]
    public async Task Unreadable_snapshot_payload_over_a_trimmed_wal_prefix_fails_the_replay()
    {
        // A blob whose frame is corrupt is rejected by ValidateRowPayload. That is
        // a failed load of a snapshot that exists, not the absence of one.
        var (grain, _, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: [TrimmedPrefixEntry(6), TrimmedPrefixEntry(7)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
        {
            SnapshotOffset = 5,
            EncodedRows = [1, 2, 3, 4, 5, 6, 7, 8],
            CapturedAtTicks = 1L,
            SnapshotOffsetsByPartition = [5],
        }));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
            $"Came up holding [{string.Join(", ", grain.EntriesForTest.Keys)}].");
    }

    [Test]
    public async Task Missing_snapshot_segment_over_a_trimmed_wal_prefix_fails_the_replay()
    {
        var (grain, _, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: [TrimmedPrefixEntry(6), TrimmedPrefixEntry(7)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
        {
            SnapshotOffset = 5,
            SegmentCount = 2,
            CapturedAtTicks = 1L,
            SnapshotOffsetsByPartition = [5],
        }));
        snapshot.LoadSegmentFrameAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<byte[]?>(null));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
            $"Came up holding [{string.Join(", ", grain.EntriesForTest.Keys)}].");
    }

    [Test]
    public async Task Failed_snapshot_load_over_an_intact_wal_fails_the_replay_closed_too()
    {
        // An intact tail is not a licence: it holds only at the instant it is
        // read. The leaf's durable pin was resolved against the coverage of the
        // snapshot that just failed, the pin store cannot lower it, and so the WAL
        // GC stays entitled to trim the covered prefix while the cold rebuild runs
        // (the WAL durability model's ColdStartOverIntactWalRace trace).
        var (grain, state, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 2, head: 3, tail: 0,
            entries: [TrimmedPrefixEntry(0), TrimmedPrefixEntry(1), TrimmedPrefixEntry(2)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<LeafSnapshotBlob?>(new InvalidOperationException("storage transient")));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
                $"Came up holding [{string.Join(", ", grain.EntriesForTest.Keys)}] from a cold replay the GC could trim under.");
            Assert.That(grain.EntriesForTest, Is.Empty);
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task Absent_snapshot_over_an_empty_cache_still_replays_cold()
    {
        // The store answering "no snapshot" is not a failed load, and keeps the
        // cold path and its fall-off guards exactly as before.
        var (grain, state, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 2, head: 3, tail: 0,
            entries: [TrimmedPrefixEntry(0), TrimmedPrefixEntry(1), TrimmedPrefixEntry(2)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.Null);
            Assert.That(grain.EntriesForTest.Keys, Is.SupersetOf(new[] { "k0", "k1", "k2" }));
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task Failed_snapshot_load_over_a_trimmed_prefix_does_not_authorise_a_deactivation_capture()
    {
        // The cold path used to set _cacheRebuiltFromWalStartThisActivation, which
        // authorised the graceful-deactivation capture to write the lossy cache
        // over the good-but-unloadable blob, making the loss permanent.
        var (grain, _, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: [TrimmedPrefixEntry(6), TrimmedPrefixEntry(7)]);
        snapshot.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<LeafSnapshotBlob?>(new InvalidOperationException("storage transient")));

        _ = await ActivateCapturingFaultAsync(grain);
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        await snapshot.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Leaf_that_failed_closed_on_a_snapshot_load_recovers_once_the_snapshot_loads()
    {
        // Failing closed is transient: the barrier re-arms on the next touch, and
        // once the store answers the leaf comes up holding the covered prefix
        // plus the replayed suffix.
        var (grain, _, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: [TrimmedPrefixEntry(6), TrimmedPrefixEntry(7)]);
        var attempts = 0;
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(_ => ++attempts == 1
            ? Task.FromException<LeafSnapshotBlob?>(new InvalidOperationException("storage transient"))
            : Task.FromResult<LeafSnapshotBlob?>(TrimmedPrefixSnapshot(5)));

        var fault = await ActivateCapturingFaultAsync(grain);
        Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>());

        // The first data operation observes the faulted replay and disarms it,
        // exactly as in production; the next one re-arms and retries the load.
        Assert.ThrowsAsync<LeafSnapshotUnavailableException>(async () => await grain.GetAsync("k0"));
        var recovered = await grain.GetAsync("k0");

        Assert.Multiple(() =>
        {
            Assert.That(recovered, Is.EqualTo(Encoding.UTF8.GetBytes("v0")));
            Assert.That(grain.EntriesForTest.Keys,
                Is.SupersetOf(new[] { "k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7" }));
        });
    }
}
