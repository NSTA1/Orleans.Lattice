using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4634: a snapshot that has VANISHED must not
/// send the leaf down the cold replay.
/// <para>
/// Issue #4450 failed the replay closed when a snapshot fails to load. An
/// absent snapshot still read as "never had one": under coverage-gated trim the
/// WAL GC had trimmed the prefix the snapshot covered, the cold rebuild replayed
/// only the surviving suffix, and the fall-off guard - which compares the tail
/// with the persisted checkpoint, a bound the trim stays within - passed. Every
/// key whose only durable copy was the snapshot then read as absent.
/// </para>
/// <para>
/// The leaf now records, per WAL partition and in its own state row, that a
/// snapshot covering the partition was kept. The durable pin flush persists that
/// record before it publishes any pin, and a cold start over an absent snapshot
/// fails closed when a recorded partition's WAL has since been trimmed.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot, ILeafReplayCoordinatorGrain Coordinator)
        CreateVanishedSnapshotLeaf(bool[]? keptCoverage, long persistedCheckpoint, long head, long tail, IReadOnlyList<CommitLogSliceEntry> entries)
    {
        var (_, state, snapshot, coordinator) = CreateTrimmedPrefixLeaf(persistedCheckpoint, head, tail, entries);
        state.State.SnapshotCoveredPartitions = keptCoverage;
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid());
        return (grain, state, snapshot, coordinator);
    }

    private static CommitLogSliceEntry[] TrimmedPrefixEntries(long from, long throughInclusive)
    {
        var entries = new List<CommitLogSliceEntry>();
        for (var offset = from; offset <= throughInclusive; offset++)
            entries.Add(TrimmedPrefixEntry(offset));
        return entries.ToArray();
    }

    /// <summary>
    /// A write observer that records the projection checkpoint of every state
    /// write except one whose only purpose was to make a newly set kept-snapshot
    /// flag durable (issue #4634): that write happens at most once per partition
    /// per leaf lifetime, carries the checkpoint unchanged, and is not part of
    /// the checkpoint cadence a fixture measures. Pass <paramref name="durableBefore"/>
    /// when observation starts after the leaf has already written its row.
    /// </summary>
    internal static Action<LeafNodeState> CheckpointAdvancingWrites(List<long> offsets, LeafNodeState? durableBefore = null)
    {
        // Only the checkpoint is seeded: in-memory flags may be set but not yet durable.
        bool[]? lastFlags = null;
        long? lastOffset = durableBefore?.ProjectionCheckpointOffset;
        long[]? lastByPartition = durableBefore?.ProjectionCheckpointOffsetsByPartition?.ToArray();
        return written =>
        {
            var flags = written.SnapshotCoveredPartitions;
            var flagRose = false;
            if (flags is not null)
            {
                for (var p = 0; p < flags.Length; p++)
                {
                    if (flags[p] && (lastFlags is null || p >= lastFlags.Length || !lastFlags[p]))
                        flagRose = true;
                }
            }

            var byPartition = written.ProjectionCheckpointOffsetsByPartition;
            var sameCheckpoint = lastOffset == written.ProjectionCheckpointOffset
                && (byPartition ?? []).SequenceEqual(lastByPartition ?? []);
            if (!(flagRose && sameCheckpoint))
                offsets.Add(written.ProjectionCheckpointOffset);

            lastFlags = flags?.ToArray();
            lastOffset = written.ProjectionCheckpointOffset;
            lastByPartition = byPartition?.ToArray();
        };
    }

    /// <summary>
    /// A leaf that loads a snapshot covering [0, 5], with every durable state
    /// write and every published pin logged in order.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, List<string> Events) CreateKeptSnapshotLeafWithEventLog()
    {
        var events = new List<string>();
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(), Arg.Any<IReadOnlyList<MaterialiserPinReport>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                events.Add("pin-publish");
                return Task.FromResult(true);
            });
        var (_, state, snapshot, coordinator) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(TrimmedPrefixSnapshot(5)));
        state.OnWriteState = written => events.Add(
            written.SnapshotCoveredPartitions is { Length: > 0 } flags && flags[0] ? "write:covered" : "write:uncovered");
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), reporter);
        return (grain, state, events);
    }

    [Test]
    public async Task Vanished_snapshot_over_a_wal_trimmed_under_it_fails_the_replay_instead_of_coming_up_from_the_suffix()
    {
        // A snapshot covering [0, 5] was kept, so the GC trimmed through 5 and the
        // tail is 6. The snapshot then vanished: the store answers, with nothing.
        var (grain, state, _, _) = CreateVanishedSnapshotLeaf(
            [true], persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
                "The only durable copy of [0, 5] was the snapshot that vanished, so the replay must fail closed "
                + $"rather than come up from the surviving suffix. It came up holding: [{string.Join(", ", grain.EntriesForTest.Keys)}].");
            Assert.That(fault?.InnerException?.Message, Does.Contain("#4634").And.Contain("partition 0"),
                "The cause must name the lost snapshot and the trim under it.");
            Assert.That(grain.EntriesForTest, Is.Empty, "Nothing may be replayed into the cache from the suffix.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L),
                "The persisted checkpoint must be untouched, so a restored snapshot is picked up by the retry.");
        });
        Assert.ThrowsAsync<LeafSnapshotUnavailableException>(async () => await grain.GetAsync("k6"),
            "Reads fail closed too, rather than reporting k0..k5 absent.");
    }

    [Test]
    public async Task Vanished_snapshot_at_checkpoint_zero_over_a_trimmed_wal_fails_the_replay()
    {
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            [true], persistedCheckpoint: 0, head: 3, tail: 1, entries: TrimmedPrefixEntries(1, 2));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>(),
            $"Came up holding [{string.Join(", ", grain.EntriesForTest.Keys)}] without k0.");
    }

    [Test]
    public async Task Vanished_snapshot_over_an_untrimmed_wal_still_fails_the_replay_closed()
    {
        // The WAL is not trimmed when the leaf activates, but the vanished
        // snapshot's durable pin cannot be lowered, so the WAL GC may trim the
        // prefix while the cold rebuild runs. The tail at activation proves
        // nothing, and is not consulted (the WAL spec's SnapshotLoss variant).
        var (grain, _, _, coordinator) = CreateVanishedSnapshotLeaf(
            [true], persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>());
        Assert.That(grain.EntriesForTest, Is.Empty);
    }

    [Test]
    public async Task Absent_snapshot_with_no_kept_record_comes_up_cold_as_before()
    {
        // A leaf that never kept a snapshot has no record: an absent snapshot is
        // the normal first start, and a trimmed tail is someone else's trim,
        // which the existing fall-off guard judges.
        var (grain, _, _, coordinator) = CreateVanishedSnapshotLeaf(
            null, persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.Null);
        Assert.That(grain.EntriesForTest, Has.Count.EqualTo(8));
    }

    [Test]
    public async Task Absent_snapshot_with_a_kept_record_for_any_partition_fails_the_replay_closed()
    {
        // Partition 0 never had a snapshot kept, but partition 1 did: its pin may
        // still license a trim of partition 1 under the cold rebuild.
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            [false, true], persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>());
    }

    [Test]
    public async Task Absent_snapshot_with_an_all_clear_record_comes_up_cold()
    {
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            [false, false], persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.Null);
        Assert.That(grain.EntriesForTest, Has.Count.EqualTo(8));
    }

    [Test]
    public async Task Kept_snapshot_record_is_durable_in_the_leaf_row_before_any_pin_is_published()
    {
        var (grain, state, events) = CreateKeptSnapshotLeafWithEventLog();

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await grain.FlushDurableMaterialiserFrontierAsync();

        var covered = events.IndexOf("write:covered");
        var publish = events.IndexOf("pin-publish");
        Assert.Multiple(() =>
        {
            Assert.That(publish, Is.GreaterThanOrEqualTo(0), "control: the flush must publish a pin.");
            Assert.That(covered, Is.GreaterThanOrEqualTo(0),
                "The loaded snapshot's coverage must reach the leaf's own row.");
            Assert.That(covered, Is.LessThan(publish),
                $"The record must be durable before any pin that licenses a trim behind the snapshot. Events: [{string.Join(", ", events)}].");
            Assert.That(state.State.SnapshotCoveredPartitions?[0], Is.True);
        });
    }

    [Test]
    public async Task Kept_snapshot_record_is_written_once_not_on_every_flush()
    {
        var (grain, _, events) = CreateKeptSnapshotLeafWithEventLog();

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await grain.FlushDurableMaterialiserFrontierAsync();
        var writesAfterFirst = events.Count(e => e.StartsWith("write:", StringComparison.Ordinal));
        await grain.FlushDurableMaterialiserFrontierAsync();
        await grain.FlushDurableMaterialiserFrontierAsync();

        Assert.That(events.Count(e => e.StartsWith("write:", StringComparison.Ordinal)), Is.EqualTo(writesAfterFirst),
            "The record only ever turns on, so a flush with nothing newly set must not write.");
    }

    [Test]
    public async Task Failed_record_write_publishes_no_pin_and_the_next_flush_retries_it()
    {
        var (grain, state, events) = CreateKeptSnapshotLeafWithEventLog();
        state.ThrowOnWrite = new TimeoutException("leaf row store unreachable");

        // Activation may itself reach a write or the flush; whichever meets the
        // single-shot fault first, no pin may be published by the flush it fails.
        try
        {
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        }
        catch (TimeoutException)
        {
        }

        if (state.ThrowOnWrite is not null)
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await grain.FlushDurableMaterialiserFrontierAsync());
        }

        Assert.That(events, Does.Not.Contain("pin-publish"),
            "A pin published over an undurable record licenses a trim that a vanished snapshot could then turn into silent loss.");

        await grain.FlushDurableMaterialiserFrontierAsync();

        Assert.Multiple(() =>
        {
            Assert.That(events.IndexOf("write:covered"), Is.GreaterThanOrEqualTo(0),
                "The record must survive the failure and be retried.");
            Assert.That(events.IndexOf("write:covered"), Is.LessThan(events.IndexOf("pin-publish")));
        });
    }

    [Test]
    public async Task Rebuild_over_a_vanished_snapshot_drops_the_record_and_the_leaf_comes_back_accepting_the_loss()
    {
        var (grain, state, snapshot, coordinator) = CreateVanishedSnapshotLeaf(
            [true], persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));
        var durable = new List<bool[]?>();
        state.OnWriteState = written => durable.Add(written.SnapshotCoveredPartitions);

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>());

        await grain.RebuildProjectionFromWalAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.State.SnapshotCoveredPartitions, Is.Null,
                "The rebuild accepts the loss, so the record of the snapshot goes.");
            Assert.That(durable, Is.Not.Empty.And.Some.Null, "and the rebuild's own write makes that durable.");
        });

        var next = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid());
        Assert.That(await ActivateCapturingFaultAsync(next), Is.Null);
        Assert.That(next.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k6", "k7" }),
            "Offsets 0..5 lived only in the vanished snapshot; that is the loss the rebuild accepted.");
    }

    [Test]
    public async Task Rebuild_discarding_an_unreadable_snapshot_drops_the_record()
    {
        var (grain, state, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));
        state.State.SnapshotCoveredPartitions = [true];
        StoreUntilCleared(snapshot, UnreadableSnapshot(5));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>());
        await grain.RebuildProjectionFromWalAsync();

        Assert.That(state.State.SnapshotCoveredPartitions, Is.Null);
    }

    [Test]
    public async Task Rebuild_over_a_readable_snapshot_keeps_the_record()
    {
        var (grain, state, snapshot, _) = CreateTrimmedPrefixLeaf(
            persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));
        StoreUntilCleared(snapshot, TrimmedPrefixSnapshot(5));

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await grain.RebuildProjectionFromWalAsync();

        Assert.That(state.State.SnapshotCoveredPartitions?[0], Is.True,
            "The snapshot still exists and still covers the trimmed prefix, so the record must stay.");
    }
}
