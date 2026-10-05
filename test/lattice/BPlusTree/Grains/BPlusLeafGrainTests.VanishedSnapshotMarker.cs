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
/// The leaf now keeps a durable kept-snapshot coverage marker in a sidecar,
/// raised before any pin that licenses a trim behind the snapshot, and a cold
/// start over an absent snapshot fails closed when the marker shows a kept
/// snapshot covered a partition whose WAL has since been trimmed.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// In-memory kept-snapshot coverage marker sidecar with fault switches and a
    /// shared event log, so a fixture can order the marker's raise against the
    /// leaf's pin publication.
    /// </summary>
    private sealed class FakeCoverageMarker(List<string>? events = null) : ILeafSnapshotCoverageMarkerGrain
    {
        public long[]? Covered { get; set; }

        public Exception? ThrowOnGet { get; set; }

        public Exception? ThrowOnRaise { get; set; }

        public int Clears { get; private set; }

        public Task<long[]?> GetAsync()
        {
            events?.Add("marker-get");
            return ThrowOnGet is { } fault ? Task.FromException<long[]?>(fault) : Task.FromResult(Covered);
        }

        public Task RaiseAsync(long[] covered)
        {
            if (ThrowOnRaise is { } fault)
            {
                ThrowOnRaise = null;
                events?.Add("marker-raise-failed");
                return Task.FromException(fault);
            }

            events?.Add($"marker-raise:{string.Join(",", covered)}");
            Covered = LeafSnapshotCoverageMarkerGrain.Raise(Covered, covered) ?? Covered;
            return Task.CompletedTask;
        }

        public Task ClearAsync()
        {
            Clears++;
            Covered = null;
            return Task.CompletedTask;
        }
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot, ILeafReplayCoordinatorGrain Coordinator)
        CreateVanishedSnapshotLeaf(FakeCoverageMarker marker, long persistedCheckpoint, long head, long tail, IReadOnlyList<CommitLogSliceEntry> entries)
    {
        var (_, state, snapshot, coordinator) = CreateTrimmedPrefixLeaf(persistedCheckpoint, head, tail, entries);
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), marker);
        return (grain, state, snapshot, coordinator);
    }

    private static CommitLogSliceEntry[] TrimmedPrefixEntries(long from, long throughInclusive)
    {
        var entries = new List<CommitLogSliceEntry>();
        for (var offset = from; offset <= throughInclusive; offset++)
            entries.Add(TrimmedPrefixEntry(offset));
        return entries.ToArray();
    }

    [Test]
    public async Task Vanished_snapshot_over_a_wal_trimmed_under_it_fails_the_replay_instead_of_coming_up_from_the_suffix()
    {
        // A snapshot covering [0, 5] was kept, so the GC trimmed through 5 and the
        // tail is 6. The snapshot then vanished: the store answers, with nothing.
        var marker = new FakeCoverageMarker { Covered = [5] };
        var (grain, state, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
                "The only durable copy of [0, 5] was the snapshot that vanished, so the replay must fail closed "
                + $"rather than come up from the surviving suffix. It came up holding: [{string.Join(", ", grain.EntriesForTest.Keys)}].");
            Assert.That(fault?.InnerException?.Message, Does.Contain("#4634").And.Contain("trimmed to offset 6"),
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
        var marker = new FakeCoverageMarker { Covered = [0] };
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 0, head: 3, tail: 1, entries: TrimmedPrefixEntries(1, 2));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>(),
            $"Came up holding [{string.Join(", ", grain.EntriesForTest.Keys)}] without k0.");
    }

    [Test]
    public async Task Vanished_snapshot_over_an_untrimmed_wal_comes_up_cold_with_every_entry()
    {
        // The snapshot was kept but the WAL was never trimmed under it, so the
        // cold rebuild has everything it needs and nothing is lost.
        var marker = new FakeCoverageMarker { Covered = [5] };
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.Null);
        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(Enumerable.Range(0, 8).Select(i => $"k{i}")));
    }

    [Test]
    public async Task Absent_snapshot_with_no_kept_marker_comes_up_cold_as_before()
    {
        // A leaf that never kept a snapshot has no marker: an absent snapshot is
        // simply the normal first start, and the cold rebuild proceeds.
        var marker = new FakeCoverageMarker();
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.Null);
        Assert.That(grain.EntriesForTest, Has.Count.EqualTo(8));
    }

    [Test]
    public async Task Absent_snapshot_on_a_leaf_with_no_persisted_checkpoint_does_not_consult_the_marker()
    {
        // Only a checkpointed leaf can have had a prefix covered and trimmed, so
        // a brand-new leaf's first start pays no marker read at all.
        var events = new List<string>();
        var marker = new FakeCoverageMarker(events) { ThrowOnGet = new InvalidOperationException("must not be read") };
        var (grain, state, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: -1, head: 3, tail: 0, entries: TrimmedPrefixEntries(0, 2));
        state.State.ProjectionCheckpointOffsetAssigned = false;

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.Null);
        Assert.That(events, Does.Not.Contain("marker-get"));
    }

    [Test]
    public async Task Absent_snapshot_whose_marker_cannot_be_read_fails_the_replay_closed()
    {
        var marker = new FakeCoverageMarker { ThrowOnGet = new TimeoutException("marker store unreachable") };
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>(),
                "A leaf that cannot tell a lost snapshot from one that never existed must not guess.");
            Assert.That(fault?.InnerException?.InnerException, Is.InstanceOf<TimeoutException>());
            Assert.That(grain.EntriesForTest, Is.Empty);
        });
    }

    [Test]
    public async Task Absent_snapshot_whose_wal_tail_cannot_be_read_fails_the_replay_closed()
    {
        var marker = new FakeCoverageMarker { Covered = [5] };
        var (grain, _, _, coordinator) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));
        coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<long>(new TimeoutException("wal unreachable")));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.That(fault, Is.InstanceOf<LeafSnapshotUnavailableException>());
        Assert.That(grain.EntriesForTest, Is.Empty);
    }

    [Test]
    public async Task Pin_flush_makes_the_kept_snapshot_marker_durable_before_publishing_any_pin()
    {
        var events = new List<string>();
        var marker = new FakeCoverageMarker(events);
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
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), marker, reporter);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await grain.FlushDurableMaterialiserFrontierAsync();

        var raise = events.FindIndex(e => e.StartsWith("marker-raise:", StringComparison.Ordinal));
        var publish = events.IndexOf("pin-publish");
        Assert.Multiple(() =>
        {
            Assert.That(publish, Is.GreaterThanOrEqualTo(0), "control: the flush must publish a pin.");
            Assert.That(raise, Is.GreaterThanOrEqualTo(0), "The loaded snapshot's coverage must reach the marker.");
            Assert.That(raise, Is.LessThan(publish),
                $"The marker must be durable before any pin that licenses a trim behind the snapshot. Events: [{string.Join(", ", events)}].");
            Assert.That(marker.Covered?[0], Is.GreaterThanOrEqualTo(5L),
                "The marker records at least the coverage of the snapshot the leaf loaded.");
        });
    }

    [Test]
    public async Task Failed_marker_raise_publishes_no_pin_and_the_next_flush_retries_it()
    {
        var events = new List<string>();
        var marker = new FakeCoverageMarker(events);
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
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), marker, reporter);
        marker.ThrowOnRaise = new TimeoutException("marker store unreachable");

        // Activation may itself reach the flush; whichever flush meets the
        // fault first, no pin may be published by it.
        try
        {
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        }
        catch (TimeoutException)
        {
        }

        if (marker.ThrowOnRaise is not null)
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await grain.FlushDurableMaterialiserFrontierAsync());
        }

        Assert.That(events, Does.Contain("marker-raise-failed"), "control: the raise must actually fail.");
        Assert.That(events, Does.Not.Contain("pin-publish"),
            "A pin published over an undurable marker licenses a trim that a vanished snapshot could then turn into silent loss.");

        await grain.FlushDurableMaterialiserFrontierAsync();

        Assert.Multiple(() =>
        {
            Assert.That(events.FindIndex(e => e.StartsWith("marker-raise:", StringComparison.Ordinal)), Is.GreaterThanOrEqualTo(0),
                "The pending raise must survive the failure and be retried.");
            Assert.That(events.FindIndex(e => e.StartsWith("marker-raise:", StringComparison.Ordinal)), Is.LessThan(events.IndexOf("pin-publish")));
            Assert.That(marker.Covered?[0], Is.GreaterThanOrEqualTo(5L));
        });
    }

    [Test]
    public async Task Rebuild_over_a_vanished_snapshot_clears_the_marker_and_the_leaf_comes_back_accepting_the_loss()
    {
        var marker = new FakeCoverageMarker { Covered = [5] };
        var (grain, state, snapshot, coordinator) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 6, entries: TrimmedPrefixEntries(6, 7));

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.InstanceOf<LeafSnapshotUnavailableException>());

        await grain.RebuildProjectionFromWalAsync();
        Assert.Multiple(() =>
        {
            Assert.That(marker.Covered, Is.Null, "The rebuild accepts the loss, so the record of the snapshot goes.");
            Assert.That(marker.Clears, Is.EqualTo(1));
        });

        var next = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), marker);
        Assert.That(await ActivateCapturingFaultAsync(next), Is.Null);
        Assert.That(next.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k6", "k7" }),
            "Offsets 0..5 lived only in the vanished snapshot; that is the loss the rebuild accepted.");
    }

    [Test]
    public async Task Clearing_the_leaf_clears_its_kept_snapshot_marker()
    {
        var marker = new FakeCoverageMarker { Covered = [5] };
        var (grain, _, _, _) = CreateVanishedSnapshotLeaf(
            marker, persistedCheckpoint: 5, head: 8, tail: 0, entries: TrimmedPrefixEntries(0, 7));
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.ClearGrainStateAsync();

        Assert.That(marker.Covered, Is.Null, "A removed leaf must not leave a marker that fails a reused key closed.");
    }
}
