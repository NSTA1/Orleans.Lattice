using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4383: the snapshot storage a leaf owns - one manifest row and one row
/// per segment - must be deletable reliably. Before this fix the clear removed
/// the manifest first and retired segments best-effort, so a segment delete that
/// failed could never be retried (the next attempt found no manifest and
/// returned), and a superseded generation whose retirement failed was never
/// revisited. Both left rows in storage for good.
/// <para>
/// These tests drive the real <see cref="LeafSnapshotStorageGrain"/> over an
/// in-memory segment store whose clears can be made to fail, and assert on the
/// rows left behind rather than on what <see cref="LeafSnapshotStorageGrain.LoadAsync"/>
/// reports - a tombstoned manifest loads as absent while its row still exists,
/// so a load-based assertion would pass on exactly the defect under test.
/// </para>
/// </summary>
public sealed partial class LeafSnapshotStorageGrainTests
{
    /// <summary>
    /// An in-memory segment row whose clear can be made to fail a set number of
    /// times, modelling a transient storage fault on the delete path.
    /// </summary>
    private sealed class CleanupSegment : ILeafSnapshotSegmentGrain
    {
        internal byte[]? Frame { get; private set; }

        internal int FailClears { get; set; }

        internal Action? OnClear { get; set; }

        public Task SaveAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
        {
            Frame = frame;
            return Task.CompletedTask;
        }

        public Task<byte[]?> LoadFrameAsync(CancellationToken cancellationToken) => Task.FromResult(Frame);

        public Task ClearAsync(CancellationToken cancellationToken)
        {
            if (FailClears > 0)
            {
                FailClears--;
                throw new TimeoutException("injected segment delete fault");
            }

            OnClear?.Invoke();
            Frame = null;
            return Task.CompletedTask;
        }

        public Task<bool> HasFrameAsync(CancellationToken cancellationToken) => Task.FromResult(Frame is not null);
    }

    private sealed class CleanupStore
    {
        internal required LeafSnapshotStorageGrain Grain { get; init; }

        internal required FakePersistentState<LeafSnapshotBlob> State { get; init; }

        internal required Dictionary<string, CleanupSegment> Segments { get; init; }

        internal required string LeafKey { get; init; }

        internal CleanupSegment Segment(int generation, int index)
        {
            var key = generation == 0 ? $"{LeafKey}/{index}" : $"{LeafKey}/g{generation}/{index}";
            if (!Segments.TryGetValue(key, out var segment))
            {
                segment = new CleanupSegment();
                Segments[key] = segment;
            }

            return segment;
        }

        internal string[] StoredSegmentKeys()
            => Segments.Where(kv => kv.Value.Frame is not null).Select(kv => kv.Key).Order(StringComparer.Ordinal).ToArray();
    }

    private static CleanupStore CreateCleanupStore()
    {
        var segments = new Dictionary<string, CleanupSegment>(StringComparer.Ordinal);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILeafSnapshotSegmentGrain>(Arg.Any<string>())
            .Returns(call =>
            {
                var key = call.ArgAt<string>(0);
                if (!segments.TryGetValue(key, out var segment))
                {
                    segment = new CleanupSegment();
                    segments[key] = segment;
                }

                return segment;
            });

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(new LatticeOptions
        {
            LeafSnapshotSegmentBytes = LatticeOptions.MinimumLeafSnapshotSegmentBytes,
        });

        var leafKey = Guid.NewGuid().ToString("N");
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf-snapshot", leafKey));

        var state = new FakePersistentState<LeafSnapshotBlob> { RecordExistsValue = false };
        var grain = new LeafSnapshotStorageGrain(context, state, factory, monitor);
        state.OnWriteState = _ => state.RecordExistsValue = true;

        return new CleanupStore { Grain = grain, State = state, Segments = segments, LeafKey = leafKey };
    }

    /// <summary>
    /// A blob several times the minimum segment window, so it persists as a
    /// segmented snapshot. The fill byte distinguishes one capture's rows from
    /// another's.
    /// </summary>
    private static LeafSnapshotBlob LargeBlob(long offset, byte fill = 1)
    {
        var rows = new LeafSnapshotRow[256];
        for (var i = 0; i < rows.Length; i++)
        {
            var payload = new byte[1024];
            Array.Fill(payload, fill);
            rows[i] = new LeafSnapshotRow(
                $"key-{i:D4}",
                LwwValue<byte[]>.Create(payload, new HybridLogicalClock { WallClockTicks = 1_000L + offset + i }));
        }

        return new LeafSnapshotBlob
        {
            SnapshotOffset = offset,
            SnapshotOffsetsByPartition = [offset],
            EncodedRows = LeafSnapshotCodec.Encode(rows),
        };
    }

    [Test]
    public async Task ClearAsync_deletes_the_manifest_row_and_every_segment_row()
    {
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        Assert.That(store.State.State.SegmentCount, Is.GreaterThan(1),
            "precondition: the blob must be segmented, or this test exercises the inline path only");

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.StoredSegmentKeys(), Is.Empty);
            Assert.That(store.State.RecordExists, Is.False, "the manifest row itself must be deleted");
        });
    }

    [Test]
    public async Task ClearAsync_stops_claiming_coverage_before_it_deletes_a_segment()
    {
        // A manifest still claiming coverage over segments that are being deleted
        // is a snapshot reporting coverage it cannot reproduce. The clear must
        // tombstone the manifest first, and the tombstone must still record every
        // segment owed so a failed delete can be retried.
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        var generation = store.State.State.SegmentGeneration;
        var count = store.State.State.SegmentCount;

        (long? Offset, long[]? Offsets, int Count, LeafSnapshotSegmentRange[]? Owed)? atFirstDelete = null;
        foreach (var segment in store.Segments.Values)
        {
            segment.OnClear = () => atFirstDelete ??= (
                store.State.State.SnapshotOffset,
                store.State.State.SnapshotOffsetsByPartition,
                store.State.State.SegmentCount,
                store.State.State.PendingSegmentRetirements?.ToArray());
        }

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.That(atFirstDelete, Is.Not.Null);
        var seen = atFirstDelete!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(seen.Offset, Is.Null);
            Assert.That(seen.Offsets, Is.Null);
            Assert.That(seen.Count, Is.Zero);
            Assert.That(seen.Owed, Does.Contain(new LeafSnapshotSegmentRange(generation, 0, count)));
        });
    }

    [Test]
    public async Task A_failed_segment_delete_throws_and_a_retry_finishes_the_clear()
    {
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        var generation = store.State.State.SegmentGeneration;
        store.Segment(generation, 1).FailClears = 1;

        Assert.That(
            async () => await store.Grain.ClearAsync(CancellationToken.None),
            Throws.InvalidOperationException,
            "a clear that left rows behind must say so, or the caller records it as done");

        Assert.Multiple(() =>
        {
            Assert.That(store.State.RecordExists, Is.True, "the manifest must survive to name what is left");
            Assert.That(store.StoredSegmentKeys(), Is.Not.Empty, "precondition: the fault must have left rows");
        });

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.StoredSegmentKeys(), Is.Empty);
            Assert.That(store.State.RecordExists, Is.False);
        });
    }

    [Test]
    public async Task A_retry_from_a_fresh_activation_finishes_the_clear()
    {
        // The retry may land on a new activation that knows only what the
        // tombstone recorded. Model it by handing the persisted state to a new
        // grain over the same segment store.
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        store.Segment(store.State.State.SegmentGeneration, 0).FailClears = 1;
        Assert.That(async () => await store.Grain.ClearAsync(CancellationToken.None), Throws.InvalidOperationException);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILeafSnapshotSegmentGrain>(Arg.Any<string>())
            .Returns(call => store.Segments.TryGetValue(call.ArgAt<string>(0), out var s) ? s : new CleanupSegment());
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf-snapshot", store.LeafKey));
        var reactivated = new LeafSnapshotStorageGrain(context, store.State, factory);

        await reactivated.ClearAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.StoredSegmentKeys(), Is.Empty);
            Assert.That(store.State.RecordExists, Is.False);
        });
    }

    [Test]
    public async Task ClearAsync_deletes_the_frames_of_a_staged_capture_that_never_committed()
    {
        var store = CreateCleanupStore();
        await store.Grain.BeginStagedSnapshotAsync(CancellationToken.None);
        var frame = LeafSnapshotCodec.Encode(
            [new LeafSnapshotRow("k", LwwValue<byte[]>.Create([1], new HybridLogicalClock { WallClockTicks = 5 }))]);
        await store.Grain.StageSnapshotSegmentAsync(frame, 1, CancellationToken.None);
        await store.Grain.StageSnapshotSegmentAsync(frame, 1, CancellationToken.None);
        Assert.That(store.StoredSegmentKeys(), Has.Length.EqualTo(2), "precondition: two staged frames");

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.That(store.StoredSegmentKeys(), Is.Empty,
            "no manifest records staged frames, so only the probe of the generation above the live one can find them");
        Assert.That(
            async () => await store.Grain.CommitStagedSnapshotAsync(new LeafSnapshotBlob { SnapshotOffset = 1 }, CancellationToken.None),
            Throws.InvalidOperationException,
            "the clear deleted the staged frames, so a later commit must not publish a manifest over them");
    }

    [Test]
    public async Task ClearAsync_deletes_a_manifest_that_claims_no_coverage()
    {
        // A leaf holding live rows with no checkpoint captures a blob whose every
        // offset is the -1 sentinel (issue #2692), and that blob IS persisted.
        // The old clear short-circuited on "no captured prefix" and left the row.
        var store = CreateCleanupStore();
        store.State.State = new LeafSnapshotBlob
        {
            SnapshotOffsetsByPartition = [-1, -1],
            Rows = [new LeafSnapshotRow("k", LwwValue<byte[]>.Create([1], HybridLogicalClock.Zero))],
        };
        store.State.RecordExistsValue = true;

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.That(store.State.RecordExists, Is.False);
    }

    [Test]
    public async Task A_superseded_generation_whose_retirement_fails_is_recorded_and_retired_by_the_next_capture()
    {
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10, fill: 1), CancellationToken.None);
        var first = store.State.State.SegmentGeneration;
        var firstCount = store.State.State.SegmentCount;

        // The top index fails: retirement runs from the highest index down, so a
        // failure there leaves the whole run - still a contiguous prefix.
        store.Segment(first, firstCount - 1).FailClears = 1;
        await store.Grain.SaveAsync(LargeBlob(20, fill: 2), CancellationToken.None);

        Assert.That(
            store.State.State.PendingSegmentRetirements,
            Is.EqualTo(new[] { new LeafSnapshotSegmentRange(first, 0, firstCount) }),
            "the superseded run must stay recorded as owed while its rows remain");
        var leftover = store.StoredSegmentKeys().Where(k => k.Contains($"/g{first}/", StringComparison.Ordinal)).ToArray();
        Assert.That(leftover, Has.Length.EqualTo(firstCount), "precondition: the failure left the run in place");

        await store.Grain.SaveAsync(LargeBlob(30, fill: 3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                store.StoredSegmentKeys().Where(k => k.Contains($"/g{first}/", StringComparison.Ordinal)),
                Is.Empty,
                "the next capture must finish the owed retirement");
            Assert.That(store.State.State.PendingSegmentRetirements, Is.Null);
        });
    }

    [Test]
    public async Task A_retirement_that_fails_part_way_leaves_a_contiguous_prefix_and_records_only_it()
    {
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        var first = store.State.State.SegmentGeneration;
        var firstCount = store.State.State.SegmentCount;
        Assert.That(firstCount, Is.GreaterThanOrEqualTo(3), "precondition: enough segments to fail in the middle");

        store.Segment(first, 1).FailClears = 1;
        await store.Grain.SaveAsync(LargeBlob(20, fill: 2), CancellationToken.None);

        var left = store.StoredSegmentKeys().Where(k => k.Contains($"/g{first}/", StringComparison.Ordinal)).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EquivalentTo(new[] { $"{store.LeafKey}/g{first}/0", $"{store.LeafKey}/g{first}/1" }));
            Assert.That(
                store.State.State.PendingSegmentRetirements,
                Is.EqualTo(new[] { new LeafSnapshotSegmentRange(first, 0, 2) }));
        });
    }

    [Test]
    public async Task A_staged_run_overtaken_by_a_segmented_save_is_declined_and_the_saves_segments_survive()
    {
        // A staged run targets the generation above the live one. A segmented
        // SaveAsync landing while the run is open commits into that same
        // generation and overwrites the run's leading frames, so the run can no
        // longer be committed: its manifest would describe a mixture of two
        // snapshots. Before the fix it committed anyway and then retired the
        // superseded generation - its own - deleting segments its manifest named.
        var store = CreateCleanupStore();
        var frame = LeafSnapshotCodec.Encode(
            [new LeafSnapshotRow("a", LwwValue<byte[]>.Create([7], new HybridLogicalClock { WallClockTicks = 50 }))]);

        await store.Grain.BeginStagedSnapshotAsync(CancellationToken.None);
        await store.Grain.StageSnapshotSegmentAsync(frame, 1, CancellationToken.None);
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        var saved = (store.State.State.SegmentGeneration, store.State.State.SegmentCount);
        Assert.That(saved.SegmentCount, Is.GreaterThan(2), "precondition: the save committed several segments");
        var savedFrames = new List<byte[]?>();
        for (var i = 0; i < saved.SegmentCount; i++)
        {
            savedFrames.Add(await store.Grain.LoadSegmentFrameAsync(i, CancellationToken.None));
        }

        var committed = await store.Grain.CommitStagedSnapshotAsync(
            new LeafSnapshotBlob { SnapshotOffset = 11, SnapshotOffsetsByPartition = [11] },
            CancellationToken.None);

        Assert.That(committed, Is.False, "an overtaken run must be declined, not published over the save's frames");
        Assert.That((store.State.State.SegmentGeneration, store.State.State.SegmentCount), Is.EqualTo(saved),
            "the save's manifest stays authoritative");
        for (var i = 0; i < saved.SegmentCount; i++)
        {
            Assert.That(await store.Grain.LoadSegmentFrameAsync(i, CancellationToken.None), Is.EqualTo(savedFrames[i]),
                $"segment {i} must still hold the save's frame, neither deleted nor overwritten");
        }
    }

    [Test]
    public async Task Staging_into_a_run_a_segmented_save_has_overtaken_is_refused_before_it_overwrites_a_live_segment()
    {
        var store = CreateCleanupStore();
        var frame = LeafSnapshotCodec.Encode(
            [new LeafSnapshotRow("a", LwwValue<byte[]>.Create([7], new HybridLogicalClock { WallClockTicks = 50 }))]);

        await store.Grain.BeginStagedSnapshotAsync(CancellationToken.None);
        await store.Grain.StageSnapshotSegmentAsync(frame, 1, CancellationToken.None);
        await store.Grain.SaveAsync(LargeBlob(10), CancellationToken.None);
        var liveSegmentOne = await store.Grain.LoadSegmentFrameAsync(1, CancellationToken.None);
        Assert.That(liveSegmentOne, Is.Not.Null, "precondition: the save committed a segment 1");

        Assert.That(
            async () => await store.Grain.StageSnapshotSegmentAsync(frame, 1, CancellationToken.None),
            Throws.InvalidOperationException,
            "the run's next index is segment 1 of the live snapshot; staging there would overwrite it");
        Assert.That(await store.Grain.LoadSegmentFrameAsync(1, CancellationToken.None), Is.EqualTo(liveSegmentOne));
    }

    [Test]
    public async Task A_clear_whose_probe_delete_fails_part_way_finds_every_remaining_frame_on_retry()
    {
        // Discovery is read-only and deletion runs top-down, so a failure leaves a
        // run from index 0 that the retry's discovery finds again. A probe that
        // deleted as it ascended would, on retry, stop at index 0 - already gone -
        // and report the generation clean with the tail still stored.
        var store = CreateCleanupStore();
        var frame = LeafSnapshotCodec.Encode(
            [new LeafSnapshotRow("k", LwwValue<byte[]>.Create([1], new HybridLogicalClock { WallClockTicks = 5 }))]);
        await store.Grain.BeginStagedSnapshotAsync(CancellationToken.None);
        for (var i = 0; i < 4; i++)
        {
            await store.Grain.StageSnapshotSegmentAsync(frame, 1, CancellationToken.None);
        }

        store.Segment(1, 2).FailClears = 1;
        Assert.That(async () => await store.Grain.ClearAsync(CancellationToken.None), Throws.InvalidOperationException);
        Assert.That(store.StoredSegmentKeys(), Is.EquivalentTo(new[] { $"{store.LeafKey}/g1/0", $"{store.LeafKey}/g1/1", $"{store.LeafKey}/g1/2" }),
            "precondition: the failure left a contiguous run from index 0");

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.That(store.StoredSegmentKeys(), Is.Empty);
    }

    [Test]
    public async Task Owed_retirements_are_never_dropped_however_many_accumulate()
    {
        // Dropping a record strands rows no probe can find, because a clear probes
        // only the live generation and the one above it.
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(1, fill: 1), CancellationToken.None);
        var firstGeneration = store.State.State.SegmentGeneration;
        const int captures = 80;
        for (var c = 0; c < captures; c++)
        {
            var superseded = store.State.State.SegmentGeneration;
            store.Segment(superseded, store.State.State.SegmentCount - 1).FailClears = int.MaxValue;
            await store.Grain.SaveAsync(LargeBlob(2 + c, fill: (byte)(2 + c)), CancellationToken.None);
        }

        Assert.That(store.State.State.PendingSegmentRetirements, Has.Length.EqualTo(captures),
            "every superseded generation whose retirement failed stays recorded");
        Assert.That(store.State.State.PendingSegmentRetirements![0].Generation, Is.EqualTo(firstGeneration));

        foreach (var segment in store.Segments.Values)
        {
            segment.FailClears = 0;
        }

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.That(store.StoredSegmentKeys(), Is.Empty, "the clear finishes every recorded retirement, oldest included");
    }

    [Test]
    public async Task ClearAsync_finishes_a_retirement_an_earlier_capture_left_owed()
    {
        var store = CreateCleanupStore();
        await store.Grain.SaveAsync(LargeBlob(10, fill: 1), CancellationToken.None);
        var first = store.State.State.SegmentGeneration;
        store.Segment(first, store.State.State.SegmentCount - 1).FailClears = 1;
        await store.Grain.SaveAsync(LargeBlob(20, fill: 2), CancellationToken.None);
        Assert.That(store.State.State.PendingSegmentRetirements, Is.Not.Null, "precondition: a retirement is owed");

        await store.Grain.ClearAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(store.StoredSegmentKeys(), Is.Empty);
            Assert.That(store.State.RecordExists, Is.False);
        });
    }
}
