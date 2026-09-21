using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2914: a leaf snapshot large enough to matter
/// was persisted as a single BLOB column, so hydrating it required one
/// contiguous allocation the size of the whole snapshot.
/// <para>
/// The distinction this fixture exists to hold is between <em>total</em> bytes
/// and <em>contiguous</em> bytes. The contiguity gate of issue #2844 predicts
/// the failure correctly and refuses the load, which is why a
/// <c>LeafSnapshotUnaffordableException</c> is now raised instead of an
/// <c>OutOfMemoryException</c> - but a refused load is still a leaf that does
/// not activate, and a leaf that does not activate never advances its
/// per-partition checkpoint and holds its block pin indefinitely. Predicting
/// the failure is not the same as removing it. This change removes it, by
/// making the largest array a hydration ever demands a bounded segment window
/// rather than a function of snapshot size.
/// </para>
/// <para>
/// THE ASSERTION THAT MAKES THIS REAL is
/// <see cref="Segmented_hydration_never_materialises_a_frame_larger_than_the_window"/>.
/// Every other test here would also pass against a "fix" that read the
/// segments back in bounded pieces and then reassembled them into one
/// contiguous frame before decoding - which would move the peak allocation out
/// of the storage provider and into our own code while leaving its SIZE
/// exactly as it was. That is the shape a plausible-looking but useless fix
/// takes, so the peak is measured rather than inferred from the absence of an
/// exception.
/// </para>
/// <para>
/// Why the split has to be across grains, and not inside the object graph: the
/// allocation that fails is raised inside
/// <c>Microsoft.Data.Sqlite.SqliteValueReader.GetValue(ordinal)</c>, i.e. the
/// provider materialises the whole BLOB COLUMN as one array before any lattice
/// code runs. Restructuring <c>LeafSnapshotBlob</c> - a jagged array, a list of
/// chunks, anything - changes what is deserialized FROM that array and cannot
/// change that the array was allocated. Only splitting the column across rows
/// helps, and in Orleans a grain-state row is keyed by grain identity, so
/// segments have to be grains.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string SegmentedTreeId = "tree-segmented-snapshot";

    /// <summary>
    /// Deliberately the minimum the options clamp permits. A small window keeps
    /// the fixture fast while exercising exactly the same code path a 4 MiB
    /// production window does: the branch is chosen by comparing a frame
    /// against the window, and that comparison has no knowledge of the scale
    /// either side of it.
    /// </summary>
    private const long SegmentTestWindowBytes = LatticeOptions.MinimumLeafSnapshotSegmentBytes;

    /// <summary>
    /// In-memory stand-in for one segment's grain-state row. It stores and
    /// returns a frame and nothing else, which is the real grain's contract -
    /// but note it is NOT a substitute for the real
    /// <see cref="LeafSnapshotStorageGrain"/>: every test below drives the real
    /// storage grain, because the manifest gate, the merge decline, and the
    /// commit ordering are the parts most able to be wrong.
    /// </summary>
    private sealed class FakeSegmentGrain : ILeafSnapshotSegmentGrain
    {
        internal byte[]? Frame { get; private set; }

        internal int SaveCount { get; private set; }

        public Task SaveAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
        {
            _ = rowCount;
            Frame = frame;
            SaveCount++;
            return Task.CompletedTask;
        }

        public Task<byte[]?> LoadFrameAsync(CancellationToken cancellationToken) => Task.FromResult(Frame);

        public Task ClearAsync(CancellationToken cancellationToken)
        {
            Frame = null;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Builds a real <see cref="LeafSnapshotStorageGrain"/> wired to an
    /// in-memory segment store, and returns the segment map so a test can
    /// inspect or corrupt individual segments.
    /// </summary>
    private static (LeafSnapshotStorageGrain Store,
                    Dictionary<string, FakeSegmentGrain> Segments,
                    FakePersistentState<LeafSnapshotBlob> State)
        CreateSegmentingStore(long windowBytes = SegmentTestWindowBytes)
    {
        var segments = new Dictionary<string, FakeSegmentGrain>(StringComparer.Ordinal);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILeafSnapshotSegmentGrain>(Arg.Any<string>())
            .Returns(call =>
            {
                var key = call.ArgAt<string>(0);
                if (!segments.TryGetValue(key, out var grain))
                {
                    grain = new FakeSegmentGrain();
                    segments[key] = grain;
                }

                return grain;
            });

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(new LatticeOptions { LeafSnapshotSegmentBytes = windowBytes });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(context, state, factory, monitor);

        return (store, segments, state);
    }

    /// <summary>
    /// Builds <paramref name="rowCount"/> rows of <paramref name="valueBytes"/>
    /// each, in ascending key order (which the frame's index table requires).
    /// Every third row carries an explicit merge mode so the round-trip test
    /// can prove the per-key discriminator survives segmentation rather than
    /// only proving the values do.
    /// </summary>
    private static LeafSnapshotRow[] BuildSegmentTestRows(int rowCount, int valueBytes)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var payload = new byte[valueBytes];
            // A per-row fill so a mis-ordered or duplicated fold is detectable
            // by value and not only by count.
            Array.Fill(payload, (byte)(i % 251));

            rows[i] = new LeafSnapshotRow(
                $"seg-key-{i:D6}",
                LwwValue<byte[]>.Create(payload, new HybridLogicalClock { WallClockTicks = 1_000L + i }),
                (i % 3) == 0 ? LatticeMergeMode.OrSet : null);
        }

        return rows;
    }

    private static LeafSnapshotBlob BuildSegmentTestBlob(LeafSnapshotRow[] rows, int partitions)
    {
        var coverage = new long[partitions];
        Array.Fill(coverage, 10L);

        return new LeafSnapshotBlob
        {
            SnapshotOffset = 10L,
            SnapshotOffsetsByPartition = coverage,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
        };
    }

    [Test]
    public async Task Large_snapshot_is_persisted_as_bounded_segments()
    {
        // 512 rows x 1 KiB is roughly 8x the 64 KiB test window, so the
        // planner must produce several segments. The row count and total size
        // are asserted as preconditions rather than assumed: a fixture whose
        // input silently shrank below the window would take the inline branch
        // and report a clean pass for having tested nothing.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 512, valueBytes: 1024);
        var blob = BuildSegmentTestBlob(rows, partitions);

        var inlineFrameBytes = blob.EncodedRows!.LongLength;
        Assert.That(inlineFrameBytes, Is.GreaterThan(SegmentTestWindowBytes * 4),
            "precondition: the unsegmented frame must be several windows wide, otherwise the inline "
            + "branch is taken and this test proves nothing about segmentation");

        var (store, segments, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        var manifest = state.State;
        var writtenSegments = segments.Values.Where(s => s.Frame is not null).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(manifest.SegmentCount, Is.GreaterThan(1),
                "a snapshot several windows wide must be split; a SegmentCount of 0 means it was "
                + "persisted inline and the contiguous column read that fails in issue #2844 is intact");
            Assert.That(manifest.EncodedRows, Is.Null,
                "the manifest row must carry NO inline payload. If it did, the manifest column would "
                + "still be the size of the whole snapshot and splitting it would have bought nothing");
            Assert.That(writtenSegments, Has.Length.EqualTo(manifest.SegmentCount),
                "every segment the manifest claims must have actually landed. The manifest is the commit "
                + "point precisely so that it cannot reference a segment that was never written");

            foreach (var segment in writtenSegments)
            {
                Assert.That(segment.Frame!.LongLength, Is.LessThanOrEqualTo(SegmentTestWindowBytes),
                    "no persisted segment may exceed the window. The window is a bound on the largest "
                    + "array a later hydration will ask the storage provider for, so a segment above it "
                    + "reintroduces exactly the allocation this change removes");
            }
        });

        Assert.That(writtenSegments.Sum(s => s.Frame!.LongLength),
            Is.GreaterThanOrEqualTo(inlineFrameBytes - (manifest.SegmentCount * 256)),
            "and the segments together must still carry the whole snapshot: total bytes are not reduced "
            + "by this change and must not be, only the largest single allocation is");
    }

    [Test]
    public async Task Segmented_snapshot_rehydrates_every_row_and_preserves_merge_modes()
    {
        // Round-trip fidelity. Segmentation is only admissible if a segmented
        // snapshot is indistinguishable from an inline one to every reader,
        // because the coverage this manifest claims authorises the WAL GC to
        // trim the prefix these rows live in. A segmented snapshot that comes
        // back with fewer rows than it claims is not a degraded snapshot, it is
        // silent data loss.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 384, valueBytes: 1024);
        var blob = BuildSegmentTestBlob(rows, partitions);

        var (store, _, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        Assert.That(state.State.SegmentCount, Is.GreaterThan(1),
            "precondition: the snapshot actually segmented, so the fold under test is the segmented one");

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, store);
        leafState.State.TreeId = SegmentedTreeId;
        leafState.State.ProjectionCheckpointOffset = -1L;

        var rehydrated = await leaf.TryRehydrateFromSnapshotAsync(default);

        Assert.That(rehydrated, Is.True, "a complete segmented snapshot must rehydrate");
        Assert.That(leaf.EntriesForTest, Has.Count.EqualTo(rows.Length),
            $"every one of the {rows.Length} rows must be folded into the cache. A short count here is "
            + "the silent-data-loss shape: the manifest's coverage claim would authorise trimming WAL "
            + "the missing rows are the only other copy of");

        var byKey = leaf.EntriesForTest.ToDictionary(e => e.Key, e => e.Value, StringComparer.Ordinal);
        Assert.Multiple(() =>
        {
            foreach (var expected in rows)
            {
                Assert.That(byKey.ContainsKey(expected.Key), Is.True, $"row {expected.Key} survived");
                Assert.That(byKey[expected.Key].Value, Is.EqualTo(expected.Value.Value),
                    $"row {expected.Key} kept its exact payload, so a mis-ordered or overlapping "
                    + "segment boundary is detectable by value and not only by count");
            }
        });

        foreach (var expected in rows.Where(r => r.MergeMode is not null))
        {
            Assert.That(leaf.CacheForTest.GetMergeMode(expected.Key), Is.EqualTo(expected.MergeMode),
                $"the durable per-key merge-mode discriminator on {expected.Key} survived segmentation. "
                + "Losing it would make a freeze after a rehydrate-from-checkpoint mode-unfaithful, "
                + "which is a correctness bug the row count alone would not reveal");
        }
    }

    [Test]
    public async Task Segmented_hydration_never_materialises_a_frame_larger_than_the_window()
    {
        // THE ANTI-FAKE-FIX ASSERTION. Everything else in this file passes
        // against a fix that reads bounded segments and then concatenates them
        // into one contiguous frame before decoding - a change that moves the
        // peak allocation from the storage provider into our own code and
        // leaves its size exactly as it was. So the peak is measured.
        //
        // It is measured at the seam that matters: the frames handed back by
        // LoadSegmentFrameAsync are the arrays a real provider would have had
        // to materialise contiguously, so the largest of them IS the peak
        // contiguous demand of the hydration.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 768, valueBytes: 1024);
        var blob = BuildSegmentTestBlob(rows, partitions);
        var unsegmentedBytes = blob.EncodedRows!.LongLength;

        var (store, _, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        var segmentCount = state.State.SegmentCount;
        Assert.That(segmentCount, Is.GreaterThan(1),
            "precondition: segmentation happened, so a peak below the whole-snapshot size is evidence "
            + "about segmentation rather than about a snapshot that was small all along");

        long peak = 0;
        long readBytes = 0;
        var reads = 0;
        for (var i = 0; i < segmentCount; i++)
        {
            var frame = await store.LoadSegmentFrameAsync(i, default);
            Assert.That(frame, Is.Not.Null, $"segment {i} reads back");
            reads++;
            readBytes += frame!.LongLength;
            peak = Math.Max(peak, frame.LongLength);
        }

        Assert.That(reads, Is.EqualTo(segmentCount),
            "input count asserted non-zero and complete: a peak computed over zero reads would be zero "
            + "and would pass the bound below while measuring nothing at all");

        Assert.Multiple(() =>
        {
            Assert.That(peak, Is.LessThanOrEqualTo(SegmentTestWindowBytes),
                "the largest single contiguous array the hydration demands must stay inside the window. "
                + "This is the whole claim of issue #2914, and it is the one an otherwise-plausible "
                + "reassembling implementation fails");
            Assert.That(peak, Is.LessThan(unsegmentedBytes / 4),
                "and it must be a small fraction of the unsegmented frame, so the bound is demonstrably "
                + "independent of snapshot size rather than merely satisfied at this size");
            Assert.That(readBytes, Is.GreaterThan(unsegmentedBytes / 2),
                "while the TOTAL bytes read stay comparable to the unsegmented frame - proving the peak "
                + "fell because the payload was split, and not because payload went missing");
        });
    }

    [Test]
    public async Task Snapshot_within_the_window_stays_inline()
    {
        // Ordinary leaves must be untouched. Segmentation costs a grain call
        // per segment and forfeits the lazy attach of issue #1839, so paying it
        // on a snapshot that never had a contiguity problem would be a straight
        // regression for the overwhelming majority of leaves.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 8, valueBytes: 128);
        var blob = BuildSegmentTestBlob(rows, partitions);

        Assert.That(blob.EncodedRows!.LongLength, Is.LessThan(SegmentTestWindowBytes),
            "precondition: this frame fits the window, so the inline branch is the one under test");

        var (store, segments, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SegmentCount, Is.Zero,
                "a snapshot inside the window is not segmented");
            Assert.That(state.State.IsSegmented(), Is.False,
                "and reports itself unsegmented, so every reader takes the pre-existing inline path");
            Assert.That(state.State.EncodedRows, Is.Not.Null,
                "the payload stays in the manifest row exactly as before");
            Assert.That(segments.Values.Count(s => s.Frame is not null), Is.Zero,
                "and no segment row is written, so the storage cost of an ordinary leaf is unchanged");
        });
    }

    [Test]
    public async Task Legacy_unsegmented_blob_still_rehydrates_inline()
    {
        // Back-compat, asserted rather than assumed. A blob written by a build
        // that predates segmentation decodes with SegmentCount defaulting to 0,
        // so it must take the inline path. Getting this wrong would strand
        // every snapshot in an upgraded deployment.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 12, valueBytes: 64);

        var coverage = new long[partitions];
        Array.Fill(coverage, 10L);

        // Persisted directly into the state, bypassing SaveAsync, so the blob
        // is exactly the shape a pre-segmentation build left behind: no
        // SegmentCount, no SegmentFrameBytes, payload inline.
        var (store, segments, state) = CreateSegmentingStore();
        state.State = new LeafSnapshotBlob
        {
            SnapshotOffset = 10L,
            SnapshotOffsetsByPartition = coverage,
            EncodedRows = LeafSnapshotCodec.Encode(rows),
        };

        Assert.That(state.State.SegmentCount, Is.Zero,
            "precondition: the legacy shape carries the default SegmentCount");

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, store);
        leafState.State.TreeId = SegmentedTreeId;
        leafState.State.ProjectionCheckpointOffset = -1L;

        var rehydrated = await leaf.TryRehydrateFromSnapshotAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.True, "a legacy blob still rehydrates");
            Assert.That(leaf.EntriesForTest, Has.Count.EqualTo(rows.Length),
                "with every row, through the unchanged inline path");
            Assert.That(segments.Values.Count(s => s.Frame is not null), Is.Zero,
                "and without consulting any segment, so the legacy path makes no new storage calls");
        });
    }

    [Test]
    public async Task Missing_segment_fails_closed_and_declines_the_rehydrate()
    {
        // Fail-closed, and specifically fail-EMPTY. A torn or partially
        // retired segmented snapshot must not hydrate the segments it CAN
        // read: a partially folded cache presents as a snapshot with silently
        // fewer rows, and the manifest's coverage claim would then authorise
        // the WAL GC to trim the prefix that is the only other copy of the
        // rows that went missing.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 384, valueBytes: 1024);
        var blob = BuildSegmentTestBlob(rows, partitions);

        var (store, segments, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        var segmentCount = state.State.SegmentCount;
        Assert.That(segmentCount, Is.GreaterThan(1),
            "precondition: more than one segment exists, so clearing the LAST one leaves earlier "
            + "segments readable and the partial-fold hazard genuinely reachable");

        // Clear the final segment: the earlier ones still read back, so a
        // fold that kept what it could would produce a plausible-looking but
        // short cache.
        var lastKey = segments.Keys.Single(k => k.EndsWith($"/{segmentCount - 1}", StringComparison.Ordinal));
        await segments[lastKey].ClearAsync(default);

        var readableBefore = 0;
        for (var i = 0; i < segmentCount - 1; i++)
        {
            var frame = await store.LoadSegmentFrameAsync(i, default);
            if (frame is { Length: > 0 })
            {
                readableBefore++;
            }
        }

        Assert.That(readableBefore, Is.EqualTo(segmentCount - 1),
            "scanned count asserted non-zero: the earlier segments really are still readable, so a "
            + "declining rehydrate below is a decision and not an artefact of everything being gone");

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, store);
        leafState.State.TreeId = SegmentedTreeId;
        leafState.State.ProjectionCheckpointOffset = -1L;

        var rehydrated = await leaf.TryRehydrateFromSnapshotAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.False,
                "an incomplete segmented snapshot must be declined outright, so activation falls through "
                + "to a full WAL replay with the -1 replay-start override");
            Assert.That(leaf.EntriesForTest, Is.Empty,
                "and the cache must be EMPTY, not partially folded. Retaining the readable segments "
                + "would be the worst outcome available: a leaf that looks hydrated, reports coverage it "
                + "cannot reproduce, and lets the WAL GC trim the rows it is missing");
        });
    }

    [Test]
    public async Task Merge_declines_rather_than_dropping_rows_of_a_segmented_snapshot()
    {
        // The monotone-merge trap. MergeMonotone's slow path enumerates the
        // EXISTING blob's rows to build a union - but a segmented manifest
        // holds no inline rows, so enumerating it yields nothing. Left
        // unguarded, a capture that regressed any partition would produce a
        // blob retaining the higher coverage backed by ZERO rows: a coverage
        // claim it cannot reproduce, which is precisely what authorises the
        // coverage-gated WAL GC to trim the last durable copy of a prefix.
        //
        // Loading every segment to merge properly would reintroduce the
        // whole-snapshot residency segmentation exists to remove, so the store
        // declines the regressing write instead and keeps the segmented
        // snapshot verbatim.
        const int partitions = 8;
        var rows = BuildSegmentTestRows(rowCount: 384, valueBytes: 1024);
        var blob = BuildSegmentTestBlob(rows, partitions);

        var (store, segments, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        var segmentCount = state.State.SegmentCount;
        var frameBytes = state.State.SegmentFrameBytes;
        Assert.That(segmentCount, Is.GreaterThan(1), "precondition: a segmented snapshot is in place");

        var landedBefore = segments.Values.Count(s => s.Frame is not null);
        Assert.That(landedBefore, Is.EqualTo(segmentCount),
            "precondition: its segments are all present, so any row loss below is caused by the merge "
            + "and not by a snapshot that was already incomplete");

        // A capture that regresses coverage on one partition. Pre-guard this
        // takes the row-merging slow path.
        var regressed = new long[partitions];
        Array.Fill(regressed, 10L);
        regressed[3] = 4L;

        await store.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = 4L,
                SnapshotOffsetsByPartition = regressed,
                EncodedRows = LeafSnapshotCodec.Encode(BuildSegmentTestRows(rowCount: 2, valueBytes: 16)),
            },
            default);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SegmentCount, Is.EqualTo(segmentCount),
                "the segmented snapshot is kept verbatim: same segment count");
            Assert.That(state.State.SegmentFrameBytes, Is.EqualTo(frameBytes),
                "and the same payload size, so the durable snapshot was not rewritten at all");
            Assert.That(state.State.EncodedRows, Is.Null,
                "and NOT replaced by a manifest carrying a handful of inline rows while still claiming "
                + "the segmented snapshot's coverage - the shape that would let the WAL GC trim a "
                + "prefix nothing else holds");
            Assert.That(segments.Values.Count(s => s.Frame is not null), Is.EqualTo(segmentCount),
                "and every segment row survives, so the snapshot remains reproducible in full");
        });

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, store);
        leafState.State.TreeId = SegmentedTreeId;
        leafState.State.ProjectionCheckpointOffset = -1L;

        Assert.That(await leaf.TryRehydrateFromSnapshotAsync(default), Is.True,
            "and it still rehydrates after the declined write");
        Assert.That(leaf.EntriesForTest, Has.Count.EqualTo(rows.Length),
            "with all of its rows, which is the claim the retained coverage has to be able to back");
    }

    [Test]
    public async Task Segmented_hydration_emits_zero_primed_segment_metrics()
    {
        // The instrumented seam, and specifically its ZERO-PRIMING. An
        // uninstrumented branch and a never-taken branch are byte-identical in
        // a scrape: both render as an absent series. That ambiguity is not
        // academic here - the whole reason this epic reads counters at all is
        // to distinguish "the leaf never hit this path" from "this build does
        // not have the path", and the two demand opposite responses. So the
        // outcome arms that did NOT occur must still be present at zero, and
        // this test asserts their presence rather than only asserting the arm
        // that fired.
        const int partitions = 8;

        // A tree id unique to this run: the priming set is static and primes a
        // tree once per process, so a shared id would make this test pass or
        // fail depending on which fixtures ran before it.
        var treeId = "tree-segment-metrics-" + Guid.NewGuid().ToString("N");

        var rows = BuildSegmentTestRows(rowCount: 384, valueBytes: 1024);
        var blob = BuildSegmentTestBlob(rows, partitions);

        var (store, _, state) = CreateSegmentingStore();
        await store.SaveAsync(blob, default);

        var segmentCount = state.State.SegmentCount;
        Assert.That(segmentCount, Is.GreaterThan(1),
            "precondition: a segmented snapshot exists, so the instrumented branch is reachable at all");

        var reads = new Dictionary<string, long>(StringComparer.Ordinal);
        var hydrations = 0L;
        long peakBytes = -1;

        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            new[]
            {
                LatticeMetrics.LeafSnapshotSegmentReadsName,
                LatticeMetrics.LeafSnapshotSegmentedHydrationsName,
                LatticeMetrics.LeafSnapshotSegmentPeakBytesName,
            },
            l => l.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                string? tree = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                    {
                        tree = tag.Value as string;
                    }
                    else if (string.Equals(tag.Key, LatticeMetrics.TagOutcome, StringComparison.Ordinal))
                    {
                        outcome = tag.Value as string;
                    }
                }

                if (!string.Equals(tree, treeId, StringComparison.Ordinal))
                {
                    return;
                }

                if (instrument.Name == LatticeMetrics.LeafSnapshotSegmentReadsName && outcome is not null)
                {
                    reads[outcome] = reads.GetValueOrDefault(outcome) + value;
                }
                else if (instrument.Name == LatticeMetrics.LeafSnapshotSegmentedHydrationsName)
                {
                    hydrations += value;
                }
                else if (instrument.Name == LatticeMetrics.LeafSnapshotSegmentPeakBytesName)
                {
                    peakBytes = Math.Max(peakBytes, value);
                }
            }));

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, store);
        leafState.State.TreeId = treeId;
        leafState.State.ProjectionCheckpointOffset = -1L;

        Assert.That(await leaf.TryRehydrateFromSnapshotAsync(default), Is.True,
            "precondition: the hydration under measurement actually succeeded, so the counts below are "
            + "measurements of a path that ran and not the silence of a path that never started");

        listener.RecordObservableInstruments();

        Assert.Multiple(() =>
        {
            Assert.That(reads.ContainsKey("loaded"), Is.True, "the loaded arm is emitted");
            Assert.That(reads.ContainsKey("missing"), Is.True,
                "AND SO IS THE MISSING ARM, at zero. Without this priming a scrape cannot tell a leaf "
                + "that read every segment cleanly from a build with no segment instrumentation at all");
            Assert.That(reads.ContainsKey("failed"), Is.True,
                "and the failed arm likewise, for the same reason");

            Assert.That(reads.GetValueOrDefault("loaded"), Is.EqualTo(segmentCount),
                "exactly one loaded read per segment the manifest claims");
            Assert.That(reads.GetValueOrDefault("missing"), Is.Zero,
                "no segment was missing on this clean hydration - a measured zero, not an absence");
            Assert.That(reads.GetValueOrDefault("failed"), Is.Zero,
                "and none threw");
            Assert.That(hydrations, Is.EqualTo(1L),
                "one segmented hydration completed");
            Assert.That(peakBytes, Is.GreaterThan(0L),
                "the high-water gauge reports a real measurement. A zero here would mean the gauge is "
                + "registered but never written, which reads in a scrape exactly like a bounded peak");
            Assert.That(peakBytes, Is.LessThanOrEqualTo(SegmentTestWindowBytes),
                "and the peak it reports is inside the window. This is the production-observable form of "
                + "the bound: an operator watching this series sees it flat at the window however large "
                + "leaf snapshots grow, and sees it track snapshot size if a future change reassembles "
                + "the payload contiguously");
        });
    }
}
