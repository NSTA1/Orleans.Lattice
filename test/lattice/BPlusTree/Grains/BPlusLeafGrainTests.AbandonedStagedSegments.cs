using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for a staged capture that fails part-way and is then
/// retried against a storage grain whose activation survived the failure.
/// <para>
/// The staging cursor (<c>stagingGeneration</c> / <c>stagedSegmentCount</c>)
/// lives on the storage grain's activation and was cleared in exactly one
/// place: a successful <c>CommitStagedSnapshotAsync</c>. Nothing cleared it on
/// a failure path, and <c>RetireSegmentsAsync</c> was never called on one. The
/// grain contract asserted that this was harmless, on the grounds that a
/// capture abandoned part-way "by a fault, a cancellation, or a lost
/// activation" leaves only unreferenced frames, which are inert. That is true
/// of the third case and false of the first two: only a lost activation takes
/// the cursor with it. A capture that merely faults - the storage timeout that
/// <c>orleans.lattice.leaf.snapshot.captures{outcome="failed"}</c> counts -
/// leaves the grain activated with its cursor mid-run.
/// </para>
/// <para>
/// The next capture then resumed at that cursor instead of restarting at zero,
/// so it staged its own segments ABOVE the abandoned ones and committed a
/// manifest spanning both. Those leading frames are not inert: the manifest
/// references them, hydration loads them, and the fold applies segments in
/// index order with later-wins-per-key. A key present in the abandoned prefix
/// and absent from the live suffix therefore has nothing to overwrite it and
/// comes back from the dead - while the manifest's coverage stamp
/// simultaneously authorises the WAL GC to trim the prefix that recorded its
/// removal. That is a no-loss violation of the kind #1535 exists to prevent,
/// reached without weakening #1535 itself.
/// </para>
/// <para>
/// WHY THERE ARE TWO TESTS. The first drives the storage-grain contract
/// directly and proves the DATA consequence: what a committed manifest
/// contains after an abandoned run. On its own it would be a false green of the
/// worst kind, because it calls <c>BeginStagedSnapshotAsync</c> itself and so
/// proves only that the member works when invoked. The second drives a real
/// leaf capture end to end and proves the CALLER invokes it - which is the half
/// that makes the fix load-bearing rather than merely present.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Wraps a real <see cref="ILeafSnapshotStorageGrain"/> and faults the
    /// <paramref name="failOnStageCall"/>th staged segment, reproducing the
    /// shape measured in the field: a storage-side fault part-way through a
    /// multi-segment capture, leaving the storage grain activated.
    /// <para>
    /// It also counts <see cref="BeginStagedSnapshotAsync"/> calls, because
    /// "did the leaf open a fresh run" is the property the end-to-end test
    /// exists to establish and it is not otherwise observable from outside.
    /// </para>
    /// </summary>
    private sealed class FaultInjectingSnapshotStore(ILeafSnapshotStorageGrain inner, int failOnStageCall)
        : ILeafSnapshotStorageGrain
    {
        private int stageCalls;

        internal int BeginCalls { get; private set; }

        /// <summary>
        /// Index returned by each successful staged segment, in order. This is
        /// the discriminator the end-to-end test turns on, and it is chosen
        /// because it needs no knowledge of where one capture ends and the next
        /// begins: a capture that opens a fresh run is handed index zero, and a
        /// capture that inherits an abandoned one is handed the cursor it left.
        /// Counting segments per capture would instead require observing the
        /// capture boundary - which, before the fix, there was no way to do.
        /// </summary>
        internal List<int> StageIndices { get; } = [];

        internal int InlineSaves { get; private set; }

        /// <summary>
        /// Faults exactly one staged segment. The failure has to be transient
        /// for this fixture to say anything: a permanently failing store never
        /// commits, and the corruption under test only becomes durable when a
        /// LATER capture succeeds and publishes a manifest over the wreckage.
        /// </summary>
        public Task BeginStagedSnapshotAsync(CancellationToken cancellationToken)
        {
            BeginCalls++;
            return inner.BeginStagedSnapshotAsync(cancellationToken);
        }

        public async Task<int> StageSnapshotSegmentAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
        {
            stageCalls++;
            if (stageCalls == failOnStageCall)
            {
                throw new TimeoutException("injected storage timeout part-way through a staged capture");
            }

            var index = await inner.StageSnapshotSegmentAsync(frame, rowCount, cancellationToken);
            StageIndices.Add(index);
            return index;
        }

        public Task<bool> CommitStagedSnapshotAsync(LeafSnapshotBlob manifest, CancellationToken cancellationToken)
            => inner.CommitStagedSnapshotAsync(manifest, cancellationToken);

        public Task<LeafSnapshotSaveOutcome> SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken)
        {
            InlineSaves++;
            return inner.SaveAsync(blob, cancellationToken);
        }

        public Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken)
            => inner.LoadAsync(cancellationToken);

        public Task<byte[]?> LoadSegmentFrameAsync(int index, CancellationToken cancellationToken)
            => inner.LoadSegmentFrameAsync(index, cancellationToken);

        public Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken)
            => inner.GetSnapshotByteSizeAsync(cancellationToken);

        public Task ClearAsync(CancellationToken cancellationToken)
            => inner.ClearAsync(cancellationToken);
    }

    private static LeafSnapshotRow AbandonedRunRow(string key, byte fill, long wallClockTicks)
    {
        var payload = new byte[32];
        Array.Fill(payload, fill);

        return new LeafSnapshotRow(
            key,
            LwwValue<byte[]>.Create(payload, new HybridLogicalClock { WallClockTicks = wallClockTicks }),
            null);
    }

    private static LeafSnapshotBlob AbandonedRunManifest(int partitions)
    {
        var coverage = new long[partitions];
        Array.Fill(coverage, 10L);

        return new LeafSnapshotBlob
        {
            SnapshotOffset = 10L,
            SnapshotOffsetsByPartition = coverage,
        };
    }

    [Test]
    public async Task A_capture_that_follows_an_abandoned_staged_run_must_not_inherit_its_segments()
    {
        // THE DATA CONSEQUENCE, at the storage-grain contract.
        //
        // Capture 1 stages one frame holding "abandoned-key" and never commits,
        // which is what a storage fault part-way through a multi-segment
        // capture leaves behind. Capture 2 then stages one frame holding
        // "live-key" and commits.
        //
        // The committed manifest must describe capture 2 and capture 2 alone.
        // Before the fix it described both, because the staging cursor was
        // still at index 1 and capture 2 stacked on top of it.
        const int partitions = 4;

        var (store, _, manifestState) = CreateSegmentingStore();

        var abandonedFrame = LeafSnapshotCodec.Encode([AbandonedRunRow("abandoned-key", 0x11, 1_000L)]);
        await store.BeginStagedSnapshotAsync(default);
        var abandonedIndex = await store.StageSnapshotSegmentAsync(abandonedFrame, 1, default);

        Assert.That(abandonedIndex, Is.Zero,
            "precondition: the abandoned run really did open at index zero, so any index above zero in "
            + "the next run can only have come from inheriting this one");

        // No commit. This is the abandoned capture.

        var liveFrame = LeafSnapshotCodec.Encode([AbandonedRunRow("live-key", 0x22, 2_000L)]);
        await store.BeginStagedSnapshotAsync(default);
        var liveIndex = await store.StageSnapshotSegmentAsync(liveFrame, 1, default);

        Assert.That(liveIndex, Is.Zero,
            "THE ASSERTION. A capture opening a fresh staged run must be handed index zero. Before the "
            + "fix it was handed index 1, because the cursor left behind by the abandoned run was only "
            + "ever reset by a successful commit - so this capture appended to a snapshot it did not "
            + "produce and could not see");

        var committed = await store.CommitStagedSnapshotAsync(AbandonedRunManifest(partitions), default);
        Assert.That(committed, Is.True, "precondition: the second capture's manifest must actually commit");

        Assert.That(manifestState.State.SegmentCount, Is.EqualTo(1),
            "the committed manifest must reference exactly the one segment the live capture staged. A "
            + "SegmentCount of 2 means it also references the abandoned capture's frame, which is stale "
            + "rows the live capture never agreed to publish");

        // And the rows themselves, because SegmentCount alone would not show
        // WHICH segment survived. Hydration folds segments in index order with
        // later-wins-per-key, so an inherited prefix does not merely bloat the
        // manifest - it resurrects any key the live capture no longer holds.
        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, store);
        leafState.State.TreeId = SegmentedTreeId;
        leafState.State.ProjectionCheckpointOffset = -1L;

        var rehydrated = await leaf.TryRehydrateFromSnapshotAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(rehydrated, Is.True, "the committed snapshot must rehydrate");
            Assert.That(leaf.EntriesForTest.ContainsKey("live-key"), Is.True,
                "the live capture's own row must survive, otherwise this fixture is asserting against an "
                + "empty snapshot and would pass for the wrong reason");
            Assert.That(leaf.EntriesForTest.ContainsKey("abandoned-key"), Is.False,
                "THE NO-LOSS ASSERTION. A row staged by a capture that never committed must not appear in "
                + "a later capture's snapshot. It is a row no live capture published, restored beneath a "
                + "coverage stamp that authorises the WAL GC to trim the prefix recording its removal");
            Assert.That(leaf.EntriesForTest, Has.Count.EqualTo(1),
                "and nothing else may have come back either");
        });
    }

    [Test]
    public async Task A_leaf_retrying_a_failed_capture_opens_a_fresh_staged_run()
    {
        // THE CALLER. The test above proves BeginStagedSnapshotAsync discards an
        // abandoned run when it is called; it cannot prove anything calls it.
        // This one drives a real leaf capture through the real capture path and
        // faults it part-way, exactly as the storage timeout measured in the
        // field does.
        //
        // The leaf SWALLOWS that fault - capture is advisory, so a failed one
        // logs and moves on - and the next capture in the same fill-and-capture
        // sequence succeeds and commits. That is precisely the production
        // shape, and it is why the corruption is durable: the retry publishes a
        // manifest over the abandoned run's frames.
        const int partitions = 1;
        const int rowCount = 256;
        const int valueBytes = 2048;

        var (store, _, manifestState) = CreateSegmentingStore();
        var faulting = new FaultInjectingSnapshotStore(store, failOnStageCall: 2);
        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(
            partitions,
            faulting,
            leafSnapshotSegmentBytes: SegmentTestWindowBytes);

        // Precondition: the snapshot must be several windows wide, or capture
        // takes the inline branch, never stages a segment, and this fixture
        // exercises nothing.
        var snapshotBytes = (long)rowCount * valueBytes;
        Assert.That(snapshotBytes, Is.GreaterThan(SegmentTestWindowBytes * 4),
            "precondition: the snapshot must be several windows wide so capture takes the staged path");

        await FillAndCaptureAsync(leaf, leafState, rowCount, valueBytes);

        Assert.That(faulting.StageIndices, Is.Not.Empty,
            "precondition: capture must have taken the staged path at all");
        Assert.That(faulting.StageIndices[0], Is.Zero,
            "precondition: the first capture opened its run at index zero, so any later index can only "
            + "have been inherited");
        Assert.That(manifestState.State.SegmentCount, Is.GreaterThan(1),
            "precondition: a capture did commit a segmented manifest, otherwise there is nothing to "
            + "assert about what that manifest spans");

        // The fault fired on stage call 2, so exactly one segment landed before
        // the capture was abandoned. Everything from here is the retry.
        var retryIndices = faulting.StageIndices.Skip(1).ToArray();

        Assert.That(retryIndices, Is.Not.Empty,
            "precondition: a capture must have run after the faulted one, otherwise the inherited-cursor "
            + "defect is never reached");

        Assert.Multiple(() =>
        {
            Assert.That(faulting.BeginCalls, Is.GreaterThanOrEqualTo(2),
                "THE CALLER ASSERTION. The leaf must open a staged run before staging into one, on every "
                + "capture. If it does not, the reset is present on the grain and unreachable from "
                + "production, which is indistinguishable from not having fixed it at all");

            Assert.That(retryIndices[0], Is.Zero,
                "THE ASSERTION. The capture that follows a faulted one must be handed index zero. Before "
                + "the fix it was handed index 1: the staging cursor was reset only by a successful "
                + "commit, so the retry appended its segments above a frame from the capture that failed");

            Assert.That(manifestState.State.SegmentCount, Is.EqualTo(retryIndices.Length),
                "and the committed manifest must span exactly the segments the successful capture staged. "
                + "A SegmentCount above that means the manifest also references the abandoned capture's "
                + "frames - stale rows published under a coverage stamp that authorises the WAL GC to "
                + "trim the prefix recording their removal");

            Assert.That(faulting.InlineSaves, Is.Zero,
                "and the retry must not have fallen back to an inline save, which would bypass staging "
                + "entirely and make this fixture prove nothing about it");
        });
    }
}
