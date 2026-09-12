using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards the observability added by issue #2756: the deferred-terminal drop
/// at the <c>MaxDurableUnresolvedReplayWork</c> cap must be metered, and the
/// prepare-ledger threshold must be inclusive.
/// <para>
/// Both clauses are load-bearing in the same way and for the same reason, and
/// it is worth being explicit about why an observability change needs guarding
/// at all. The entire case for #2756 was that a zero reading and a broken
/// instrument are indistinguishable. An emission that is wrong - wrong site,
/// wrong condition, wrong tag, never reached - therefore does not merely fail
/// to help: it produces a confident zero, which is read as "the clamp is not
/// firing", which is the exact false conclusion the instrument was added to
/// prevent. An unguarded emission moves the ambiguity one level up rather than
/// removing it.
/// </para>
/// <para>
/// These arms reuse the saturation harness built for #2746 in
/// <c>BPlusLeafGrainTests.ReplayDeferredClampProgress.cs</c> and the
/// unresolved-prepare window built for #2183 in
/// <c>BPlusLeafGrainTests.ReplayFlushCeiling.cs</c>, rather than standing up a
/// third way to saturate a ledger.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Collects every measurement on <paramref name="instrument"/> with its
    /// tags flattened, for the duration of <paramref name="body"/>.
    /// <para>
    /// Taking the instrument as a parameter is deliberate and is the
    /// repository convention: reading it at the call site forces the owning
    /// type initialiser to complete BEFORE the listener exists, so the
    /// meter-field ordering hazard that silently disables an instrument during
    /// publication is not expressible here.
    /// </para>
    /// </summary>
    private static async Task<List<(long Value, Dictionary<string, object?> Tags)>> RecordMeasurementsAsync(
        Counter<long> instrument,
        Func<Task> body)
    {
        var measurements = new List<(long, Dictionary<string, object?>)>();
        var gate = new Lock();

        using var listener = MeterListening.StartForInstrument(
            instrument,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var flattened = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var tag in tags)
                    flattened[tag.Key] = tag.Value;

                lock (gate)
                    measurements.Add((value, flattened));
            }));

        await body();

        listener.RecordObservableInstruments();

        lock (gate)
            return [.. measurements];
    }

    /// <summary>
    /// Runs one uninterrupted activation whose partition 0 window carries
    /// <paramref name="deleteRanges"/> deferred range deletes against a
    /// <paramref name="cap"/>-entry durable ledger.
    /// <para>
    /// Saturation is reached WITHIN this single activation, deliberately.
    /// Pre-seeding the persisted ledger does not work: entries restored above
    /// a partition's persisted checkpoint are pruned at activation, so a
    /// seeded ledger comes back empty and nothing is ever refused. Filling it
    /// from the replay itself is also the shape the live fault takes - a wide
    /// gap containing range deletes on a partition that is not absorbed last.
    /// </para>
    /// <para>
    /// Partition 0 is given the SMALLER backlog (12 offsets against 20), so
    /// pass 1 sweeps it first and it is therefore NOT the partition absorbed
    /// last. Only the last partition drains its terminals inline; every other
    /// one defers, which is what puts these terminals in front of the cap at
    /// all.
    /// </para>
    /// </summary>
    private static async Task<Exception?> RunDeferredDropReplayAsync(int cap, int deleteRanges)
    {
        var state = NewFlushCeilingState();

        var p0 = new CommitLogSliceEntry[12];
        for (var i = 1; i <= 12; i++)
            p0[i - 1] = i <= deleteRanges ? FlushDeleteRange(i) : FlushSet(i, $"d{i:D2}");

        var p1 = new CommitLogSliceEntry[20];
        for (var i = 1; i <= 20; i++)
            p1[i - 1] = FlushSet(i, $"e{i:D2}");

        ILeafReplayCoordinatorGrain[] coordinators =
        [
            BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: null, p0),
            BuildObservableCoordinator(head: 20, sliceSize: 4, tail: 0, onRead: null, p1),
        ];

        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(
            state,
            coordinators,
            store.Stub,
            maxDurableUnresolvedReplayWork: cap);

        try
        {
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Test]
    public async Task Dropping_a_deferred_terminal_at_the_saturated_cap_emits_the_drop_counter()
    {
        // P1 for issue #2756. Partition 0 is non-last, so each of its four
        // range deletes defers instead of draining inline. The durable ledger
        // holds two, and TryRecordUnresolvedReplayWork then refuses at
        // work.Count >= cap: the remaining two fall to the in-memory clamp and
        // are dropped.
        //
        // This is the single clause the whole issue rests on. If it does not
        // emit here, a redeployed silo reads zero and concludes the clamp is
        // not firing.
        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafDeferredTerminalsDroppedAtCap,
            async () => await RunDeferredDropReplayAsync(cap: 2, deleteRanges: 4));

        var drops = measurements.Where(m => m.Value > 0).ToList();

        Assert.That(drops, Is.Not.Empty,
            "A deferred terminal refused by a saturated ledger must increment "
            + "orleans.lattice.leaf.deferred_terminals_dropped_at_cap. A silent drop here is the "
            + "whole defect issue #2756 removes: it can pin the partition's replay checkpoint and "
            + "so, through the whole-tree Zero block pin, stop WAL reclamation for every leaf in "
            + "the tree, with no signal anywhere to say so.");

        Assert.Multiple(() =>
        {
            Assert.That(drops.Select(d => d.Value), Is.All.EqualTo(1L),
                "Each dropped terminal must count exactly one.");
            Assert.That(
                drops[0].Tags[LatticeMetrics.TagTree],
                Is.EqualTo(FlushCeilingTreeId),
                "The drop must be attributed to the tree, or an operator cannot tell which tree "
                + "is pinned.");
            Assert.That(
                drops[0].Tags[LatticeMetrics.TagPartition],
                Is.EqualTo(0),
                "The drop must be attributed to the WAL partition that dropped it. Partition 0 is "
                + "the non-last partition in this harness, and per-partition attribution is what "
                + "distinguishes one pinned partition from a tree-wide fault.");
        });
    }

    [Test]
    public async Task A_deferred_terminal_the_ledger_absorbs_emits_no_drop()
    {
        // Control for the arm above. Identical window and identical deferred
        // range delete, differing ONLY in that the ledger has room. Without
        // this the positive arm would be satisfied by a counter that fires on
        // every deferred terminal rather than on a dropped one, which would
        // read as a permanent fault on a healthy tree.
        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafDeferredTerminalsDroppedAtCap,
            async () => await RunDeferredDropReplayAsync(
                cap: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
                deleteRanges: 4));

        Assert.That(measurements.Where(m => m.Value > 0), Is.Empty,
            "A deferred terminal the durable ledger absorbs is not a drop and must not be counted "
            + "as one - the counter measures the clamp fall-back, not deferral.");
    }

    [Test]
    public async Task The_drop_counter_is_pre_minted_for_every_partition_that_enters_replay()
    {
        // A Counter exports no series at all until its first Add, so an absent
        // series is ambiguous between "never fired" and "instrument never
        // wired" - which is precisely the ambiguity this instrument exists to
        // remove, and would be self-defeating to reproduce. Pre-minting at zero
        // makes a flat zero line a MEASURED zero, and makes a missing line mean
        // the build did not land. The bundled dashboard panel depends on this:
        // it deliberately omits the `or vector(0)` fallback its neighbours use.
        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafDeferredTerminalsDroppedAtCap,
            async () => await RunDeferredDropReplayAsync(
                cap: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
                deleteRanges: 1));

        var mintedPartitions = measurements
            .Where(m => m.Value == 0)
            .Select(m => m.Tags[LatticeMetrics.TagPartition])
            .Distinct()
            .ToList();

        Assert.That(mintedPartitions, Is.EquivalentTo(new object?[] { 0, 1 }),
            "Every partition that enters replay must mint its series at zero with the exact tag "
            + "set a later drop would carry, so an absent series is attributable to the build "
            + "rather than to the clamp never having fired.");
    }

    /// <summary>
    /// A window carrying exactly ONE unresolved prepare. One is the whole
    /// point: with a cap of 1 the ledger comes to rest at EXACTLY the cap,
    /// which is the value the strict <c>&gt;</c> threshold could never observe
    /// and the inclusive <c>&gt;=</c> threshold must.
    /// </summary>
    private static CommitLogSliceEntry[] WindowWithOneUnresolvedPrepare(Guid tx)
    {
        var entries = new CommitLogSliceEntry[12];
        entries[0] = FlushSet(1, "q01");
        entries[1] = new CommitLogSliceEntry(2, BuildPreparedSet(
            tx, "q02", Encoding.UTF8.GetBytes("vA"), treeId: FlushCeilingTreeId));
        for (var i = 3; i <= 12; i++)
            entries[i - 1] = FlushSet(i, $"q{i:D2}");
        return entries;
    }

    [Test]
    public async Task A_prepare_ledger_resting_exactly_at_the_cap_emits_the_beyond_cap_counter()
    {
        // P4 for issue #2756, and the arm that pins the off-by-one.
        //
        // ONE resident unresolved prepare against a cap of ONE leaves
        // work.Count at exactly 1. The capped deferred recorder refuses at
        // work.Count >= cap BEFORE adding, so a ledger at exactly the cap is
        // the resting value at which terminals begin being dropped - and the
        // strict `>` this threshold used to carry only ever fired at cap + 1,
        // which a ledger held at the cap never reaches. The counter was blind
        // at precisely the value that implies dropping.
        //
        // Revert `>=` to `>` in EnsureUnresolvedPrepareRecorded and this arm
        // goes red while the control below stays green.
        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafUnresolvedPrepareLedgerBeyondCap,
            async () =>
            {
                var state = NewFlushCeilingState();
                await RunInterruptedReplaysAsync(
                    state,
                    cts =>
                    [
                        BuildObservableCoordinator(
                            head: 12,
                            sliceSize: 2,
                            tail: state.State.ProjectionCheckpointOffset,
                            onRead: read =>
                            {
                                if (read == 2)
                                    cts.Cancel();
                            },
                            WindowWithOneUnresolvedPrepare(Guid.NewGuid())),
                    ],
                    attempts: 1,
                    maxDurableUnresolvedReplayWork: 1,
                    maxLeafReplayEntries: 2,
                    recordUnresolvedPreparesBeyondCap: true);
            });

        Assert.That(measurements.Where(m => m.Value > 0), Is.Not.Empty,
            "A prepare ledger resting at EXACTLY MaxDurableUnresolvedReplayWork must be reported. "
            + "That is the value at which the capped deferred recorder begins dropping every "
            + "subsequent terminal forever, so a threshold that fires only above it is blind "
            + "precisely where it matters.");
    }

    [Test]
    public async Task A_prepare_ledger_below_the_cap_emits_nothing()
    {
        // Control for the arm above, and the reason the inclusive threshold is
        // a widening by exactly one rather than an unconditional emission. The
        // same single prepare against an ample cap leaves work.Count well below
        // it and must stay silent; without this arm, `>=` degenerating into a
        // counter that fires on every recorded prepare would pass unnoticed and
        // the signal would be pure noise.
        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafUnresolvedPrepareLedgerBeyondCap,
            async () =>
            {
                var state = NewFlushCeilingState();
                await RunInterruptedReplaysAsync(
                    state,
                    cts =>
                    [
                        BuildObservableCoordinator(
                            head: 12,
                            sliceSize: 2,
                            tail: state.State.ProjectionCheckpointOffset,
                            onRead: read =>
                            {
                                if (read == 2)
                                    cts.Cancel();
                            },
                            WindowWithOneUnresolvedPrepare(Guid.NewGuid())),
                    ],
                    attempts: 1,
                    maxDurableUnresolvedReplayWork: 64,
                    maxLeafReplayEntries: 2,
                    recordUnresolvedPreparesBeyondCap: true);
            });

        Assert.That(measurements.Where(m => m.Value > 0), Is.Empty,
            "A prepare ledger well below the cap is not a crossing and must not be reported.");
    }
}
