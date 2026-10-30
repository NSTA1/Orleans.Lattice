using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- (#5242) The merge-observer event sink ---
    //
    // An observer's AcceptWithEvent reason used to be discarded. Each event is
    // now counted on orleans.lattice.leaf.merge_observer_events (tagged by tree)
    // and logged as a structured warning, on the two paths that consult the
    // observer: the foreground leaf set and the typed-CRDT apply. MergeManyAsync
    // never consults the observer, which the last test pins so a change to the
    // observed-path set is a deliberate one.

    private const string MergeObserverEventMessageMarker = "orleans.lattice.leaf.merge_observer_events";

    private static (BPlusLeafGrain Grain, RecordingLoggerFactory Logs, string TreeId) CreateEventSinkGrain(
        Func<LatticeMergeMode, LatticeMergeOutcome> outcome)
    {
        var treeId = $"merge-observer-events-{Guid.NewGuid():N}";
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        var logs = new RecordingLoggerFactory();
        var grain = CreateObserverGrain(new RecordingMergeObserver(outcome), state: state, loggerFactory: logs);
        return (grain, logs, treeId);
    }

    private static async Task<List<(long Value, Dictionary<string, object?> Tags)>> RecordMergeObserverEventsAsync(
        string treeId, Func<Task> body)
    {
        var all = await RecordMeasurementsAsync(LatticeMetrics.LeafMergeObserverEvents, body);
        return all.Where(m => Equals(m.Tags.GetValueOrDefault(LatticeMetrics.TagTree), treeId)).ToList();
    }

    private static List<RecordedLogEntry> MergeObserverEventLogs(RecordingLoggerFactory logs) =>
        logs.Entries.Where(e => e.Message.Contains(MergeObserverEventMessageMarker, StringComparison.Ordinal)).ToList();

    private static void AssertSingleMergeObserverEvent(
        List<(long Value, Dictionary<string, object?> Tags)> measurements,
        List<RecordedLogEntry> entries,
        string treeId,
        LatticeMergeMode mode,
        string reason)
    {
        Assert.Multiple(() =>
        {
            Assert.That(measurements, Has.Count.EqualTo(1),
                "one AcceptWithEvent outcome must advance orleans.lattice.leaf.merge_observer_events exactly once (issue #5242)");
            Assert.That(measurements.Single().Value, Is.EqualTo(1));
            Assert.That(measurements.Single().Tags.ContainsKey(LatticeTenantLabel.TagTenant), Is.True,
                "every leaf-level instrument carries the derived tenant tag");

            Assert.That(entries, Has.Count.EqualTo(1), "one AcceptWithEvent outcome must log exactly one entry");
            var entry = entries.Single();
            Assert.That(entry.Level, Is.EqualTo(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.That(entry.Value("TreeId"), Is.EqualTo(treeId));
            Assert.That(entry.Value("MergeMode"), Is.EqualTo(mode));
            Assert.That(entry.Value("Reason"), Is.EqualTo(reason));
        });
    }

    [Test]
    public async Task MergeObserverEvent_on_set_counts_and_logs_once_with_the_tree_tag()
    {
        var (grain, logs, treeId) = CreateEventSinkGrain(_ => LatticeMergeOutcome.AcceptWithEvent("rule 0 failed"));

        var measurements = await RecordMergeObserverEventsAsync(
            treeId, () => grain.SetAsync("k", Encoding.UTF8.GetBytes("raw")));

        AssertSingleMergeObserverEvent(
            measurements, MergeObserverEventLogs(logs), treeId, LatticeMergeMode.LwwRegister, "rule 0 failed");
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(Encoding.UTF8.GetBytes("raw")),
            "observability only: the merged value is stored unchanged");
    }

    [Test]
    public async Task MergeObserverEvent_on_crdt_apply_counts_and_logs_once_with_the_crdt_mode()
    {
        var (grain, logs, treeId) = CreateEventSinkGrain(_ => LatticeMergeOutcome.AcceptWithEvent("too large"));

        var measurements = await RecordMergeObserverEventsAsync(
            treeId, () => grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, OrSetAddDelta("a")));

        AssertSingleMergeObserverEvent(
            measurements, MergeObserverEventLogs(logs), treeId, LatticeMergeMode.OrSet, "too large");
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(grain.EntriesForTest["k"].Value!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("a")), Is.True,
            "observability only: the merged CRDT state is stored unchanged");
    }

    [Test]
    public async Task MergeObserverEvent_is_neither_counted_nor_logged_when_the_observer_accepts()
    {
        var (grain, logs, treeId) = CreateEventSinkGrain(_ => LatticeMergeOutcome.Accept());

        var measurements = await RecordMergeObserverEventsAsync(treeId, async () =>
        {
            await grain.SetAsync("k", Encoding.UTF8.GetBytes("raw"));
            await grain.ApplyCrdtDeltaAsync("s", LatticeMergeMode.OrSet, OrSetAddDelta("a"));
        });

        Assert.Multiple(() =>
        {
            Assert.That(measurements, Is.Empty, "an accepted merge is not an event");
            Assert.That(MergeObserverEventLogs(logs), Is.Empty, "an accepted merge logs nothing");
        });
    }

    [Test]
    public async Task MergeObserverEvent_is_not_raised_by_merge_many_which_never_consults_the_observer()
    {
        var (grain, logs, treeId) = CreateEventSinkGrain(_ => LatticeMergeOutcome.AcceptWithEvent("unseen"));

        var measurements = await RecordMergeObserverEventsAsync(treeId, () => grain.MergeManyAsync(Lww("a", "b")));

        Assert.Multiple(() =>
        {
            Assert.That(measurements, Is.Empty,
                "MergeManyAsync (replication apply, tree merge, merging restore, snapshot and resize copies) "
                + "is not an observed merge path; the option and docs say so");
            Assert.That(MergeObserverEventLogs(logs), Is.Empty);
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }));
        });
    }
}
