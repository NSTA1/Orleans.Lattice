using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the deactivation-time checkpoint observation added for issue
/// #2280: once per graceful deactivation the leaf records how many projection
/// checkpoint offsets the activation banked durably on its way out, tagged by
/// tree, deactivation reason and activation temperature.
/// <para>
/// The instrument exists to answer whether a cold replay is banking progress
/// before it goes away, so these tests pin the three properties that decide
/// whether it can answer that: it fires exactly once per deactivation, it
/// reports the durably banked advance rather than an in-memory one, and it
/// carries no unbounded-cardinality tag.
/// </para>
/// <para>
/// The instrument is a <b>lower bound</b> on deactivations by construction -
/// crash teardowns bypass the hook, and an activation that throws never
/// reaches it - so nothing here asserts a census, and
/// <see cref="BPlusLeafGrainActivationFailureHookContractTests"/> pins the
/// Orleans behaviour that makes the second of those gaps real.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string DeactivationObservationInstrument =
        "orleans.lattice.leaf.deactivation.checkpoint_delta";

    private static ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)> CaptureCheckpointDeltas(
        out IDisposable listener)
    {
        var records = new ConcurrentBag<(long, KeyValuePair<string, object?>[])>();
        listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafDeactivationCheckpointDelta,
            l => l.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => records.Add((value, tags.ToArray()))));
        return records;
    }

    [Test]
    public async Task OnDeactivateAsync_records_the_checkpoint_offsets_banked_during_teardown()
    {
        // The hook flushes a pending checkpoint on the way out (see
        // OnDeactivateAsync_flushes_pending_checkpoint). That flush is the
        // progress the activation banks durably, and it is exactly what this
        // observation must report - measured against the PERSISTED mark, not
        // the in-memory one. GetCurrentCheckpointForPartition returns
        // max(persisted, pending), so differencing THAT would already include
        // the pending 99 on entry and report zero here at every rate of
        // occurrence: a structural zero rather than a weak measurement.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "deact-observation-tree";
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(99);

        var records = CaptureCheckpointDeltas(out var listener);
        using (listener)
        {
            await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                CancellationToken.None);
        }

        Assert.That(records, Has.Count.EqualTo(1),
            "The observation must be recorded exactly ONCE per deactivation - a hook that recorded "
            + "per partition or per retry would inflate the count and make the histogram's own count "
            + "series unusable as a deactivation tally.");
        Assert.That(records.Single().Value, Is.EqualTo(99L),
            "The recorded delta must be the advance in the PERSISTED checkpoint across the hook.");
    }

    [Test]
    public async Task OnDeactivateAsync_records_zero_rather_than_nothing_when_no_progress_was_banked()
    {
        // A deactivation that banks nothing must still RECORD, reporting zero.
        // Omitting the measurement would make "banked nothing" and "did not
        // deactivate" indistinguishable in the series, which is the failure
        // mode this whole instrument exists to end.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "deact-observation-tree";
        var grain = CreateGrain(state);

        var records = CaptureCheckpointDeltas(out var listener);
        using (listener)
        {
            await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                CancellationToken.None);
        }

        Assert.That(records, Has.Count.EqualTo(1),
            "A deactivation that banks nothing must still be observed.");
        Assert.That(records.Single().Value, Is.Zero);
    }

    [Test]
    public async Task Deactivation_observation_is_tagged_by_tree_and_reason_and_never_by_leaf()
    {
        // Cardinality guard. The live-leaf population is unbounded, so a leaf
        // id in a TAG would make this series unbounded too. Per-leaf detail
        // belongs on the accompanying debug log line, and this test is what
        // stops a well-meaning "just add the leaf, it is useful" change.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "deact-observation-tree";
        var grain = CreateGrain(state, replicaId: "leaf-that-must-not-appear-in-a-tag");

        var records = CaptureCheckpointDeltas(out var listener);
        using (listener)
        {
            await ((IGrainBase)grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "drain"),
                CancellationToken.None);
        }

        var tags = records.Single().Tags;

        Assert.That(
            tags.Select(t => t.Key),
            Is.EquivalentTo(new[]
            {
                LatticeMetrics.TagTree,
                LatticeMetrics.TagDeactivationReason,
                LatticeMetrics.TagActivationTemperature,
                LatticeTenantLabel.TagTenant,
            }),
            "The observation must carry EXACTLY these four bounded tags. Any additional tag - a leaf id "
            + "above all - makes the series cardinality unbounded. The tenant dimension is derived from "
            + "the tree, so it adds no cardinality beyond the tree tag already present.");

        Assert.That(
            tags.Single(t => t.Key == LatticeMetrics.TagTree).Value,
            Is.EqualTo("deact-observation-tree"));
        Assert.That(
            tags.Single(t => t.Key == LatticeMetrics.TagDeactivationReason).Value,
            Is.EqualTo(DeactivationReasonCode.ApplicationRequested.ToString()),
            "The reason tag must carry the reason Orleans supplied, so a shutdown-driven teardown can be "
            + "separated from an idle collection.");
        Assert.That(
            tags.Single(t => t.Key == LatticeMetrics.TagActivationTemperature).Value,
            Is.EqualTo(LatticeMetrics.ActivationTemperatureWarm.Value),
            "A grain that never ran a cold WAL rebuild must report warm.");
    }
}
