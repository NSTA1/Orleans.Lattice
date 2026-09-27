using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3628: the per-activation drain cost of the repocontext container rose
/// about 4x and nothing on the deactivation path could say which barrier the
/// time went to. <see cref="LatticeMetrics.LeafDeactivationBarrierDuration"/>
/// times every barrier; these tests pin that each barrier that runs is timed
/// under its own <c>reason</c>, and that a faulting barrier is timed too.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static IDisposable ListenForBarrierDurations(List<(string Reason, string? Tree, double Milliseconds)> samples) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.LeafDeactivationBarrierDuration,
            l => l.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                string? reason = null;
                string? tree = null;
                foreach (var t in tags)
                {
                    if (t.Key == LatticeMetrics.TagReason && t.Value is string r)
                    {
                        reason = r;
                    }
                    else if (t.Key == LatticeMetrics.TagTree && t.Value is string tr)
                    {
                        tree = tr;
                    }
                }

                if (reason is not null)
                {
                    lock (samples)
                    {
                        samples.Add((reason, tree, value));
                    }
                }
            }));

    [Test]
    public async Task Deactivation_times_every_durability_barrier_under_its_own_reason()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, _) = CreateLeafForBarrierContainment(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingCheckpointAsync(grain, state, wal);

        var samples = new List<(string Reason, string? Tree, double Milliseconds)>();
        using (ListenForBarrierDurations(samples))
        {
            await DeactivateLeafAsync(grain);
        }

        var ours = samples.Where(s => s.Tree == BarrierContainmentTreeId).ToList();
        Assert.Multiple(() =>
        {
            foreach (var barrier in new[]
            {
                LatticeMetrics.DeactivationBarrierCheckpointFlush.Value,
                LatticeMetrics.DeactivationBarrierSnapshotCapture.Value,
                LatticeMetrics.DeactivationBarrierFrontierPin.Value,
            })
            {
                Assert.That(ours.Count(s => Equals(s.Reason, barrier)), Is.EqualTo(1),
                    $"the '{barrier}' barrier ran once and must contribute exactly one duration sample, "
                    + "tagged with the leaf's tree, or a drain cannot be decomposed by barrier.");
            }

            Assert.That(ours.Select(s => s.Milliseconds), Has.All.GreaterThanOrEqualTo(0d));
        });
    }

    [Test]
    public async Task A_faulting_deactivation_barrier_is_still_timed()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var (grain, state, _) = CreateLeafForBarrierContainment(wal.Coordinator);
        await ActivateAsync(grain);
        await QueuePendingCheckpointAsync(grain, state, wal);

        state.ThrowOnWrite = new InvalidOperationException("injected checkpoint-flush storage fault");

        var samples = new List<(string Reason, string? Tree, double Milliseconds)>();
        using (ListenForBarrierDurations(samples))
        {
            await DeactivateLeafAsync(grain);
        }

        Assert.That(
            samples.Count(s => s.Tree == BarrierContainmentTreeId
                && Equals(s.Reason, LatticeMetrics.DeactivationBarrierCheckpointFlush.Value)),
            Is.EqualTo(1),
            "a barrier that faulted still consumed part of the drain, so it must still be timed; "
            + "dropping the sample on the fault path would make a failing barrier look free.");
    }
}
