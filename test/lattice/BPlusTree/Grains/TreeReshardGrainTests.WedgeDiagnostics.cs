using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// reshard activity counter: per-tree reshard activity counters
/// (<c>ShardRootReshardInitiated</c> / <c>Rejected</c> / <c>Completed</c>
/// / <c>InFlight</c>). Distinct from the existing
/// <c>TreeReshardGrainTests</c> coverage (which exercises behaviour) -
/// these tests assert each counter / histogram fires on the exact code
/// path it is meant to attribute.
/// </summary>
public partial class TreeReshardGrainTests
{
    private sealed class MeterCapture : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

        public MeterCapture()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.Start();
        }

        public long Count(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName).Sum(r => (long)r.Value);

        public string? FirstReasonTag(string instrumentName)
        {
            var hit = Records.FirstOrDefault(r => r.Name == instrumentName);
            if (hit == default) { return null; }
            return hit.Tags.FirstOrDefault(t => t.Key == "reason").Value as string;
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public void ReshardAsync_increments_rejected_with_argument_out_of_range_min_reason()
    {
        using var capture = new MeterCapture();
        var (grain, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(1));

        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.rejected"), Is.EqualTo(1L),
            "reshard activity counter: newShardCount<2 must increment the rejected counter exactly once");
        Assert.That(capture.FirstReasonTag("orleans.lattice.shard_root.reshard.rejected"),
            Is.EqualTo("argument_out_of_range_min"),
            "reshard activity counter: the reason tag must attribute the rejection class");
        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.initiated"), Is.EqualTo(0L),
            "reshard activity counter: a rejected reshard must NOT increment the initiated counter");
    }

    [Test]
    public void ReshardAsync_increments_rejected_with_argument_out_of_range_max_reason()
    {
        using var capture = new MeterCapture();
        var (grain, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.ReshardAsync(LatticeConstants.DefaultVirtualShardCount + 1));

        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.rejected"), Is.EqualTo(1L));
        Assert.That(capture.FirstReasonTag("orleans.lattice.shard_root.reshard.rejected"),
            Is.EqualTo("argument_out_of_range_max"));
    }

    [Test]
    public async Task ReshardAsync_empty_tree_fast_path_increments_initiated_and_completed_in_lockstep()
    {
        using var capture = new MeterCapture();
        // CreateGrain default models a non-empty tree (every shard reports
        // AnyAsync = true). Override to model the empty-tree fast path
        // explicitly so the reshard takes the ApplyEmptyTreeResharAsync
        // branch which counts as both initiated and completed.
        var (grain, _, grainFactory, _) = CreateGrain();
        var emptyShard = Substitute.For<IShardRootGrain>();
        emptyShard.AnyBoundedAsync(Arg.Any<string?>())
            .Returns(Task.FromResult(new ShardAnyPage { Found = false }));
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(emptyShard);

        await grain.ReshardAsync(4);

        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.initiated"), Is.EqualTo(1L),
            "reshard activity counter: empty-tree fast path must increment initiated");
        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.completed"), Is.EqualTo(1L),
            "reshard activity counter: empty-tree fast path is a successful reshard and must increment completed in lockstep with initiated");
        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.rejected"), Is.EqualTo(0L),
            "reshard activity counter: empty-tree fast path is not a rejection");
    }

    [Test]
    public void ReshardAsync_increments_in_flight_histogram_at_entry()
    {
        using var capture = new MeterCapture();
        var (grain, _, _, _) = CreateGrain();

        // The first invocation always observes InProgress=false (= 0).
        // The validation that follows throws (argument < 2) but the
        // in-flight observation already happened before validation.
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(1));

        Assert.That(capture.Count("orleans.lattice.shard_root.reshard.in_flight"), Is.EqualTo(0L),
            "reshard activity counter: in-flight histogram observation must be 0 on first call (InProgress=false), which sums to 0");
        // Confirm an observation was actually recorded (sum=0 alone doesn't prove the record).
        Assert.That(capture.Records.Any(r => r.Name == "orleans.lattice.shard_root.reshard.in_flight"), Is.True,
            "reshard activity counter: an in-flight observation must be recorded at every ReshardAsync entry");
    }
}
