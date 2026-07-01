using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the per-tick partition-priming fan-out on the outbound
/// shipper. The pump primes one shipping page per WAL partition at the
/// start of every drain tick; issuing those reads concurrently (rather
/// than one serialized round-trip at a time) is what keeps a
/// multi-partition tree's steady-state throughput from collapsing when
/// the WAL shards are activated on a different silo.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task InitializeDrainTick_primes_all_partitions_concurrently()
    {
        const int partitions = 4;
        var (grain, _, feeds, transport, _) = CreateMultiPartition(partitions);

        var gate = new object();
        var entered = 0;
        var maxConcurrent = 0;
        var allEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        foreach (var feed in feeds)
        {
            feed.OnReadShipping = async _ =>
            {
                lock (gate)
                {
                    entered++;
                    if (entered > maxConcurrent) maxConcurrent = entered;
                    if (entered >= partitions) allEntered.TrySetResult();
                }
                // Hold each priming read open until every partition's
                // read has entered. If the shipper fans the reads out
                // concurrently all N sit here together (maxConcurrent
                // reaches N); if it serialized them, only one is ever
                // in flight and the barrier never releases - the bounded
                // wait then makes the serial regression fail fast rather
                // than hang.
                await Task.WhenAny(allEntered.Task, Task.Delay(TimeSpan.FromSeconds(5)));
                lock (gate) { entered--; }
            };
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(maxConcurrent, Is.EqualTo(partitions),
            "the per-partition priming reads must be issued concurrently (fan-out), "
            + "not serialized one WAL round-trip at a time");
    }

    [Test]
    public async Task InitializeDrainTick_reads_every_partition_exactly_once_when_idle()
    {
        // Fan-out must still touch each partition exactly once per tick -
        // no partition is skipped and none is read twice - so an idle
        // multi-partition tree pays exactly one read per partition.
        const int partitions = 3;
        var (grain, _, feeds, _, _) = CreateMultiPartition(partitions);

        await grain.PumpForTestingAsync(CancellationToken.None);

        foreach (var feed in feeds)
        {
            Assert.That(feed.ReadCalls, Is.EqualTo(1),
                "each partition must be primed exactly once per pump tick");
            Assert.That(feed.ReadFromSequences[0], Is.EqualTo(0L),
                "an un-cursored partition primes from sequence 0");
        }
    }

    [Test]
    public async Task InitializeDrainTick_merges_across_partitions_after_concurrent_prime()
    {
        // The concurrent prime must not disturb the k-way HLC merge: two
        // partitions each holding one entry must still ship both, in
        // HLC-ascending order, on a single tick.
        const int partitions = 2;
        var (grain, state, feeds, transport, _) = CreateMultiPartition(partitions);
        feeds[0].Append(MakeEntry("k-late", ticks: 20));
        feeds[1].Append(MakeEntry("k-early", ticks: 10));
        var ackHlc = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "both partitions' entries must ship on the tick after a concurrent prime");
    }
}
