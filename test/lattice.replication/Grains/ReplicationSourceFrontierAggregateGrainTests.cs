using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4586 part 2b: the sender's per-peer aggregate of its trees' applied low
/// watermarks, its generation, and the pacing of lineage re-seeds.
/// </summary>
[TestFixture]
public class ReplicationSourceFrontierAggregateGrainTests
{
    private static readonly Guid Lineage = Guid.NewGuid();

    private static HybridLogicalClock Hlc(int counter) => new() { WallClockTicks = 1_000, Counter = counter };

    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static (ReplicationSourceFrontierAggregateGrain Grain, FakePersistentState<ReplicationSourceFrontierAggregateState> State, List<string> Trees, Clock Clock) Create(params string[] trees)
    {
        var list = trees.ToList();
        var membership = Substitute.For<IReplicatedTreeMembership>();
        membership.ReplicatedTrees.Returns(_ => list.ToArray());
        var state = new FakePersistentState<ReplicationSourceFrontierAggregateState>();
        var clock = new Clock(DateTimeOffset.UnixEpoch);
        return (new ReplicationSourceFrontierAggregateGrain(membership, state) { TimeProvider = clock }, state, list, clock);
    }

    [Test]
    public async Task Aggregate_is_the_minimum_over_every_replicated_tree_and_zero_while_one_has_none()
    {
        var (grain, _, _, _) = Create("t1", "t2");

        var (onlyOne, _) = await grain.ReportAsync("t1", Lineage, Hlc(5), holdsLineageReseed: false);
        var (both, _) = await grain.ReportAsync("t2", Lineage, Hlc(3), holdsLineageReseed: false);
        var (zeroed, _) = await grain.ReportAsync("t1", Lineage, HybridLogicalClock.Zero, holdsLineageReseed: false);

        Assert.Multiple(() =>
        {
            Assert.That(onlyOne, Is.EqualTo(HybridLogicalClock.Zero), "a tree with no report vouches for nothing");
            Assert.That(both, Is.EqualTo(Hlc(3)));
            Assert.That(zeroed, Is.EqualTo(HybridLogicalClock.Zero), "a tree whose watermark dropped to none zeroes the aggregate");
        });
    }

    [Test]
    public async Task A_stale_report_drops_out_of_the_aggregate()
    {
        var (grain, _, _, clock) = Create("t1", "t2");
        await grain.ReportAsync("t1", Lineage, Hlc(5), holdsLineageReseed: false);
        clock.Now += ReplicationSourceFrontierAggregateGrain.ReportTtl + TimeSpan.FromSeconds(1);

        var (origin, _) = await grain.ReportAsync("t2", Lineage, Hlc(7), holdsLineageReseed: false);

        Assert.That(origin, Is.EqualTo(HybridLogicalClock.Zero), "a tree whose shipper stopped reporting vouches for nothing");
    }

    [Test]
    public async Task Generation_rises_on_activation_on_a_lineage_change_and_when_a_tree_joins_and_never_otherwise()
    {
        var (grain, state, trees, _) = Create("t1");
        var (_, first) = await grain.ReportAsync("t1", Lineage, Hlc(5), holdsLineageReseed: false);
        var (_, steady) = await grain.ReportAsync("t1", Lineage, Hlc(6), holdsLineageReseed: false);
        var restamped = Guid.NewGuid();
        var (_, changed) = await grain.ReportAsync("t1", restamped, Hlc(6), holdsLineageReseed: false);
        trees.Add("t2");
        var (_, joined) = await grain.ReportAsync("t1", restamped, Hlc(6), holdsLineageReseed: false);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1L), "an activation starts a generation");
            Assert.That(steady, Is.EqualTo(first));
            Assert.That(changed, Is.GreaterThan(steady), "a receiver lineage change starts a generation");
            Assert.That(joined, Is.GreaterThan(changed), "a joining tree starts a generation");
            Assert.That(state.State.Generation, Is.EqualTo(joined), "the generation is durable");
        });
    }

    [Test]
    public async Task First_sighting_of_a_lineage_starts_a_generation_and_the_aggregate_waits_for_the_tree_to_re_cover()
    {
        var (grain, _, _, _) = Create("t1", "t2");
        var (_, start) = await grain.ReportAsync("t1", Lineage, Hlc(5), holdsLineageReseed: false);
        var (_, unlineaged) = await grain.ReportAsync("t2", Guid.Empty, HybridLogicalClock.Zero, holdsLineageReseed: false);
        var (heldAtZero, sighted) = await grain.ReportAsync("t2", Lineage, HybridLogicalClock.Zero, holdsLineageReseed: false);
        var (recovered, steady) = await grain.ReportAsync("t2", Lineage, Hlc(4), holdsLineageReseed: false);

        var (fresh, _, _, _) = Create("t1", "t2");
        await fresh.ReportAsync("t1", Lineage, Hlc(5), holdsLineageReseed: false);
        var (_, beforeFirst) = await fresh.ReportAsync("t1", Lineage, Hlc(5), holdsLineageReseed: false);
        var (_, firstReport) = await fresh.ReportAsync("t2", Lineage, Hlc(3), holdsLineageReseed: false);

        Assert.Multiple(() =>
        {
            Assert.That(unlineaged, Is.EqualTo(start), "a tree tracking no lineage starts nothing");
            Assert.That(sighted, Is.GreaterThan(unlineaged), "a lineage seen where none was starts a generation");
            Assert.That(heldAtZero, Is.EqualTo(HybridLogicalClock.Zero), "the aggregate vouches for nothing until the tree re-covers");
            Assert.That(recovered, Is.EqualTo(Hlc(4)));
            Assert.That(steady, Is.EqualTo(sighted));
            Assert.That(firstReport, Is.GreaterThan(beforeFirst), "a tree's first report under a lineage starts a generation");
        });
    }

    [Test]
    public async Task Lineage_re_seeds_are_paced_one_at_a_time_per_peer()
    {
        var (grain, _, _, clock) = Create("t1", "t2");

        var firstGranted = await grain.TryAcquireLineageReseedAsync("t1");
        var secondRefused = await grain.TryAcquireLineageReseedAsync("t2");
        var firstAgain = await grain.TryAcquireLineageReseedAsync("t1");
        await grain.ReleaseLineageReseedAsync("t1");
        var secondGranted = await grain.TryAcquireLineageReseedAsync("t2");

        clock.Now += ReplicationSourceFrontierAggregateGrain.LineageReseedLeaseTtl + TimeSpan.FromSeconds(1);
        var lapsedSlotReused = await grain.TryAcquireLineageReseedAsync("t1");

        Assert.Multiple(() =>
        {
            Assert.That(firstGranted, Is.True);
            Assert.That(secondRefused, Is.False, "a second tree waits while one re-seeds");
            Assert.That(firstAgain, Is.True, "the holder keeps its slot");
            Assert.That(secondGranted, Is.True, "a released slot goes to the next tree");
            Assert.That(lapsedSlotReused, Is.True, "a slot nobody renews lapses");
        });
    }
}
