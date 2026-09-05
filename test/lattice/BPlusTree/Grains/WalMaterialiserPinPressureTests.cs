using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalMaterialiserPinPressure"/>, the caller-side
/// instrumentation of the <b>durable</b> leaf-materialiser pin store that backs
/// both halves of this change: the shed gate (issue #2014) and the WAL
/// saturation signal's durable-floor input (issue #2015).
/// <para>
/// Two properties are load-bearing. First, the latency trip is gated on the
/// configured threshold, so a host that has not opted in records nothing at all.
/// Second, the shed window is self-tuning off the measured duration, so a
/// healthy store (sub-millisecond writes) never sheds.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinPressureTests
{
    private static int _treeSeed;
    private string _tree = null!;
    private string _shardKey = null!;

    [SetUp]
    public void SetUp()
    {
        WalMaterialiserPinPressure.ResetForTests();
        _tree = $"tree-pressure-{Interlocked.Increment(ref _treeSeed)}";
        _shardKey = _tree + WalMaterialiserPinRouting.ShardSeparator + "3";
    }

    [TearDown]
    public void TearDown() => WalMaterialiserPinPressure.ResetForTests();

    private long TripsFor(int shard = 3) =>
        WalMaterialiserPinPressure._latencyTrips.TryGetValue((_tree, shard), out var v) ? v : 0;

    [Test]
    public void RecordWrite_with_disabled_threshold_records_no_trip()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 60_000, faulted: false, latencyThresholdMs: null);

        Assert.That(TripsFor(), Is.EqualTo(0),
            "with the saturation input disabled (threshold null) even an extreme write must not be counted");
    }

    [Test]
    public void RecordWrite_under_threshold_records_no_trip()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 10, faulted: false, latencyThresholdMs: 1_000);

        Assert.That(TripsFor(), Is.EqualTo(0));
    }

    [Test]
    public void RecordWrite_at_threshold_records_a_trip()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 1_000, faulted: false, latencyThresholdMs: 1_000);

        Assert.That(TripsFor(), Is.EqualTo(1),
            "the threshold is inclusive, matching the writer-side flush-latency input");
    }

    [Test]
    public void RecordWrite_fault_records_a_trip_regardless_of_duration()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 0, faulted: true, latencyThresholdMs: 1_000);

        Assert.That(TripsFor(), Is.EqualTo(1),
            "a pin write that throws is at least as strong a signal of an unhealthy durable store as one that runs long");
    }

    [Test]
    public void RecordWrite_accumulates_trips_per_shard()
    {
        var otherShardKey = _tree + WalMaterialiserPinRouting.ShardSeparator + "5";

        WalMaterialiserPinPressure.RecordWrite(_shardKey, 2_000, false, 1_000);
        WalMaterialiserPinPressure.RecordWrite(_shardKey, 2_000, false, 1_000);
        WalMaterialiserPinPressure.RecordWrite(otherShardKey, 2_000, false, 1_000);

        Assert.Multiple(() =>
        {
            Assert.That(TripsFor(shard: 3), Is.EqualTo(2));
            Assert.That(TripsFor(shard: 5), Is.EqualTo(1));
        });
    }

    [Test]
    public void ShouldShed_is_false_for_an_unseen_shard()
    {
        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.False);
    }

    [Test]
    public void ShouldShed_is_false_after_an_instant_write()
    {
        // A healthy store's writes measure zero elapsed milliseconds, which must
        // open no shed window at all - otherwise the gate would suppress traffic
        // on a perfectly healthy deployment.
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 0, faulted: false, latencyThresholdMs: null);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.False);
    }

    [Test]
    public void ShouldShed_is_true_after_a_slow_write()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 30_000, faulted: false, latencyThresholdMs: null);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.True,
            "a multi-second durable write must open a shed window so reports stop piling into the shard's non-reentrancy queue");
    }

    [Test]
    public void ShouldShed_is_false_after_a_cheap_fault()
    {
        // A pin write can fail cheaply - a synchronous rejection, a missing
        // activation - in a millisecond or two. LeafCursorReporter rolls the
        // debounce back on exactly that failure so the next checkpoint retries,
        // so a window opened by a cheap fault would shed the retry the rollback
        // exists to guarantee. Only demonstrated cost opens the gate.
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 3, faulted: true, latencyThresholdMs: 1_000);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.False,
            "a fault that cost nothing to attempt must not suppress its own retry");
    }

    [Test]
    public void ShouldShed_is_true_after_an_expensive_fault()
    {
        // The issue #2012 condition: attempts that cost seconds apiece. Here the
        // fault IS the evidence of pressure, and piling on more attempts only
        // lengthens the queue every other reporting leaf waits behind.
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 30_000, faulted: true, latencyThresholdMs: 1_000);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.True,
            "a fault that took seconds must open a shed window even though nothing landed");
    }

    [Test]
    public void ShouldShed_window_is_scoped_to_the_shard_that_was_slow()
    {
        var otherShardKey = _tree + WalMaterialiserPinRouting.ShardSeparator + "5";
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 30_000, faulted: false, latencyThresholdMs: null);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(otherShardKey), Is.False,
            "one slow shard must not suppress reporting to a healthy sibling shard");
    }

    [Test]
    public void RecordWrite_never_shortens_an_existing_shed_window()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 30_000, faulted: false, latencyThresholdMs: null);
        // A subsequent fast write must not clear the window opened by the slow
        // one: the store has demonstrated it can stall, and the window is the
        // amortisation of that demonstrated cost.
        WalMaterialiserPinPressure.RecordWrite(_shardKey, elapsedMs: 1, faulted: false, latencyThresholdMs: null);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.True);
    }

    [Test]
    public void ResetForTests_clears_trips_and_shed_windows()
    {
        WalMaterialiserPinPressure.RecordWrite(_shardKey, 30_000, true, 1_000);

        WalMaterialiserPinPressure.ResetForTests();

        Assert.Multiple(() =>
        {
            Assert.That(TripsFor(), Is.EqualTo(0));
            Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.False);
        });
    }

    [Test]
    public void ForceShedForTests_opens_a_shed_window()
    {
        WalMaterialiserPinPressure.ForceShedForTests(_shardKey, durationMs: 60_000);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.True);
    }

    [Test]
    public void ForceShedForTests_with_an_elapsed_window_does_not_shed()
    {
        WalMaterialiserPinPressure.ForceShedForTests(_shardKey, durationMs: -1);

        Assert.That(WalMaterialiserPinPressure.ShouldShed(_shardKey), Is.False,
            "the shed window must expire on its own so a recovered store resumes reporting without intervention");
    }
}
