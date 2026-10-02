using System.Collections.Concurrent;

using Microsoft.Extensions.Options;

using NSubstitute;

using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3319: the batched durable-pin write path
/// recorded its debounce state <b>before</b> the write it describes.
/// <para>
/// <c>PersistPinBatchDurablyAsync</c> bucketed the reports by routed pin shard
/// and, during that walk, recorded every consumer in the per-<c>(tree,
/// consumer)</c> debounce state - at which point nothing had been written. The
/// method then swallows-and-logs any fault, by design, so a faulted shard left
/// the reporter claiming a durable write that never happened and nothing
/// cleared it.
/// </para>
/// <para>
/// The harm is that the claim is <i>consulted</i>:
/// <c>NoteDurableMaterialiserFrontier</c> drops a report at or below the
/// recorded frontier and offset outright, before it reaches the wall-clock
/// spacing gate and before the crossing-zero escape. So the next report for
/// those consumers was coalesced away - suppressing the very retry that would
/// have repaired the missed write. That suppression, not the stale dictionary
/// entry, is what these tests pin down.
/// </para>
/// <para>
/// <c>SeedShardAsync</c>'s own contract states the obligation ("a caller
/// holding debounce state must roll it back rather than record it as written"),
/// and <c>WriteDurablePinAsync</c> already honours it on the single-consumer
/// path. Only the batch path did not.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterDurablePinDebounceOrderingTests
{
    private const string Tree = "tree-3319";

    /// <summary>
    /// Pin-shard fan-out these tests run at. It must exceed one: the whole
    /// point is per-shard attribution, and the default resolved from a
    /// <see langword="null"/> options monitor is a single shard, which would
    /// route both consumers to the same grain and quietly void every assertion
    /// below.
    /// </summary>
    private const int PinShards = 4;

    /// <summary>
    /// The frontier and offset the faulted batch claims to have written. Every
    /// retry below re-reports this <b>identical</b> pair rather than an advance,
    /// which is what makes these tests independent of wall-clock timing: an
    /// equal report is suppressed by the unconditional coalescing predicate,
    /// which has no spacing escape, so a test that takes a second longer on a
    /// loaded agent cannot pass for the wrong reason.
    /// </summary>
    private static readonly HybridLogicalClock Frontier = new() { WallClockTicks = 100, Counter = 0 };

    private const long Offset = 100;

    [SetUp]
    public void ResetPressure() => WalMaterialiserPinPressure.ResetForTests();

    [TearDown]
    public void ClearPressure() => WalMaterialiserPinPressure.ResetForTests();

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = PinShards };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Two consumer ids that route to different pin shards, discovered rather
    /// than hard-coded so a change to the routing hash cannot quietly collapse
    /// this fixture onto a single shard and void the per-shard assertions.
    /// </summary>
    private static (string Healthy, string Faulted) ConsumersOnDistinctShards(int shards)
    {
        var healthy = $"_lattice_materialiser_{Tree}_leaf-0";
        var healthyKey = WalMaterialiserPinRouting.ShardKey(Tree, healthy, shards);

        for (var i = 1; i < 512; i++)
        {
            var candidate = $"_lattice_materialiser_{Tree}_leaf-{i}";
            if (!string.Equals(
                    WalMaterialiserPinRouting.ShardKey(Tree, candidate, shards),
                    healthyKey,
                    StringComparison.Ordinal))
            {
                return (healthy, candidate);
            }
        }

        throw new InvalidOperationException(
            $"No two of 512 candidate consumer ids route to different pin shards at a shard count of {shards}; "
            + "the per-shard attribution these tests assert cannot be exercised.");
    }

    private static (LeafCursorReporter Reporter, TogglingPinGrain Faulting, TogglingPinGrain Healthy, string FaultedConsumer, string HealthyConsumer) Create()
    {
        var monitor = Monitor();
        var shards = WalMaterialiserPinRouting.ResolveShardCount(monitor);
        Assert.That(shards, Is.GreaterThan(1),
            "these tests need a real fan-out to attribute an outcome to one shard and not the other");

        var (healthyConsumer, faultedConsumer) = ConsumersOnDistinctShards(shards);
        var faultedKey = WalMaterialiserPinRouting.ShardKey(Tree, faultedConsumer, shards);

        var faulting = new TogglingPinGrain { Fail = true };
        var healthy = new TogglingPinGrain { Fail = false };

        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>()));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(callInfo =>
            string.Equals(callInfo.ArgAt<string>(0), faultedKey, StringComparison.Ordinal)
                ? faulting
                : healthy);

        return (new LeafCursorReporter(registry, factory, monitor), faulting, healthy, faultedConsumer, healthyConsumer);
    }

    private static IReadOnlyList<MaterialiserPinReport> Batch(string healthy, string faulted) =>
    [
        new MaterialiserPinReport(healthy, Frontier, Offset),
        new MaterialiserPinReport(faulted, Frontier, Offset),
    ];

    [Test]
    public async Task A_faulted_shard_write_is_not_recorded_as_landed_so_the_next_report_is_retried()
    {
        var (reporter, faulting, healthy, faultedConsumer, healthyConsumer) = Create();

        var acknowledged = await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Batch(healthyConsumer, faultedConsumer), CancellationToken.None);

        Assert.That(acknowledged, Is.False,
            "a batch whose shard write faulted has not landed and must not be acknowledged");
        Assert.That(faulting.Attempts, Is.EqualTo(1),
            "the faulting shard must actually have been attempted, otherwise the retry below is not "
            + "exercising a recovery from a missed write at all");
        Assert.That(faulting.Landed, Is.Empty,
            "nothing reached the faulting shard's durable store, which is the premise of this test");

        // Heal the shard, then re-report the IDENTICAL frontier and offset the
        // faulted batch claimed to have written. It is an advance over nothing,
        // so a reporter that recorded the faulted write as landed coalesces it
        // away unconditionally and the pin never reaches this frontier at all.
        faulting.Fail = false;
        reporter.NoteDurableMaterialiserFrontier(Tree, faultedConsumer, Frontier, Offset);

        await TestPoll.UntilAsync(
            () => faulting.Landed.Any(r => r.Consumer == faultedConsumer && r.Frontier == Frontier),
            "the report missed by the faulted shard write must be retried and land",
            TimeSpan.FromSeconds(2));

        Assert.That(faulting.Landed.Any(r => r.Consumer == faultedConsumer && r.Frontier == Frontier), Is.True,
            "a faulted batch write must leave the debounce state untouched for that shard's consumers, "
            + "so the next report retries it rather than being coalesced away as already durable");
    }

    [Test]
    public async Task A_shard_that_wrote_successfully_still_coalesces_its_next_identical_report()
    {
        var (reporter, _, healthy, faultedConsumer, healthyConsumer) = Create();

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Batch(healthyConsumer, faultedConsumer), CancellationToken.None);

        var landedByBatch = healthy.Landed.Count(r => r.Consumer == healthyConsumer);
        Assert.That(landedByBatch, Is.EqualTo(1),
            "the healthy shard's write must have landed exactly once from the batch");

        // The sibling shard faulted, but this one did not. Admitting a retry
        // here too would be the opposite over-correction: a blanket rollback
        // that un-coalesces writes which legitimately were durable, turning
        // every partially-faulted batch into redundant traffic on the healthy
        // shards. The record is per shard, keyed to that shard's own outcome.
        reporter.NoteDurableMaterialiserFrontier(Tree, healthyConsumer, Frontier, Offset);
        await Task.Delay(250);

        Assert.That(healthy.Landed.Count(r => r.Consumer == healthyConsumer), Is.EqualTo(landedByBatch),
            "a shard whose write landed must keep its debounce state, so an identical follow-up report "
            + "is still coalesced away rather than rewritten");
    }

    [Test]
    public async Task A_faulted_birth_seed_is_not_recorded_as_landed_either()
    {
        var (reporter, faulting, _, faultedConsumer, healthyConsumer) = Create();

        // The write-through birth seed shares PersistPinBatchDurablyAsync with
        // the retention flush, so it inherits the same ordering. It is the more
        // consequential of the two: the block pin is the barrier that stops WAL
        // GC trimming past a leaf that has not yet checkpointed.
        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree, Batch(healthyConsumer, faultedConsumer), CancellationToken.None);

        Assert.That(faulting.Attempts, Is.EqualTo(1));
        Assert.That(faulting.Landed, Is.Empty);

        faulting.Fail = false;
        reporter.NoteDurableMaterialiserFrontier(Tree, faultedConsumer, Frontier, Offset);

        await TestPoll.UntilAsync(
            () => faulting.Landed.Any(r => r.Consumer == faultedConsumer && r.Frontier == Frontier),
            "the block pin missed by the faulted seed must be retried and land",
            TimeSpan.FromSeconds(2));

        Assert.That(faulting.Landed.Any(r => r.Consumer == faultedConsumer && r.Frontier == Frontier), Is.True,
            "a faulted birth seed must not record its pins as durable, or the leaf's next checkpoint "
            + "coalesces the block pin away and no durable floor is ever established for it");
    }

    /// <summary>
    /// Pin-grain fake whose failure can be switched off, so one test can drive a
    /// write to fault and then observe whether the retry it should have left
    /// admissible actually lands. Counts attempts separately from landed
    /// reports: a suppressed retry and a faulted one are both "nothing landed",
    /// and only the attempt count tells them apart.
    /// </summary>
    private sealed class TogglingPinGrain : IWalMaterialiserPinGrain
    {
        private int _attempts;

        public volatile bool Fail;

        public int Attempts => Volatile.Read(ref _attempts);

        public ConcurrentBag<(string Consumer, HybridLogicalClock Frontier, long Offset)> Landed { get; } = new();

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) =>
            Accept([new MaterialiserPinReport(consumerId, frontier, -1)]);

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Accept(reports);

        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Accept(reports);

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));

        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal));

        public Task RemoveAsync(string consumerId) => Task.CompletedTask;

        public Task ClearAsync() => Task.CompletedTask;

        private Task Accept(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Interlocked.Increment(ref _attempts);

            if (Fail)
            {
                // A plain transient fault, not one of the teardown shapes the
                // reporter diverts to its direct-store fallback, so it reaches
                // the swallow-and-log this test is about. Synchronous, so it
                // stays under the pin-pressure shed trigger floor and cannot
                // open a shed window that would suppress the retry for an
                // unrelated reason.
                return Task.FromException(new TimeoutException("durable pin store unavailable"));
            }

            for (var i = 0; i < reports.Count; i++)
            {
                Landed.Add((reports[i].ConsumerId, reports[i].Frontier, reports[i].CheckpointOffset));
            }

            return Task.CompletedTask;
        }
    }
}
