using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6017 on the WAL-only usage aggregator: the shared fan-out runs on its
/// own budget rather than its first caller's token, the budget cancels the
/// partition scans it abandons, and a partition whose scan was refused or
/// failed reports its last measured figures - for a bounded age, and only on
/// the same WAL - instead of turning the report partial.
/// </summary>
public sealed partial class LatticeWalUsageGrainTests
{
    private static readonly LatticeOptions UncachedOptions = new() { StorageUsageCacheTtl = TimeSpan.Zero };

    [Test]
    public async Task GetWalUsageAsync_cancelling_one_caller_leaves_the_shared_fan_out_running()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var scanTokens = new List<CancellationToken>();
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns(async call =>
        {
            scanTokens.Add(call.Arg<CancellationToken>());
            var size = await gate.Task;
            return (size, size);
        });
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        var grain = CreateGrain(factory, out _);

        using var callerASource = new CancellationTokenSource();
        var callerA = grain.GetWalUsageAsync(callerASource.Token);
        var callerB = grain.GetWalUsageAsync(CancellationToken.None);

        callerASource.Cancel();
        Assert.That(
            async () => await callerA.WaitAsync(TimeSpan.FromSeconds(5)),
            Throws.InstanceOf<OperationCanceledException>(),
            "a caller that cancels must stop waiting at once");

        gate.SetResult(321L);
        var report = await callerB.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(report.WalRetainedBytes, Is.EqualTo(321L), "the other caller still gets the shared answer");
            Assert.That(scanTokens, Has.Count.EqualTo(1), "the callers share one fan-out");
            Assert.That(scanTokens.All(t => !t.IsCancellationRequested), Is.True,
                "one caller's cancellation must not cancel the scan the others are waiting on");
        });
    }

    [Test]
    public async Task GetWalUsageAsync_budget_expiry_cancels_the_partition_scan_and_counts_it()
    {
        var time = new ManualTimeProvider();
        var budget = TimeSpan.FromSeconds(5);
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        var scanToken = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns(call =>
        {
            var token = call.Arg<CancellationToken>();
            scanToken.TrySetResult(token);
            return HangUntilCancelledAsync(token);
        });
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        var grain = CreateGrain(
            factory,
            out _,
            new LatticeOptions { StorageUsageCacheTtl = TimeSpan.Zero, StorageUsageRollupBudget = budget },
            time);

        long budgetRefusals = 0;
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [LatticeMetrics.StorageUsageScansRefusedName],
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                if (HasTag(tags, LatticeMetrics.TagTree, TreeId) && HasTag(tags, LatticeMetrics.TagReason, "budget"))
                {
                    Interlocked.Add(ref budgetRefusals, value);
                }
            }));

        var pending = grain.GetWalUsageAsync(CancellationToken.None);
        var token = await scanToken.Task.WaitAsync(TimeSpan.FromSeconds(5));
        time.Advance(budget);
        var report = await pending.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(token.IsCancellationRequested, Is.True,
                "the budget must cancel the partition scan it gives up on, so the scan does not run on unobserved");
            Assert.That(report.Partial, Is.True, "a cancelled scan with no earlier figure is unavailable, not zero");
            Assert.That(report.WalRetainedBytes, Is.Zero);
            Assert.That(Interlocked.Read(ref budgetRefusals), Is.EqualTo(1L),
                "the cancelled scan is counted with reason = budget");
        });
    }

    [Test]
    public async Task GetWalUsageAsync_refused_partition_keeps_its_previous_bytes_and_is_not_partial()
    {
        var time = new ManualTimeProvider();
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns((500L, 700L), (-1L, -1L));
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        var grain = CreateGrain(factory, out _, UncachedOptions, time);

        var first = await grain.GetWalUsageAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(1));
        var second = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.WalRetainedBytes, Is.EqualTo(500L), "a refused partition reports its last figure, never zero");
            Assert.That(second.WalPhysicalBytes, Is.EqualTo(700L));
            Assert.That(second.Partial, Is.False, "a carried figure does not make the report partial");
            Assert.That(second.Stale, Is.True, "a carried figure is flagged stale");
            Assert.That(second.OldestMeasuredAtUtc, Is.EqualTo(first.SampledAt));
            Assert.That(first.Stale, Is.False);
            Assert.That(first.OldestMeasuredAtUtc, Is.Null);
        });
    }

    [Test]
    public async Task GetWalUsageAsync_carried_figure_older_than_the_cap_reports_partial()
    {
        var time = new ManualTimeProvider();
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns((500L, 500L), (-1L, -1L), (-1L, -1L));
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        var grain = CreateGrain(factory, out _, UncachedOptions, time);

        await grain.GetWalUsageAsync(CancellationToken.None);
        time.Advance(WalPartitionUsageCarry.MaxCarryAge);
        var atTheCap = await grain.GetWalUsageAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(1));
        var pastTheCap = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(atTheCap.Partial, Is.False, "a figure exactly at the cap is still carried");
            Assert.That(atTheCap.WalRetainedBytes, Is.EqualTo(500L));
            Assert.That(pastTheCap.Partial, Is.True, "a figure older than the cap is no longer carried");
            Assert.That(pastTheCap.Stale, Is.False);
            Assert.That(pastTheCap.WalRetainedBytes, Is.Zero);
        });
    }

    [Test]
    public async Task GetWalUsageAsync_recreated_tree_does_not_inherit_the_previous_bytes()
    {
        var factory = Substitute.For<IGrainFactory>();
        var entry = RegistryEntry(Guid.NewGuid(), WalPlacementPin.Create());
        StubRegistry(factory, () => entry);
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns((500L, 500L), (-1L, -1L));
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        var grain = CreateGrain(factory, out _, UncachedOptions, new ManualTimeProvider());

        await grain.GetWalUsageAsync(CancellationToken.None);

        // Purge and recreate: a new registry row carries a new lineage.
        entry = RegistryEntry(Guid.NewGuid(), WalPlacementPin.Create());
        var recreated = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recreated.WalRetainedBytes, Is.Zero, "a recreated tree must not inherit its predecessor's bytes");
            Assert.That(recreated.Partial, Is.True);
            Assert.That(recreated.Stale, Is.False);
        });
    }

    [Test]
    public async Task GetWalUsageAsync_moved_partition_does_not_inherit_the_previous_bytes()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lineage = Guid.NewGuid();
        var entry = RegistryEntry(lineage, WalPlacementPin.Create());
        StubRegistry(factory, () => entry);
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetByteSizesAsync(Arg.Any<CancellationToken>()).Returns((500L, 500L), (-1L, -1L));
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);
        var grain = CreateGrain(factory, out _, UncachedOptions, new ManualTimeProvider());

        await grain.GetWalUsageAsync(CancellationToken.None);

        // A placement move re-points the partition at another provider.
        entry = RegistryEntry(lineage, WalPlacementPin.Create().WithPartition(0, "other-account", newVersion: 1));
        var moved = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(moved.WalRetainedBytes, Is.Zero, "a moved partition must not report its old backend's bytes");
            Assert.That(moved.Partial, Is.True);
        });
    }

    [TestCase(20, 30, 20)]
    [TestCase(60, 30, 20)]
    [TestCase(0, 30, 20)]
    [TestCase(20, -1, 20)]
    public void ComputeFanOutBudget_takes_the_lesser_of_the_rollup_budget_and_two_thirds_of_the_response_timeout(
        int rollupSeconds, int responseSeconds, int expectedSeconds)
    {
        var response = responseSeconds < 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(responseSeconds);

        var budget = LatticeWalUsageGrain.ComputeFanOutBudget(TimeSpan.FromSeconds(rollupSeconds), response);

        Assert.That(budget, Is.EqualTo(TimeSpan.FromSeconds(expectedSeconds)));
    }

    [Test]
    public void ComputeFanOutBudget_with_neither_bound_is_unbounded()
    {
        Assert.That(LatticeWalUsageGrain.ComputeFanOutBudget(TimeSpan.Zero, Timeout.InfiniteTimeSpan), Is.Null);
    }

    private static async Task<(long Retained, long Physical)> HangUntilCancelledAsync(CancellationToken token)
    {
        await Task.Delay(Timeout.Infinite, token);
        return (0L, 0L);
    }

    private static bool HasTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, string value)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == key && (string?)tag.Value == value)
            {
                return true;
            }
        }

        return false;
    }

    private static TreeRegistryEntry RegistryEntry(Guid lineage, WalPlacementPin pin) => new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 1,
        WalPartitions = 1,
        Lineage = lineage,
        WalPlacement = pin,
    };

    private static void StubRegistry(IGrainFactory factory, Func<TreeRegistryEntry> entry)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(entry()));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
    }
}
