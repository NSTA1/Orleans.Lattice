using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalSaturationSamplerDrainLagTests
{
    private WalSaturationSampler CreateHolderLogSampler(
        LatticeOptions options, ManualTimeProvider time, CapturingLogger logger)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        return new WalSaturationSampler(
            NewSignal(),
            new WalSaturationObserverDispatcher(
                Array.Empty<IWalSaturationObserver>(),
                NullLogger<WalSaturationObserverDispatcher>.Instance),
            monitor, logger, time, _cursors);
    }

    [Test]
    public async Task Holder_log_default_repeats_at_interval_and_resets_after_recovery()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(_headTicks, TimeSpan.Zero));
        var logger = new CapturingLogger();
        var sampler = CreateHolderLogSampler(new LatticeOptions
        {
            WalDrainLagConsumerFreshness = TimeSpan.Zero,
        }, time, logger);
        SetLevel(TimeSpan.FromHours(1));
        SetConsumers(("first-holder", TimeSpan.FromHours(1)));

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(1), "the edge logs immediately, before classification windows elapse");
        time.Advance(TimeSpan.FromMinutes(10) - TimeSpan.FromTicks(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(1), "not before the default interval");

        SetConsumers(("new-holder", TimeSpan.FromHours(1)));
        time.Advance(TimeSpan.FromTicks(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(2), "a standing breach must be re-attributed at ten minutes");
        Assert.That(logger.Lines[1].Message, Does.Contain("new-holder").And.Not.Contain("first-holder"));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(2), "no repeated emission at the same instant");

        SetLevel(TimeSpan.Zero);
        time.Advance(TimeSpan.FromMinutes(10));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(2), "recovered trees stop logging");
        SetLevel(TimeSpan.FromHours(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(3), "a new crossing must not wait for an old interval");
        time.Advance(TimeSpan.FromMinutes(9));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(3));
    }

    [TestCase(1, false)]
    [TestCase(2, false)]
    [TestCase(3, false)]
    [TestCase(4, false)]
    [TestCase(4, true)]
    public async Task Holder_log_names_up_to_three_eligible_consumers_in_cursor_order(int eligibleCount, bool ascending)
    {
        var time = new ManualTimeProvider(new DateTimeOffset(_headTicks, TimeSpan.Zero));
        var logger = new CapturingLogger();
        var sampler = CreateHolderLogSampler(new LatticeOptions(), time, logger);
        SetLevel(TimeSpan.FromHours(1));
        var snapshots = new List<WalCursorSnapshot>
        {
            new("never-reported", HybridLogicalClock.Zero, _headTicks),
            new("cold", At(time.GetUtcNow() - TimeSpan.FromHours(3)), _headTicks - TimeSpan.FromMinutes(6).Ticks),
            new(LeafConsumerId("position-stale"), At(time.GetUtcNow() - TimeSpan.FromHours(2)), _headTicks)
            {
                CursorAdvancedAtTicks = _headTicks - TimeSpan.FromMinutes(6).Ticks,
            },
        };
        for (var index = 0; index < eligibleCount; index++)
        {
            var rank = ascending ? index + 1 : eligibleCount - index;
            snapshots.Add(new WalCursorSnapshot(
                $"holder-{rank}",
                new HybridLogicalClock { WallClockTicks = _headTicks - TimeSpan.FromHours(1).Ticks, Counter = rank },
                _headTicks - TimeSpan.FromSeconds(rank).Ticks)
            {
                CursorAdvancedAtTicks = rank == 1 ? null : _headTicks - TimeSpan.FromSeconds(rank * 2).Ticks,
            });
        }
        _cursors.SnapshotAsync(_treeId, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(snapshots));

        await sampler.SampleOnceAsync(CancellationToken.None);

        var expectedCount = Math.Min(3, eligibleCount);
        Assert.That(logger.Lines, Has.Count.EqualTo(expectedCount));
        for (var index = 0; index < expectedCount; index++)
        {
            var rank = index + 1;
            var line = logger.Lines[index];
            Assert.Multiple(() =>
            {
                Assert.That(line.Level, Is.EqualTo(LogLevel.Warning));
                Assert.That(line.Fields["ConsumerId"], Is.EqualTo($"holder-{rank}"));
                Assert.That(line.Fields["Cursor"], Is.EqualTo(snapshots.Single(s => s.ConsumerId == $"holder-{rank}").Cursor));
                Assert.That(line.Fields["LaggingConsumers"], Is.EqualTo(eligibleCount));
                Assert.That(line.Fields["ReportAgeSeconds"], Is.EqualTo((double)rank));
                Assert.That(line.Fields["PositionAgeSeconds"], Is.EqualTo(rank == 1 ? null : (double?)(rank * 2)));
                Assert.That(line.Fields["ObservedAtUtc"], Is.EqualTo(time.GetUtcNow()));
                Assert.That(line.Fields["HolderRank"], Is.EqualTo(rank));
                Assert.That(line.Fields["HolderCount"], Is.EqualTo(expectedCount));
                Assert.That(line.Fields["ObservationId"], Is.EqualTo(logger.Lines[0].Fields["ObservationId"]));
            });
        }
    }

    [Test]
    public async Task Holder_log_null_interval_logs_only_on_each_crossing()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(_headTicks, TimeSpan.Zero));
        var logger = new CapturingLogger();
        var sampler = CreateHolderLogSampler(new LatticeOptions
        {
            WalDrainLagHolderLogInterval = null,
            WalDrainLagConsumerFreshness = TimeSpan.Zero,
        }, time, logger);
        SetLevel(TimeSpan.FromHours(1));
        SetConsumers(("holder", TimeSpan.FromHours(1)));
        await sampler.SampleOnceAsync(CancellationToken.None);
        for (var index = 0; index < 3; index++)
        {
            time.Advance(TimeSpan.FromHours(1));
            await sampler.SampleOnceAsync(CancellationToken.None);
        }
        Assert.That(logger.Lines, Has.Count.EqualTo(1));
        SetLevel(TimeSpan.Zero);
        await sampler.SampleOnceAsync(CancellationToken.None);
        SetLevel(TimeSpan.FromHours(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Holder_log_null_interval_does_not_log_a_late_snapshot_without_a_new_crossing()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(_headTicks, TimeSpan.Zero));
        var logger = new CapturingLogger();
        var sampler = CreateHolderLogSampler(new LatticeOptions
        {
            WalDrainLagHolderLogInterval = null,
            WalDrainLagConsumerFreshness = TimeSpan.Zero,
        }, time, logger);
        SetLevel(TimeSpan.FromHours(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Is.Empty);
        SetConsumers(("late-holder", TimeSpan.FromHours(1)));
        time.Advance(TimeSpan.FromHours(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Is.Empty, "null preserves the original crossing predicate, not merely the first successful log");
        SetLevel(TimeSpan.Zero);
        await sampler.SampleOnceAsync(CancellationToken.None);
        SetLevel(TimeSpan.FromHours(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Holder_log_custom_interval_is_independent_per_tree_and_disable_clears_it()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(_headTicks, TimeSpan.Zero));
        var logger = new CapturingLogger();
        var options = new LatticeOptions
        {
            WalDrainLagHolderLogInterval = TimeSpan.FromMinutes(2),
            WalDrainLagConsumerFreshness = TimeSpan.Zero,
        };
        var sampler = CreateHolderLogSampler(options, time, logger);
        SetLevel(TimeSpan.FromHours(1));
        SetConsumers(("holder", TimeSpan.FromHours(1)));
        await sampler.SampleOnceAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(1));
        var otherTree = _treeId + "-other";
        SetLevel(TimeSpan.FromHours(1), otherTree);
        _cursors.SnapshotAsync(otherTree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(new[]
            {
                new WalCursorSnapshot("other-holder", At(time.GetUtcNow() - TimeSpan.FromHours(1)), _headTicks),
            }));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines.Select(l => l.Fields["TreeId"]), Is.EqualTo(new[] { _treeId, otherTree }));
        time.Advance(TimeSpan.FromMinutes(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines.Select(l => l.Fields["TreeId"]), Is.EqualTo(new[] { _treeId, otherTree, _treeId }));

        options.WalSaturationMaterialiserLagThreshold = null;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Has.Count.EqualTo(3));
        options.WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(30);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines.Skip(3).Select(l => l.Fields["TreeId"]), Is.EquivalentTo(new[] { _treeId, otherTree }));
    }

    [Test]
    public async Task Holder_log_snapshot_with_no_eligible_holder_does_not_invent_an_identity()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(_headTicks, TimeSpan.Zero));
        var logger = new CapturingLogger();
        var sampler = CreateHolderLogSampler(new LatticeOptions(), time, logger);
        SetLevel(TimeSpan.FromHours(1));
        SetConsumers(("never-reported", NeverReported));
        await sampler.SampleOnceAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(10));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(logger.Lines, Is.Empty, "the snapshot can change after the minimum read; do not name a default holder");
        await _cursors.Received(2).SnapshotAsync(_treeId, Arg.Any<CancellationToken>());
    }
}
