using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the bootstrap progress observability surface: the
/// three <c>orleans.lattice.replication.bootstrap.*</c> instruments
/// and the structured phase-transition <see cref="LogLevel.Information"/>
/// logs emitted by <see cref="LatticeBootstrapCoordinatorGrain"/>.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    /// <summary>
    /// Builds a coordinator grain with an NSubstitute-backed logger so
    /// tests can assert on phase-transition log calls. Mirrors the
    /// default <see cref="Create"/> factory but swaps the logger.
    /// </summary>
    private static (
        LatticeBootstrapCoordinatorGrain Grain,
        FakePersistentState<BootstrapCoordinatorState> State,
        IBootstrapSnapshotSource Provider,
        IReplicationApplier Apply,
        IReplicationHighWaterMarkGrain Hwm,
        ILogger<LatticeBootstrapCoordinatorGrain> Logger) CreateWithLogger(
            FakePersistentState<BootstrapCoordinatorState>? existingState = null,
            string treeName = Tree)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("bootstrap-coordinator", treeName));
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<IBootstrapSnapshotSource>();
        var reminders = Substitute.For<IReminderRegistry>();
        var apply = Substitute.For<IReplicationApplier>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new ApplyResult
            {
                Applied = true,
                HighWaterMark = ((WalRecord)call[0]).Timestamp,
            }));
        var logger = Substitute.For<ILogger<LatticeBootstrapCoordinatorGrain>>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        var fakeState = existingState ?? new FakePersistentState<BootstrapCoordinatorState>();
        var grain = new LatticeBootstrapCoordinatorGrain(
            context, factory, provider, apply, reminders, logger, fakeState);
        return (grain, fakeState, provider, apply, hwm, logger);
    }

    // --- bootstrap.entries_received counter ---

    [Test]
    public async Task Drain_increments_entries_received_counter_per_applied_entry()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapEntriesReceivedName);

        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, _, _) = Create(fake);
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2, 3 }, Timestamp = Hlc(2) },
            new SnapshotEntry { Key = "c", Value = new byte[] { 4, 5, 6 }, Timestamp = Hlc(3) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        var measurements = collector.Measurements;
        Assert.That(measurements, Has.Count.EqualTo(3));
        Assert.That(measurements.Sum(m => m.Value), Is.EqualTo(3L));
        foreach (var m in measurements)
        {
            Assert.That(m.Value, Is.EqualTo(1L));
            Assert.That(m.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == LatticeReplicationMetrics.TagTree && (string?)t.Value == Tree));
            Assert.That(m.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == LatticeReplicationMetrics.TagOrigin && (string?)t.Value == SourceCluster));
        }
    }

    [Test]
    public async Task Drain_with_no_entries_does_not_increment_entries_received_counter()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapEntriesReceivedName);

        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, _, _) = Create(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream())));

        await grain.ProcessNextPhaseAsync();

        Assert.That(collector.Measurements, Is.Empty);
    }

    // --- bootstrap.bytes_received counter ---

    [Test]
    public async Task Drain_increments_bytes_received_counter_by_value_length()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapBytesReceivedName);

        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, _, provider, _, _, _) = Create(fake);
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[1], Timestamp = Hlc(1) },
            new SnapshotEntry { Key = "b", Value = new byte[2], Timestamp = Hlc(2) },
            new SnapshotEntry { Key = "c", Value = new byte[3], Timestamp = Hlc(3) },
        };
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream(entries))));

        await grain.ProcessNextPhaseAsync();

        var measurements = collector.Measurements;
        Assert.That(measurements, Has.Count.EqualTo(3));
        Assert.That(measurements.Sum(m => m.Value), Is.EqualTo(6L));
        foreach (var m in measurements)
        {
            Assert.That(m.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == LatticeReplicationMetrics.TagTree && (string?)t.Value == Tree));
            Assert.That(m.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == LatticeReplicationMetrics.TagOrigin && (string?)t.Value == SourceCluster));
        }
    }

    // --- bootstrap.duration histogram on terminal transitions ---

    [Test]
    public async Task Successful_PinAndComplete_records_duration_histogram_with_outcome_live()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapDurationName);

        // Drive a full kickoff -> drain -> pin sequence so the
        // duration anchor is set on kickoff and the terminal record
        // fires from PinAndCompleteAsync.
        var (grain, _, _, provider, _, _, _) = Create();
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream())));

        await grain.TryInitiateBootstrapAsync(SourceCluster);
        await grain.ProcessNextPhaseAsync(); // drain -> IncrementalHandoff
        await grain.ProcessNextPhaseAsync(); // pin -> LiveIncremental + record

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagTree && (string?)t.Value == Tree));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagOrigin && (string?)t.Value == SourceCluster));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagOutcome
            && (string?)t.Value == LatticeReplicationMetrics.BootstrapOutcomeLive));
    }

    [Test]
    public async Task Failed_transition_records_duration_histogram_with_outcome_failed()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapDurationName);

        var (grain, _, _, provider, _, apply, _) = Create();
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1) }))));
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("apply boom"));

        await grain.TryInitiateBootstrapAsync(SourceCluster);
        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.GreaterThanOrEqualTo(0.0));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == LatticeReplicationMetrics.TagOutcome
            && (string?)t.Value == LatticeReplicationMetrics.BootstrapOutcomeFailed));
    }

    [Test]
    public async Task Duration_histogram_records_at_most_once_per_terminal_transition()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.BootstrapDurationName);

        var (grain, _, _, provider, _, _, _) = Create();
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream())));

        await grain.TryInitiateBootstrapAsync(SourceCluster);
        await grain.ProcessNextPhaseAsync();
        await grain.ProcessNextPhaseAsync(); // terminal pin emits histogram
        await grain.ProcessNextPhaseAsync(); // re-pump in LiveIncremental is a no-op

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
    }

    // --- phase-transition structured logs ---

    [Test]
    public async Task Kickoff_logs_Idle_to_RequestingSnapshot_transition_at_information()
    {
        var (grain, _, _, _, _, logger) = CreateWithLogger();

        await grain.TryInitiateBootstrapAsync(SourceCluster);

        logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(state => state.ToString()!.Contains("Idle -> RequestingSnapshot")
                && state.ToString()!.Contains(Tree)
                && state.ToString()!.Contains(SourceCluster)),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task Drain_logs_RequestingSnapshot_to_ApplyingSnapshot_transition()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, provider, _, _, logger) = CreateWithLogger(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream())));

        await grain.ProcessNextPhaseAsync();

        logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(state => state.ToString()!.Contains("RequestingSnapshot -> ApplyingSnapshot")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task Drain_does_not_re_log_ApplyingSnapshot_pivot_on_crash_resume()
    {
        // A reactivation that finds Phase=ApplyingSnapshot in
        // persistent state should not re-emit the pivot log: the
        // pivot already happened on the original activation. Verifies
        // the pivotedToApplying guard.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.ApplyingSnapshot);
        var (grain, _, provider, _, _, logger) = CreateWithLogger(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream())));

        await grain.ProcessNextPhaseAsync();

        logger.DidNotReceive().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(state => state.ToString()!.Contains("RequestingSnapshot -> ApplyingSnapshot")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task Drain_logs_ApplyingSnapshot_to_IncrementalHandoff_at_end_of_stream()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake);
        var (grain, _, provider, _, _, logger) = CreateWithLogger(fake);
        provider.ExportAsync(Tree, SourceCluster, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(), Stream())));

        await grain.ProcessNextPhaseAsync();

        logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(state => state.ToString()!.Contains("ApplyingSnapshot -> IncrementalHandoff")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task PinAndComplete_logs_IncrementalHandoff_to_LiveIncremental()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        var (grain, _, _, _, _, logger) = CreateWithLogger(fake);

        await grain.ProcessNextPhaseAsync();

        logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(state => state.ToString()!.Contains("IncrementalHandoff -> LiveIncremental")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task Failed_transition_logs_phase_to_Failed()
    {
        var (grain, _, provider, apply, _, logger) = CreateWithLogger();
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector(),
                Stream(new SnapshotEntry { Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1) }))));
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));

        await grain.TryInitiateBootstrapAsync(SourceCluster);
        Assert.That(
            async () => await grain.ProcessNextPhaseAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        logger.Received().Log(
            LogLevel.Information,
            Arg.Any<EventId>(),
            Arg.Is<object>(state => state.ToString()!.Contains("-> Failed")),
            Arg.Any<Exception?>(),
            Arg.Any<Func<object, Exception?, string>>());
    }
}
