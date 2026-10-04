using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4526: <see cref="ILatticeReplicationAdmin.ForceLiftBootstrapReadFenceAsync"/>
/// is an alarmed operator override. It audits at Warning before dispatching,
/// requires a reason, fails closed without a grain factory, and counts every
/// lift on <see cref="LatticeReplicationMetrics.BootstrapReadFenceForceLifted"/>.
/// </summary>
[TestFixture]
public class LatticeReplicationAdminForceLiftTests
{
    private const string Tree = "orders";

    private sealed class RecordingLogger : ILogger<LatticeReplicationAdmin>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }

    private static (LatticeReplicationAdmin Admin, ILatticeBootstrapCoordinatorGrain Grain, RecordingLogger Log) Create(bool lifted)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = "self" });
        var grain = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        grain.ForceLiftReadFenceAsync(Arg.Any<CancellationToken>()).Returns(lifted);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree, null).Returns(grain);
        var log = new RecordingLogger();
        var admin = new LatticeReplicationAdmin(
            Substitute.For<ILatticeBootstrapCoordinator>(), monitor, log, timeProvider: null, grainFactory: factory);
        return (admin, grain, log);
    }

    private static System.Diagnostics.Metrics.MeterListener Listen(List<long> sink) =>
        Orleans.Lattice.Testing.MeterListening.StartForInstrument(
            LatticeReplicationMetrics.BootstrapReadFenceForceLifted,
            listener => listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree && Equals(tag.Value, Tree))
                    {
                        lock (sink) sink.Add(value);
                        return;
                    }
                }
            }));

    [Test]
    public async Task A_force_lift_is_audited_at_warning_with_its_reason_and_counted()
    {
        var (admin, grain, log) = Create(lifted: true);
        var measurements = new List<long>();
        using var listener = Listen(measurements);

        var lifted = await admin.ForceLiftBootstrapReadFenceAsync(Tree, "source cluster decommissioned");

        await grain.Received(1).ForceLiftReadFenceAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(lifted, Is.True);
            Assert.That(log.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("source cluster decommissioned")), Is.EqualTo(1),
                "the request is audited at Warning, with its reason, before dispatch");
            Assert.That(log.Entries.Count(e => e.Level == LogLevel.Warning), Is.EqualTo(2),
                "the lift itself is recorded at Warning too");
            Assert.That(measurements.Sum(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_force_lift_that_finds_no_fence_changes_nothing_and_is_not_counted()
    {
        var (admin, _, log) = Create(lifted: false);
        var measurements = new List<long>();
        using var listener = Listen(measurements);

        var lifted = await admin.ForceLiftBootstrapReadFenceAsync(Tree, "checking");

        Assert.Multiple(() =>
        {
            Assert.That(lifted, Is.False);
            Assert.That(measurements, Is.Empty);
            Assert.That(log.Entries.Count(e => e.Level == LogLevel.Warning), Is.EqualTo(1), "the attempt is still audited");
        });
    }

    [Test]
    public void A_force_lift_requires_a_reason()
    {
        var (admin, _, _) = Create(lifted: true);

        Assert.ThrowsAsync<ArgumentException>(() => admin.ForceLiftBootstrapReadFenceAsync(Tree, ""));
    }

    [Test]
    public void A_force_lift_fails_closed_without_a_grain_factory()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var admin = new LatticeReplicationAdmin(
            Substitute.For<ILatticeBootstrapCoordinator>(), monitor, new RecordingLogger());

        Assert.ThrowsAsync<InvalidOperationException>(() => admin.ForceLiftBootstrapReadFenceAsync(Tree, "reason"));
    }

    [Test]
    public void A_custom_admin_that_does_not_implement_force_lift_refuses_it()
    {
        ILatticeReplicationAdmin admin = new MinimalAdmin();

        Assert.ThrowsAsync<NotSupportedException>(() => admin.ForceLiftBootstrapReadFenceAsync(Tree, "reason"));
    }

    private sealed class MinimalAdmin : ILatticeReplicationAdmin
    {
        public Task<OperatorReseedDecision> RequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public Task<OperatorReseedDecision> ForceRequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();
    }
}
