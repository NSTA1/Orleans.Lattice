using NSubstitute;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A tracked remediation or migration waits to be accepted while another alias
/// operation (for example a replication bootstrap copy) holds the tree's alias
/// reservation, instead of failing on a transient refusal (issue #5750). The wait
/// is bounded by a ceiling and cancellable, and any other refusal still fails at once.
/// </summary>
[TestFixture]
public sealed class SchemaRemediationDriverAliasWaitTests
{
    private static readonly TimeSpan[] NoDelay = [TimeSpan.Zero];

    private static readonly LatticeSchemaRemediationReport Accepted =
        LatticeSchemaRemediationReport.InFlight(LatticeSchemaRemediationPhase.DryRun, 0, "orders/remediated/x", "op-1");

    private static LatticeAliasOperationInProgressException Held() => new("orders", "resize:bootstrap");

    [Test]
    public async Task Accept_waits_behind_a_held_alias_reservation_and_then_succeeds()
    {
        var calls = 0;
        Task<LatticeSchemaRemediationReport> Accept() =>
            ++calls < 3 ? Task.FromException<LatticeSchemaRemediationReport>(Held()) : Task.FromResult(Accepted);

        var report = await SchemaRemediationDriver.AcceptAsync(
            "orders", Accept, TimeSpan.FromMinutes(15), logger: null, CancellationToken.None, NoDelay);

        Assert.Multiple(() =>
        {
            Assert.That(report, Is.EqualTo(Accepted));
            Assert.That(calls, Is.EqualTo(3));
        });
    }

    [Test]
    public void Accept_fails_with_the_refusal_once_the_wait_would_pass_its_ceiling()
    {
        var calls = 0;
        Task<LatticeSchemaRemediationReport> Accept()
        {
            calls++;
            return Task.FromException<LatticeSchemaRemediationReport>(Held());
        }

        var refused = Assert.ThrowsAsync<LatticeAliasOperationInProgressException>(() =>
            SchemaRemediationDriver.AcceptAsync(
                "orders", Accept, TimeSpan.FromSeconds(30), logger: null, CancellationToken.None,
                [TimeSpan.FromSeconds(20)]));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2), "one wait fits the ceiling; the next would pass it");
            Assert.That(refused!.Message, Does.Contain("orders").And.Contain("resize:bootstrap"));
        });
    }

    [Test]
    public void Accept_with_a_zero_ceiling_fails_on_the_first_refusal_without_waiting()
    {
        var calls = 0;
        Task<LatticeSchemaRemediationReport> Accept()
        {
            calls++;
            return Task.FromException<LatticeSchemaRemediationReport>(Held());
        }

        Assert.ThrowsAsync<LatticeAliasOperationInProgressException>(() =>
            SchemaRemediationDriver.AcceptAsync("orders", Accept, TimeSpan.Zero, logger: null, CancellationToken.None, NoDelay));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void Cancelling_the_operation_ends_the_wait()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            () => SchemaRemediationDriver.AcceptAsync(
                "orders", () => Task.FromException<LatticeSchemaRemediationReport>(Held()), TimeSpan.FromMinutes(15),
                logger: null, cancelled.Token, [TimeSpan.FromMinutes(1)]),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void Any_other_refusal_fails_at_once()
    {
        var calls = 0;
        Task<LatticeSchemaRemediationReport> Accept()
        {
            calls++;
            return Task.FromException<LatticeSchemaRemediationReport>(new InvalidOperationException("not versioned"));
        }

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            SchemaRemediationDriver.AcceptAsync(
                "orders", Accept, TimeSpan.FromMinutes(15), logger: null, CancellationToken.None, NoDelay));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task The_wait_is_reported_once_on_entering_it_not_once_per_refusal()
    {
        var calls = 0;
        Task<LatticeSchemaRemediationReport> Accept() =>
            ++calls < 4 ? Task.FromException<LatticeSchemaRemediationReport>(Held()) : Task.FromResult(Accepted);
        var progress = new RecordingProgress();

        await SchemaRemediationDriver.AcceptAsync(
            "orders", Accept, TimeSpan.FromMinutes(15), logger: null, CancellationToken.None, NoDelay, progress);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(4), "precondition: three refusals");
            Assert.That(progress.Phases, Is.EqualTo(new[] { SchemaOperationPhases.WaitingForAliasOperation }),
                "issue #5812: the status says why the operation has not started, written once");
        });
    }

    [Test]
    public async Task An_accept_that_is_not_refused_reports_no_wait()
    {
        var progress = new RecordingProgress();

        await SchemaRemediationDriver.AcceptAsync(
            "orders", () => Task.FromResult(Accepted), TimeSpan.FromMinutes(15), logger: null, CancellationToken.None,
            NoDelay, progress);

        Assert.That(progress.Phases, Is.Empty);
    }

    [Test]
    public async Task Driving_the_accepted_remediation_replaces_the_wait_with_its_first_phase()
    {
        var calls = 0;
        Task<LatticeSchemaRemediationReport> Accept() =>
            ++calls < 2 ? Task.FromException<LatticeSchemaRemediationReport>(Held()) : Task.FromResult(Accepted);
        var progress = new RecordingProgress();
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        grain.RunSliceAsync("op-1").Returns(new SchemaRemediationSlice(
            LatticeSchemaRemediationReport.Completed(5, "orders/remediated/x", "op-1"), null));

        var accepted = await SchemaRemediationDriver.AcceptAsync(
            "orders", Accept, TimeSpan.FromMinutes(15), logger: null, CancellationToken.None, NoDelay, progress);
        await SchemaRemediationDriver.DriveAsync(grain, accepted, progress, CancellationToken.None);

        Assert.That(progress.Phases.Take(2),
            Is.EqualTo(new[] { SchemaOperationPhases.WaitingForAliasOperation, SchemaOperationPhases.DryRun }),
            "the wait is left by the remediation's first phase");
    }

    [Test]
    public void The_wait_ceiling_defaults_to_fifteen_minutes()
    {
        Assert.That(new LatticeSchemaEnforcementOptions().AliasReservationWaitTimeout, Is.EqualTo(TimeSpan.FromMinutes(15)));
    }

    private sealed class RecordingProgress : ILatticeOperationProgress
    {
        public List<string> Phases { get; } = [];

        public ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null)
        {
            Phases.Add(phase);
            return ValueTask.CompletedTask;
        }
    }
}
