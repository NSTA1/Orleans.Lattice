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
            Assert.That(report, Is.SameAs(Accepted));
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
                logger: null, cancelled.Token, [TimeSpan.FromHours(1)]),
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
    public void The_wait_ceiling_defaults_to_fifteen_minutes()
    {
        Assert.That(new LatticeSchemaEnforcementOptions().AliasReservationWaitTimeout, Is.EqualTo(TimeSpan.FromMinutes(15)));
    }
}
