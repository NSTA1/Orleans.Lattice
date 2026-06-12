using System.Diagnostics.Metrics;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class RemediationGuardTests
{
    private const long Minute = TimeSpan.TicksPerMinute;

    private static string UniqueTree() => "tree-" + Guid.NewGuid().ToString("N");

    [Test]
    public void TryBeginRemediation_first_pass_in_fresh_window_is_allowed()
    {
        var guard = new RemediationGuard();

        Assert.That(guard.TryBeginRemediation("peer", windowBudget: 3, windowTicks: Minute, nowTicks: 0), Is.True);
    }

    [Test]
    public void TryBeginRemediation_blocks_once_budget_is_consumed_within_window()
    {
        var guard = new RemediationGuard();

        Assert.That(guard.TryBeginRemediation("peer", 3, Minute, 0), Is.True);
        guard.RecordEntriesShipped("peer", 3);

        Assert.That(guard.TryBeginRemediation("peer", 3, Minute, 1000), Is.False);
    }

    [Test]
    public void TryBeginRemediation_allows_again_after_window_rolls_over()
    {
        var guard = new RemediationGuard();

        guard.TryBeginRemediation("peer", 3, Minute, 0);
        guard.RecordEntriesShipped("peer", 5);
        Assert.That(guard.TryBeginRemediation("peer", 3, Minute, 1000), Is.False);

        Assert.That(guard.TryBeginRemediation("peer", 3, Minute, Minute + 1), Is.True);
    }

    [Test]
    public void RecordEntriesShipped_ignores_non_positive_counts()
    {
        var guard = new RemediationGuard();

        guard.TryBeginRemediation("peer", 1, Minute, 0);
        guard.RecordEntriesShipped("peer", 0);

        // Budget of 1 with zero consumed still permits the next pass.
        Assert.That(guard.TryBeginRemediation("peer", 1, Minute, 10), Is.True);
    }

    [Test]
    public void RecordFailure_opens_circuit_at_threshold()
    {
        var guard = new RemediationGuard();

        Assert.That(guard.RecordFailure("peer", failureThreshold: 3, nowTicks: 0), Is.False);
        Assert.That(guard.RecordFailure("peer", 3, 0), Is.False);
        Assert.That(guard.RecordFailure("peer", 3, 0), Is.True);
    }

    [Test]
    public void RecordFailure_with_threshold_one_opens_on_first_failure()
    {
        var guard = new RemediationGuard();

        Assert.That(guard.RecordFailure("peer", failureThreshold: 1, nowTicks: 0), Is.True);
    }

    [Test]
    public void IsCircuitBlocking_is_true_while_cooling_and_false_after_cooldown()
    {
        var guard = new RemediationGuard();
        guard.RecordFailure("peer", failureThreshold: 1, nowTicks: 0);

        Assert.Multiple(() =>
        {
            Assert.That(guard.IsCircuitBlocking("peer", cooldownTicks: Minute, nowTicks: Minute / 2), Is.True);
            Assert.That(guard.IsCircuitBlocking("peer", cooldownTicks: Minute, nowTicks: Minute + 1), Is.False);
        });
    }

    [Test]
    public void IsCircuitBlocking_is_false_for_unknown_peer()
    {
        var guard = new RemediationGuard();

        Assert.That(guard.IsCircuitBlocking("never-seen", Minute, 0), Is.False);
    }

    [Test]
    public void RecordSuccess_resets_failures_and_closes_circuit()
    {
        var guard = new RemediationGuard();
        guard.RecordFailure("peer", failureThreshold: 1, nowTicks: 0);
        Assert.That(guard.IsCircuitBlocking("peer", Minute, 0), Is.True);

        guard.RecordSuccess("peer");

        Assert.That(guard.IsCircuitBlocking("peer", Minute, 0), Is.False);
    }

    [Test]
    public void RecordFailure_during_half_open_refreshes_cooldown()
    {
        var guard = new RemediationGuard();
        guard.RecordFailure("peer", failureThreshold: 1, nowTicks: 0);

        // Cooldown elapsed: half-open trial allowed.
        Assert.That(guard.IsCircuitBlocking("peer", Minute, Minute + 1), Is.False);

        // Trial fails: breaker re-opens with cooldown anchored at the trial time.
        Assert.That(guard.RecordFailure("peer", 1, Minute + 1), Is.True);
        Assert.That(guard.IsCircuitBlocking("peer", Minute, Minute + 2), Is.True);
    }

    [Test]
    public void Accounting_is_isolated_per_peer()
    {
        var guard = new RemediationGuard();

        guard.RecordFailure("peer-a", failureThreshold: 1, nowTicks: 0);

        Assert.Multiple(() =>
        {
            Assert.That(guard.IsCircuitBlocking("peer-a", Minute, 0), Is.True);
            Assert.That(guard.IsCircuitBlocking("peer-b", Minute, 0), Is.False);
        });
    }

    [Test]
    public void PublishDisabled_reports_gauge_value_one_with_reason_tag()
    {
        // Construct a guard first so the process-wide gauge is registered before
        // the collector subscribes.
        _ = new RemediationGuard();
        var tree = UniqueTree();
        const string peer = "site-b";

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DigestRemediationDisabledName);

        RemediationGuard.PublishDisabled(tree, peer, RemediationDisabledReason.CircuitOpen);
        collector.RecordObservableInstruments();

        var measurement = collector.Measurements.Single(m =>
            m.Tags.Any(t => t.Key == "tree" && (string?)t.Value == tree));

        Assert.Multiple(() =>
        {
            Assert.That(measurement.Value, Is.EqualTo(1L));
            Assert.That(measurement.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "peer" && (string?)t.Value == peer));
            Assert.That(measurement.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "circuit_open"));
        });

        RemediationGuard.ClearDisabled(tree, peer);
    }

    [Test]
    public void ClearDisabled_removes_the_gauge_series()
    {
        _ = new RemediationGuard();
        var tree = UniqueTree();
        const string peer = "site-c";

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DigestRemediationDisabledName);

        RemediationGuard.PublishDisabled(tree, peer, RemediationDisabledReason.OptOut);
        RemediationGuard.ClearDisabled(tree, peer);
        collector.RecordObservableInstruments();

        Assert.That(
            collector.Measurements.Where(m =>
                m.Tags.Any(t => t.Key == "tree" && (string?)t.Value == tree)),
            Is.Empty);
    }

    [Test]
    public void PublishDisabled_overwrites_reason_for_same_tree_peer()
    {
        _ = new RemediationGuard();
        var tree = UniqueTree();
        const string peer = "site-d";

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DigestRemediationDisabledName);

        RemediationGuard.PublishDisabled(tree, peer, RemediationDisabledReason.OptOut);
        RemediationGuard.PublishDisabled(tree, peer, RemediationDisabledReason.BudgetExhausted);
        collector.RecordObservableInstruments();

        var measurement = collector.Measurements.Single(m =>
            m.Tags.Any(t => t.Key == "tree" && (string?)t.Value == tree));

        Assert.That(measurement.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "reason" && (string?)t.Value == "budget_exhausted"));

        RemediationGuard.ClearDisabled(tree, peer);
    }

    [Test]
    public void Null_arguments_throw()
    {
        var guard = new RemediationGuard();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => guard.TryBeginRemediation(null!, 1, Minute, 0));
            Assert.Throws<ArgumentNullException>(() => guard.RecordEntriesShipped(null!, 1));
            Assert.Throws<ArgumentNullException>(() => guard.IsCircuitBlocking(null!, Minute, 0));
            Assert.Throws<ArgumentNullException>(() => guard.RecordSuccess(null!));
            Assert.Throws<ArgumentNullException>(() => guard.RecordFailure(null!, 1, 0));
            Assert.Throws<ArgumentNullException>(() => RemediationGuard.PublishDisabled(null!, "p", RemediationDisabledReason.OptOut));
            Assert.Throws<ArgumentNullException>(() => RemediationGuard.PublishDisabled("t", null!, RemediationDisabledReason.OptOut));
            Assert.Throws<ArgumentNullException>(() => RemediationGuard.ClearDisabled(null!, "p"));
            Assert.Throws<ArgumentNullException>(() => RemediationGuard.ClearDisabled("t", null!));
        });
    }
}
