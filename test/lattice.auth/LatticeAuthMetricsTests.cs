using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeAuthMetrics"/> instruments on the
/// <c>orleans.lattice.auth</c> meter: the canonical meter name, the effect tag
/// helper, the snapshot-rebuild counter, and the compiled-snapshot epoch / age
/// observable gauges. Each counter is asserted through a
/// <see cref="System.Diagnostics.Metrics.MeterListener"/> so the wiring - not just the method - is covered.
/// </summary>
[TestFixture]
public sealed class LatticeAuthMetricsTests
{
    [Test]
    public void Meter_name_is_the_canonical_auth_meter()
    {
        Assert.That(LatticeAuthMetrics.MeterName, Is.EqualTo("orleans.lattice.auth"));
        Assert.That(LatticeAuthMetrics.Meter.Name, Is.EqualTo("orleans.lattice.auth"));
    }

    [Test]
    public void EffectTag_maps_allowed_and_denied_to_the_canonical_values()
    {
        Assert.That(LatticeAuthMetrics.EffectTag(true), Is.EqualTo(LatticeAuthMetrics.EffectAllow));
        Assert.That(LatticeAuthMetrics.EffectTag(false), Is.EqualTo(LatticeAuthMetrics.EffectDeny));
        Assert.That(LatticeAuthMetrics.EffectAllow, Is.EqualTo("allow"));
        Assert.That(LatticeAuthMetrics.EffectDeny, Is.EqualTo("deny"));
    }

    [Test]
    public async Task Snapshot_rebuild_increments_the_rebuild_counter()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.SnapshotRebuildsName);
        var maintainer = new CompiledPolicySnapshotMaintainer(
            new EmptyPolicyStore(), NullLogger<CompiledPolicySnapshotMaintainer>.Instance);

        await maintainer.RebuildNowAsync();

        Assert.That(collector.Measurements.Select(m => m.Value).Sum(), Is.GreaterThanOrEqualTo(1),
            "a snapshot rebuild must be counted on the auth meter");
    }

    [Test]
    public async Task Snapshot_epoch_gauge_reports_the_live_maintainer_epoch()
    {
        var maintainer = new CompiledPolicySnapshotMaintainer(
            new EmptyPolicyStore(), NullLogger<CompiledPolicySnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();
        var epoch = maintainer.CurrentEpoch;

        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.SnapshotEpochName);
        collector.RecordObservableInstruments();

        Assert.That(collector.Measurements.Select(m => m.Value), Does.Contain(epoch),
            "the epoch gauge must report the live maintainer's current epoch");
        GC.KeepAlive(maintainer);
    }

    [Test]
    public async Task Snapshot_age_gauge_reports_a_non_negative_measurement()
    {
        var maintainer = new CompiledPolicySnapshotMaintainer(
            new EmptyPolicyStore(), NullLogger<CompiledPolicySnapshotMaintainer>.Instance);
        await maintainer.RebuildNowAsync();

        using var collector = new MeterCollector<double>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.SnapshotAgeName);
        collector.RecordObservableInstruments();

        Assert.That(collector.Measurements, Is.Not.Empty, "the age gauge must report at least one live maintainer");
        Assert.That(collector.Measurements.Select(m => m.Value), Has.All.GreaterThanOrEqualTo(0d));
        GC.KeepAlive(maintainer);
    }

    /// <summary>A policy store that yields no rules.</summary>
    private sealed class EmptyPolicyStore : ILatticeAuthorizationPolicyStore
    {
        public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult<LatticeAuthorizationRule?>(null);

        public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

#pragma warning disable CS1998 // async iterator with no await
        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(
            string treeId,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield break;
        }

        public async IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield break;
        }
#pragma warning restore CS1998
    }
}
