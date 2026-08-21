using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Add-only branch coverage for <see cref="LatticeOptionsValidator"/> targeting the
/// guard clauses beyond the admission-advisory options (materialiser, snapshot, WAL,
/// and saturation-classifier knobs) that the original test suite does not exercise.
/// Each case mutates one field of an otherwise-valid <see cref="LatticeOptions"/> to
/// an out-of-range value and asserts the validator fails naming that field.
/// </summary>
[TestFixture]
public class LatticeOptionsValidatorBranchTests
{
    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        return new LatticeOptionsValidator().Validate(null, options);
    }

    private static readonly TimeSpan NegOne = TimeSpan.FromSeconds(-1);

    private static IEnumerable<TestCaseData> InvalidCases()
    {
        yield return Case("MaxLeafReplayEntries", o => o.MaxLeafReplayEntries = 0);
        yield return Case("MaterialiserCheckpointEntries", o => o.MaterialiserCheckpointEntries = 0);
        yield return Case("MaterialiserCheckpointInterval", o => o.MaterialiserCheckpointInterval = NegOne);
        yield return Case("LeafProjectionRetention", o => o.LeafProjectionRetention = TimeSpan.Zero);
        yield return Case("ProjectionRebuildPolicy", o => o.ProjectionRebuildPolicy = (ProjectionRebuildPolicy)999);
        yield return Case("LeafSnapshotMargin", o => o.LeafSnapshotMargin = 2.0);
        yield return Case("LeafSnapshotReClassifyEveryNCheckpoints", o => o.LeafSnapshotReClassifyEveryNCheckpoints = -1);
        yield return Case("WalMaxPendingBatches", o => o.WalMaxPendingBatches = 0);
        yield return Case("MaxSnapshotReplayEntries", o => o.MaxSnapshotReplayEntries = 0);
        yield return Case("WalPartitions", o => o.WalPartitions = 0);
        yield return Case("SnapshotLeafIdleTtl", o => o.SnapshotLeafIdleTtl = TimeSpan.Zero);
        yield return Case("SnapshotBaselineTtl", o => o.SnapshotBaselineTtl = TimeSpan.Zero);
        yield return Case("WalFlushTimeout", o => o.WalFlushTimeout = TimeSpan.Zero);
        yield return Case("ShardForwardTimeout", o => o.ShardForwardTimeout = TimeSpan.Zero);
        yield return Case("ActivationReadyTimeout", o => o.ActivationReadyTimeout = TimeSpan.Zero);
        yield return Case("DigestPublishTimeout", o => o.DigestPublishTimeout = TimeSpan.Zero);
        yield return Case("WalAppendDispatchTimeout", o => o.WalAppendDispatchTimeout = TimeSpan.Zero);
        yield return Case("WalFlushPreflightTimeout", o => o.WalFlushPreflightTimeout = TimeSpan.Zero);
        yield return Case("WalDrainBudget", o => o.WalDrainBudget = TimeSpan.Zero);
        yield return Case("WalSaturationSampleInterval", o => o.WalSaturationSampleInterval = TimeSpan.Zero);
        yield return Case("WalSaturationThrottledRatio", o => o.WalSaturationThrottledRatio = 2.0);
        yield return Case("WalSaturationDispatchTimeoutThreshold", o => o.WalSaturationDispatchTimeoutThreshold = 0);
        yield return Case("WalSaturationProviderFailureRateThreshold", o => o.WalSaturationProviderFailureRateThreshold = -1);
        yield return Case("WalSaturationRecoveryWindow", o => o.WalSaturationRecoveryWindow = NegOne);
        yield return Case("WalSaturationFlushLatencyThreshold", o => o.WalSaturationFlushLatencyThreshold = TimeSpan.Zero);
        yield return Case("WalSaturationFlushLatencySampleWindows", o => o.WalSaturationFlushLatencySampleWindows = 0);
        yield return Case("WalSaturationMaterialiserLagThreshold", o => o.WalSaturationMaterialiserLagThreshold = TimeSpan.Zero);
        yield return Case("WalSaturationMaterialiserLagSampleWindows", o => o.WalSaturationMaterialiserLagSampleWindows = 0);
        yield return Case("WalMaterialiserMaxConcurrentReplays", o => o.WalMaterialiserMaxConcurrentReplays = -1);
        yield return Case("WalReplayMaxRecordsPerTurn", o => o.WalReplayMaxRecordsPerTurn = -1);
        yield return Case("WalAdmissionSaturationWaitBudget", o => o.WalAdmissionSaturationWaitBudget = NegOne);
        yield return Case("WalThrottledAdmissionPace", o => o.WalThrottledAdmissionPace = NegOne);
    }

    private static TestCaseData Case(string field, Action<LatticeOptions> mutate) =>
        new TestCaseData(field, mutate).SetName($"Validate_{field}_out_of_range_fails");

    [TestCaseSource(nameof(InvalidCases))]
    public void Validate_out_of_range_field_fails_naming_field(string field, Action<LatticeOptions> mutate)
    {
        var result = Validate(mutate);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(field));
        });
    }

    [Test]
    public void MaterialiserCheckpointInterval_infinite_succeeds()
    {
        Assert.That(Validate(o => o.MaterialiserCheckpointInterval = Timeout.InfiniteTimeSpan).Succeeded, Is.True);
    }

    [Test]
    public void LeafProjectionRetention_infinite_succeeds()
    {
        Assert.That(Validate(o => o.LeafProjectionRetention = Timeout.InfiniteTimeSpan).Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationRecoveryWindow_infinite_succeeds()
    {
        Assert.That(Validate(o => o.WalSaturationRecoveryWindow = Timeout.InfiniteTimeSpan).Succeeded, Is.True);
    }

    [Test]
    public void LeafSnapshotMargin_nan_fails()
    {
        Assert.That(Validate(o => o.LeafSnapshotMargin = double.NaN).Failed, Is.True);
    }

    [Test]
    public void WalSaturationThrottledRatio_nan_fails()
    {
        Assert.That(Validate(o => o.WalSaturationThrottledRatio = double.NaN).Failed, Is.True);
    }
}
