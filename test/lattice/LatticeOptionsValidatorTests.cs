using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

public class LatticeOptionsValidatorTests
{
    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        var validator = new LatticeOptionsValidator();
        return validator.Validate(null, options);
    }

    [Test]
    public void Valid_defaults_pass()
    {
        var result = Validate(_ => { });
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void QueueCapacity_null_succeeds()
    {
        var result = Validate(o => o.QueueCapacity = null);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(1)]
    [TestCase(1000)]
    public void QueueCapacity_positive_succeeds(int value)
    {
        var result = Validate(o => o.QueueCapacity = value);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void QueueCapacity_below_one_fails(int value)
    {
        var result = Validate(o => o.QueueCapacity = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain("QueueCapacity"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void KeysPageSize_must_be_positive(int value)
    {
        var result = Validate(o => o.KeysPageSize = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain("KeysPageSize"));
    }

    [Test]
    public void Valid_custom_values_pass()
    {
        var result = Validate(o =>
        {
            o.KeysPageSize = 1;
        });
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void MaxLeafReplayEntries_must_be_at_least_one(int value)
    {
        var result = Validate(o => o.MaxLeafReplayEntries = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.MaxLeafReplayEntries)));
    }

    [Test]
    public void MaxLeafReplayEntries_at_one_passes()
    {
        var result = Validate(o => o.MaxLeafReplayEntries = 1);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(-1)]
    [TestCase(-100)]
    public void LeafSnapshotReClassifyEveryNCheckpoints_must_be_non_negative(int value)
    {
        var result = Validate(o => o.LeafSnapshotReClassifyEveryNCheckpoints = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage,
            Does.Contain(nameof(LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints)));
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(64)]
    public void LeafSnapshotReClassifyEveryNCheckpoints_non_negative_passes(int value)
    {
        var result = Validate(o => o.LeafSnapshotReClassifyEveryNCheckpoints = value);
        Assert.That(result.Succeeded, Is.True);
    }

    // --- v6.0.1 ship-default pins --------------------------------------
    // One per knob the throughput campaign flipped from a v6.0.0 baseline.
    // Each pin asserts (a) the live property's default matches the named
    // constant, and (b) the named constant carries the documented value.
    // The constant indirection exists so a future re-tune lands in one
    // place; the pin catches a future blind edit that flips the default
    // without coordinating with the docs / changelog.

    [Test]
    public void WalMaxPendingBatches_default_is_sixteen()
    {
        Assert.That(new LatticeOptions().WalMaxPendingBatches, Is.EqualTo(16));
        Assert.That(LatticeOptions.DefaultWalMaxPendingBatches, Is.EqualTo(16));
    }

    [Test]
    public void DirtyLeafFlushIntervalMs_default_is_fifty_ms()
    {
        Assert.That(new LatticeOptions().DirtyLeafFlushIntervalMs, Is.EqualTo(50));
        Assert.That(LatticeOptions.DefaultDirtyLeafFlushIntervalMs, Is.EqualTo(50));
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(-8)]
    public void WalPartitions_must_be_at_least_one(int value)
    {
        var result = Validate(o => o.WalPartitions = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalPartitions)));
    }

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(8)]
    [TestCase(1024)]
    public void WalPartitions_positive_passes(int value)
    {
        var result = Validate(o => o.WalPartitions = value);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalPartitions_default_is_eight()
    {
        Assert.That(new LatticeOptions().WalPartitions, Is.EqualTo(8));
        Assert.That(LatticeOptions.DefaultWalPartitions, Is.EqualTo(8));
    }

    [Test]
    public void WalFlushTimeout_default_is_fifteen_seconds()
    {
        Assert.That(new LatticeOptions().WalFlushTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
        Assert.That(LatticeOptions.DefaultWalFlushTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
    }

    [Test]
    public void WalFlushTimeout_positive_passes()
    {
        var result = Validate(o => o.WalFlushTimeout = TimeSpan.FromSeconds(5));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalFlushTimeout_infinite_passes()
    {
        var result = Validate(o => o.WalFlushTimeout = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalFlushTimeout_zero_fails()
    {
        var result = Validate(o => o.WalFlushTimeout = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalFlushTimeout)));
    }

    [Test]
    public void WalFlushTimeout_negative_fails()
    {
        var result = Validate(o => o.WalFlushTimeout = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalFlushTimeout)));
    }

    [Test]
    public void ShardForwardTimeout_default_is_fifteen_seconds()
    {
        Assert.That(new LatticeOptions().ShardForwardTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
        Assert.That(LatticeOptions.DefaultShardForwardTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
    }

    [Test]
    public void ShardForwardTimeout_positive_passes()
    {
        var result = Validate(o => o.ShardForwardTimeout = TimeSpan.FromSeconds(5));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void ShardForwardTimeout_infinite_passes()
    {
        var result = Validate(o => o.ShardForwardTimeout = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void ShardForwardTimeout_zero_fails()
    {
        var result = Validate(o => o.ShardForwardTimeout = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ShardForwardTimeout)));
    }

    [Test]
    public void ShardForwardTimeout_negative_fails()
    {
        var result = Validate(o => o.ShardForwardTimeout = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ShardForwardTimeout)));
    }

    [Test]
    public void ActivationReadyTimeout_default_is_fifteen_seconds()
    {
        Assert.That(new LatticeOptions().ActivationReadyTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
        Assert.That(LatticeOptions.DefaultActivationReadyTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
    }

    [Test]
    public void ActivationReadyTimeout_positive_passes()
    {
        var result = Validate(o => o.ActivationReadyTimeout = TimeSpan.FromSeconds(5));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void DigestPublishTimeout_default_is_fifteen_seconds()
    {
        Assert.That(new LatticeOptions().DigestPublishTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
        Assert.That(LatticeOptions.DefaultDigestPublishTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
    }

    [Test]
    public void DigestPublishTimeout_positive_passes()
    {
        var result = Validate(o => o.DigestPublishTimeout = TimeSpan.FromSeconds(5));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void ActivationReadyTimeout_infinite_passes()
    {
        var result = Validate(o => o.ActivationReadyTimeout = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void DigestPublishTimeout_infinite_passes()
    {
        var result = Validate(o => o.DigestPublishTimeout = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void ActivationReadyTimeout_zero_fails()
    {
        var result = Validate(o => o.ActivationReadyTimeout = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ActivationReadyTimeout)));
    }

    [Test]
    public void ActivationReadyTimeout_negative_fails()
    {
        var result = Validate(o => o.ActivationReadyTimeout = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ActivationReadyTimeout)));
    }

    [Test]
    public void DigestPublishTimeout_zero_fails()
    {
        var result = Validate(o => o.DigestPublishTimeout = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.DigestPublishTimeout)));
    }

    [Test]
    public void DigestPublishTimeout_negative_fails()
    {
        var result = Validate(o => o.DigestPublishTimeout = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.DigestPublishTimeout)));
    }

    [Test]
    public void WalAppendDispatchTimeout_default_is_thirty_seconds()
    {
        Assert.That(new LatticeOptions().WalAppendDispatchTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(LatticeOptions.DefaultWalAppendDispatchTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void WalAppendDispatchTimeout_positive_passes()
    {
        var result = Validate(o => o.WalAppendDispatchTimeout = TimeSpan.FromSeconds(5));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalAppendDispatchTimeout_infinite_passes()
    {
        var result = Validate(o => o.WalAppendDispatchTimeout = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalAppendDispatchTimeout_zero_fails()
    {
        var result = Validate(o => o.WalAppendDispatchTimeout = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalAppendDispatchTimeout)));
    }

    [Test]
    public void WalAppendDispatchTimeout_negative_fails()
    {
        var result = Validate(o => o.WalAppendDispatchTimeout = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalAppendDispatchTimeout)));
    }

    [Test]
    public void WalFlushPreflightTimeout_default_is_five_seconds()
    {
        Assert.That(new LatticeOptions().WalFlushPreflightTimeout, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(LatticeOptions.DefaultWalFlushPreflightTimeout, Is.EqualTo(TimeSpan.FromSeconds(5)));
    }

    [Test]
    public void WalFlushPreflightTimeout_positive_passes()
    {
        var result = Validate(o => o.WalFlushPreflightTimeout = TimeSpan.FromMilliseconds(50));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalFlushPreflightTimeout_infinite_passes()
    {
        var result = Validate(o => o.WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalFlushPreflightTimeout_zero_fails()
    {
        var result = Validate(o => o.WalFlushPreflightTimeout = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalFlushPreflightTimeout)));
    }

    [Test]
    public void WalFlushPreflightTimeout_negative_fails()
    {
        var result = Validate(o => o.WalFlushPreflightTimeout = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalFlushPreflightTimeout)));
    }

    [Test]
    public void WalDrainBudget_default_is_seventy_five_seconds()
    {
        Assert.That(new LatticeOptions().WalDrainBudget, Is.EqualTo(TimeSpan.FromSeconds(75)));
        Assert.That(LatticeOptions.DefaultWalDrainBudget, Is.EqualTo(TimeSpan.FromSeconds(75)));
    }

    [Test]
    public void WalDrainBudget_default_is_five_times_default_flush_timeout()
    {
        // The default is documented as 5 * WalFlushTimeout. Pin the
        // relationship so a future change to WalFlushTimeout does not
        // silently break the documented scaling.
        Assert.That(LatticeOptions.DefaultWalDrainBudget,
            Is.EqualTo(LatticeOptions.DefaultWalFlushTimeout * 5),
            "WalDrainBudget default must remain 5 * WalFlushTimeout default per the documented scaling rule.");
    }

    [Test]
    public void WalDrainBudget_positive_passes()
    {
        var result = Validate(o => o.WalDrainBudget = TimeSpan.FromSeconds(5));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalDrainBudget_infinite_passes()
    {
        var result = Validate(o => o.WalDrainBudget = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalDrainBudget_zero_fails()
    {
        var result = Validate(o => o.WalDrainBudget = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalDrainBudget)));
    }

    [Test]
    public void WalDrainBudget_negative_fails()
    {
        var result = Validate(o => o.WalDrainBudget = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalDrainBudget)));
    }

    [Test]
    public void WalSaturationSampleInterval_default_is_two_hundred_milliseconds()
    {
        Assert.That(new LatticeOptions().WalSaturationSampleInterval, Is.EqualTo(TimeSpan.FromMilliseconds(200)));
        Assert.That(LatticeOptions.DefaultWalSaturationSampleInterval, Is.EqualTo(TimeSpan.FromMilliseconds(200)));
    }

    [Test]
    public void WalSaturationSampleInterval_positive_passes()
    {
        var result = Validate(o => o.WalSaturationSampleInterval = TimeSpan.FromMilliseconds(50));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationSampleInterval_infinite_passes()
    {
        // Infinite explicitly disables the sampler - signal pins to Healthy.
        var result = Validate(o => o.WalSaturationSampleInterval = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationSampleInterval_zero_fails()
    {
        var result = Validate(o => o.WalSaturationSampleInterval = TimeSpan.Zero);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalSaturationSampleInterval)));
    }

    [Test]
    public void WalSaturationSampleInterval_negative_fails()
    {
        // Use -1s (not -1ms) because -1ms equals Timeout.InfiniteTimeSpan
        // and is therefore explicitly allowed as the "disable sampler"
        // sentinel.
        var result = Validate(o => o.WalSaturationSampleInterval = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalSaturationSampleInterval)));
    }

    [Test]
    public void WalSaturationThrottledRatio_default_is_zero_point_seventy_five()
    {
        Assert.That(new LatticeOptions().WalSaturationThrottledRatio, Is.EqualTo(0.75));
        Assert.That(LatticeOptions.DefaultWalSaturationThrottledRatio, Is.EqualTo(0.75));
    }

    [TestCase(0.0)]
    [TestCase(0.5)]
    [TestCase(1.0)]
    public void WalSaturationThrottledRatio_in_range_passes(double value)
    {
        var result = Validate(o => o.WalSaturationThrottledRatio = value);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(-0.01)]
    [TestCase(1.01)]
    [TestCase(double.NaN)]
    public void WalSaturationThrottledRatio_out_of_range_fails(double value)
    {
        var result = Validate(o => o.WalSaturationThrottledRatio = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalSaturationThrottledRatio)));
    }

    [Test]
    public void WalSaturationDispatchTimeoutThreshold_default_is_one()
    {
        Assert.That(new LatticeOptions().WalSaturationDispatchTimeoutThreshold, Is.EqualTo(1));
        Assert.That(LatticeOptions.DefaultWalSaturationDispatchTimeoutThreshold, Is.EqualTo(1));
    }

    [Test]
    public void WalSaturationDispatchTimeoutThreshold_positive_passes()
    {
        var result = Validate(o => o.WalSaturationDispatchTimeoutThreshold = 5);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void WalSaturationDispatchTimeoutThreshold_non_positive_fails(int value)
    {
        var result = Validate(o => o.WalSaturationDispatchTimeoutThreshold = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalSaturationDispatchTimeoutThreshold)));
    }

    [Test]
    public void WalSaturationProviderFailureRateThreshold_default_is_one()
    {
        Assert.That(new LatticeOptions().WalSaturationProviderFailureRateThreshold, Is.EqualTo(1));
        Assert.That(LatticeOptions.DefaultWalSaturationProviderFailureRateThreshold, Is.EqualTo(1));
    }

    [Test]
    public void WalSaturationProviderFailureRateThreshold_positive_passes()
    {
        var result = Validate(o => o.WalSaturationProviderFailureRateThreshold = 5);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationProviderFailureRateThreshold_zero_passes()
    {
        // Zero is the documented "disable the provider-failure-rate
        // trigger entirely" sentinel. The validator must accept it
        // (unlike the dispatch-timeout-threshold counterpart, where
        // zero is rejected because the dispatch-timeout signal cannot
        // be opted out of independently of the sampler interval).
        var result = Validate(o => o.WalSaturationProviderFailureRateThreshold = 0);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationProviderFailureRateThreshold_negative_fails()
    {
        var result = Validate(o => o.WalSaturationProviderFailureRateThreshold = -1);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalSaturationProviderFailureRateThreshold)));
    }

    [Test]
    public void WalSaturationRecoveryWindow_default_is_one_second()
    {
        Assert.That(new LatticeOptions().WalSaturationRecoveryWindow, Is.EqualTo(TimeSpan.FromSeconds(1)));
        Assert.That(LatticeOptions.DefaultWalSaturationRecoveryWindow, Is.EqualTo(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public void WalSaturationRecoveryWindow_positive_passes()
    {
        var result = Validate(o => o.WalSaturationRecoveryWindow = TimeSpan.FromMilliseconds(500));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationRecoveryWindow_zero_passes()
    {
        // Zero is the documented "disable the upgrade entirely" sentinel
        // that restores the per-tick classifier behaviour the sampler
        // shipped with before the recovery-window upgrade. Validator
        // must accept it (unlike most TimeSpan options where zero is
        // rejected as a no-op).
        var result = Validate(o => o.WalSaturationRecoveryWindow = TimeSpan.Zero);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationRecoveryWindow_infinite_passes()
    {
        // Infinite holds the Throttled floor forever after the first
        // Saturated observation - the documented sentinel.
        var result = Validate(o => o.WalSaturationRecoveryWindow = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationRecoveryWindow_negative_fails()
    {
        // -1s is not the InfiniteTimeSpan sentinel (which is -1ms);
        // it is a genuine negative TimeSpan and must be rejected.
        var result = Validate(o => o.WalSaturationRecoveryWindow = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalSaturationRecoveryWindow)));
    }

    // --- Admission-gate saturation wait budget ---

    [Test]
    public void WalAdmissionSaturationWaitBudget_default_is_five_seconds()
    {
        Assert.That(new LatticeOptions().WalAdmissionSaturationWaitBudget, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(LatticeOptions.DefaultWalAdmissionSaturationWaitBudget, Is.EqualTo(TimeSpan.FromSeconds(5)));
    }

    [Test]
    public void WalAdmissionSaturationWaitBudget_positive_passes()
    {
        var result = Validate(o => o.WalAdmissionSaturationWaitBudget = TimeSpan.FromSeconds(10));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalAdmissionSaturationWaitBudget_zero_passes()
    {
        // Zero is the documented operator opt-out: the admission
        // gate is bypassed entirely (the historical
        // pre-admission-gate behaviour). The validator must accept
        // it (unlike most TimeSpan options where zero is rejected
        // as a no-op).
        var result = Validate(o => o.WalAdmissionSaturationWaitBudget = TimeSpan.Zero);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalAdmissionSaturationWaitBudget_infinite_passes()
    {
        // Infinite waits forever on WaitForHealthyAsync - the
        // documented sentinel.
        var result = Validate(o => o.WalAdmissionSaturationWaitBudget = Timeout.InfiniteTimeSpan);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalAdmissionSaturationWaitBudget_negative_fails()
    {
        // -1s is not the InfiniteTimeSpan sentinel (which is -1ms);
        // it is a genuine negative TimeSpan and must be rejected.
        var result = Validate(o => o.WalAdmissionSaturationWaitBudget = TimeSpan.FromSeconds(-1));
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalAdmissionSaturationWaitBudget)));
    }
}