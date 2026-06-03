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
    public void WalMaxPendingBatches_default_is_eight()
    {
        Assert.That(new LatticeOptions().WalMaxPendingBatches, Is.EqualTo(8));
        Assert.That(LatticeOptions.DefaultWalMaxPendingBatches, Is.EqualTo(8));
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
}