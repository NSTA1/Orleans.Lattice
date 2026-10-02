using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests that <see cref="LatticeOptionsValidator"/> bounds every duration option the
/// runtime arms as a timer-backed wait at the longest value a timer accepts.
/// <para>
/// The per-option checks admit any positive value (or
/// <see cref="Timeout.InfiniteTimeSpan"/>), but each option is armed as a
/// <see cref="CancellationTokenSource(TimeSpan)"/>,
/// <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/>,
/// <see cref="Task.Delay(TimeSpan)"/> or <see cref="Task.WaitAsync(TimeSpan)"/> -
/// all of which throw <see cref="ArgumentOutOfRangeException"/> above
/// <c>0xFFFFFFFE</c> milliseconds. So a value such as <see cref="TimeSpan.MaxValue"/>,
/// the usual spelling of "no timeout", passed validation and then failed every WAL
/// flush, append, forward or digest publish that armed it. Each rejected value is
/// checked against the primitive itself, so these tests cannot pass by rejecting a
/// value the runtime would accept.
/// </para>
/// <para>
/// <see cref="LatticeOptions.HotShardSampleInterval"/> and
/// <see cref="LatticeOptions.ShardHealingInterval"/> are grain-timer periods rather
/// than waits, and a grain timer refuses the same range: an over-long value threw
/// each time the hot-shard monitor or the healing orchestrator armed its timer, so
/// the tree was silently never sampled for auto-split and never healed.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeOptionsValidatorTimerCeilingTests
{
    private static readonly TimeSpan TimerCeiling = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private static readonly (string Field, Action<LatticeOptions, TimeSpan> Set)[] Fields =
    [
        (nameof(LatticeOptions.WalFlushTimeout), (o, v) => o.WalFlushTimeout = v),
        (nameof(LatticeOptions.WalFlushPreflightTimeout), (o, v) => o.WalFlushPreflightTimeout = v),
        (nameof(LatticeOptions.WalDrainBudget), (o, v) => o.WalDrainBudget = v),
        (nameof(LatticeOptions.WalAppendDispatchTimeout), (o, v) => o.WalAppendDispatchTimeout = v),
        (nameof(LatticeOptions.ShardForwardTimeout), (o, v) => o.ShardForwardTimeout = v),
        (nameof(LatticeOptions.ActivationReadyTimeout), (o, v) => o.ActivationReadyTimeout = v),
        (nameof(LatticeOptions.DigestPublishTimeout), (o, v) => o.DigestPublishTimeout = v),
        (nameof(LatticeOptions.EmptyTreeProbeBudget), (o, v) => o.EmptyTreeProbeBudget = v),
        (nameof(LatticeOptions.StarvationDriveBudget), (o, v) => o.StarvationDriveBudget = v),
        (nameof(LatticeOptions.SetManyFanOutBudget), (o, v) => o.SetManyFanOutBudget = v),
        (nameof(LatticeOptions.WalSaturationSampleInterval), (o, v) => o.WalSaturationSampleInterval = v),
        (nameof(LatticeOptions.WalAdmissionSaturationWaitBudget), (o, v) => o.WalAdmissionSaturationWaitBudget = v),
        (nameof(LatticeOptions.WalAdmissionSaturationCallBudget), (o, v) => o.WalAdmissionSaturationCallBudget = v),
        (nameof(LatticeOptions.WalThrottledAdmissionPace), (o, v) => o.WalThrottledAdmissionPace = v),
        (nameof(LatticeOptions.MaxScanPageStallDuration), (o, v) => o.MaxScanPageStallDuration = v),
        (nameof(LatticeOptions.HotShardSampleInterval), (o, v) => o.HotShardSampleInterval = v),
        (nameof(LatticeOptions.ShardHealingInterval), (o, v) => o.ShardHealingInterval = v),
        (nameof(LatticeOptions.CompactionShardTickInterval), (o, v) => o.CompactionShardTickInterval = v),
        (nameof(LatticeOptions.StorageUsageRollupBudget), (o, v) => o.StorageUsageRollupBudget = v),
    ];

    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        return new LatticeOptionsValidator().Validate(null, options);
    }

    private static IEnumerable<TestCaseData> DurationsAboveTheCeiling()
    {
        foreach (var (field, set) in Fields)
        {
            foreach (var value in new[] { TimerCeiling + TimeSpan.FromMilliseconds(1), TimeSpan.FromDays(60), TimeSpan.MaxValue })
            {
                yield return new TestCaseData(field, set, value)
                    .SetName($"{field}_of_{value.TotalMilliseconds:F0}_ms_is_rejected");
            }
        }
    }

    private static IEnumerable<TestCaseData> DurationsAtTheCeiling()
    {
        foreach (var (field, set) in Fields)
        {
            yield return new TestCaseData(field, set).SetName($"{field}_at_the_timer_ceiling_is_admitted");
        }
    }

    [Test]
    public void The_validator_ceiling_is_the_longest_wait_a_timer_accepts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeOptionsValidator.MaxTimerDuration, Is.EqualTo(TimerCeiling));
            Assert.That(() => new CancellationTokenSource(TimerCeiling).Dispose(), Throws.Nothing);
            Assert.That(
                () => new CancellationTokenSource(TimerCeiling + TimeSpan.FromMilliseconds(1)),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [TestCaseSource(nameof(DurationsAboveTheCeiling))]
    public void A_duration_a_timer_cannot_wait_is_rejected(
        string field, Action<LatticeOptions, TimeSpan> set, TimeSpan value)
    {
        var result = Validate(o => set(o, value));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(field + " must be at most"));
            Assert.That(
                () => new CancellationTokenSource(value),
                Throws.InstanceOf<ArgumentOutOfRangeException>(),
                "anti-vacuity: the rejected value is one CancellationTokenSource itself refuses");
        });
    }

    [TestCaseSource(nameof(DurationsAtTheCeiling))]
    public void A_duration_at_the_timer_ceiling_is_admitted(string field, Action<LatticeOptions, TimeSpan> set)
    {
        var result = Validate(o => set(o, TimerCeiling));

        Assert.That(result.Succeeded, Is.True, $"{field}: {result.FailureMessage}");
    }

    [Test]
    public void Infinite_is_still_admitted_where_it_was_before()
    {
        var result = Validate(o =>
        {
            o.WalFlushTimeout = Timeout.InfiniteTimeSpan;
            o.WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan;
            o.WalDrainBudget = Timeout.InfiniteTimeSpan;
            o.WalAppendDispatchTimeout = Timeout.InfiniteTimeSpan;
            o.ShardForwardTimeout = Timeout.InfiniteTimeSpan;
            o.ActivationReadyTimeout = Timeout.InfiniteTimeSpan;
            o.DigestPublishTimeout = Timeout.InfiniteTimeSpan;
            o.EmptyTreeProbeBudget = Timeout.InfiniteTimeSpan;
            o.SetManyFanOutBudget = Timeout.InfiniteTimeSpan;
            o.WalSaturationSampleInterval = Timeout.InfiniteTimeSpan;
            o.WalAdmissionSaturationWaitBudget = Timeout.InfiniteTimeSpan;
            o.WalAdmissionSaturationCallBudget = Timeout.InfiniteTimeSpan;
            o.MaxScanPageStallDuration = Timeout.InfiniteTimeSpan;
        });

        Assert.That(result.Succeeded, Is.True, result.FailureMessage);
    }

    [Test]
    public void A_non_positive_storage_usage_rollup_budget_still_disables_the_budget()
    {
        foreach (var disabled in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1), Timeout.InfiniteTimeSpan })
        {
            var result = Validate(o => o.StorageUsageRollupBudget = disabled);

            Assert.That(result.Succeeded, Is.True, $"{disabled}: {result.FailureMessage}");
        }
    }

    [Test]
    public void A_compaction_tick_above_the_timer_ceiling_is_one_a_grain_timer_period_refuses()
    {
        // The compaction pass arms CompactionShardTickInterval as a grain-timer
        // period; Orleans validates a period against the same TimeProvider timer
        // range, so the value the validator now rejects is one the timer itself
        // would refuse each time a pass started.
        var overLong = TimerCeiling + TimeSpan.FromMilliseconds(1);

        Assert.That(
            () => TimeProvider.System.CreateTimer(static _ => { }, null, TimeSpan.Zero, overLong).Dispose(),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
