using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Validation rules for the four automatic over-split healing knobs.
/// <para>
/// The repository convention is that an illegal configuration is <b>rejected at
/// startup</b> rather than silently clamped, so an operator learns about a
/// mis-set knob immediately instead of discovering it later as strange
/// behaviour. These tests pin which values are illegal for each knob and - just
/// as importantly - which are legal, because two of them deliberately admit
/// zero as a documented "disabled" setting while a third deliberately does not.
/// </para>
/// </summary>
public class LatticeOptionsValidatorHealingTests
{
    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        return new LatticeOptionsValidator().Validate(null, options);
    }

    // --- ShardHealingEnabled: the kill switch -----------------------------

    [Test]
    public void ShardHealingEnabled_defaults_to_on()
        => Assert.That(new LatticeOptions().ShardHealingEnabled, Is.True,
            "healing is default-on (D5) so a deployment whose trees are already shattered repairs "
            + "itself with no operator action; opt-in healing heals nothing");

    [TestCase(true)]
    [TestCase(false)]
    public void ShardHealingEnabled_accepts_both_states(bool value)
        => Assert.That(Validate(o => o.ShardHealingEnabled = value).Succeeded, Is.True,
            "the kill switch must be settable in both directions without a validation failure");

    // --- ShardHealingInterval: no meaningful "disabled" value -------------

    [Test]
    public void ShardHealingInterval_of_zero_is_rejected()
    {
        var result = Validate(o => o.ShardHealingInterval = TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True,
                "a non-positive interval could never schedule an observation, so it is rejected rather "
                + "than treated as disabled; ShardHealingEnabled is the way to switch healing off");
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ShardHealingInterval)));
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ShardHealingEnabled)),
                "the message must point the operator at the knob that actually disables healing");
        });
    }

    [Test]
    public void ShardHealingInterval_negative_is_rejected()
        => Assert.That(Validate(o => o.ShardHealingInterval = TimeSpan.FromSeconds(-1)).Failed, Is.True);

    [Test]
    public void ShardHealingInterval_positive_is_accepted()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Validate(o => o.ShardHealingInterval = TimeSpan.FromSeconds(1)).Succeeded, Is.True);
            Assert.That(Validate(o => o.ShardHealingInterval = TimeSpan.FromHours(6)).Succeeded, Is.True);
        });
    }

    [Test]
    public void ShardHealingInterval_defaults_to_thirty_seconds()
        => Assert.That(new LatticeOptions().ShardHealingInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));

    // --- ShardHealingCooldown: zero is legal and means "no stand-off" -----

    [Test]
    public void ShardHealingCooldown_of_zero_is_accepted()
        => Assert.That(Validate(o => o.ShardHealingCooldown = TimeSpan.Zero).Succeeded, Is.True,
            "zero disables the post-split stand-off, leaving only the skew dead band; that is a "
            + "supported configuration, not an error");

    [Test]
    public void ShardHealingCooldown_negative_is_rejected()
    {
        var result = Validate(o => o.ShardHealingCooldown = TimeSpan.FromSeconds(-1));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ShardHealingCooldown)));
        });
    }

    [Test]
    public void ShardHealingCooldown_defaults_to_five_minutes()
        => Assert.That(new LatticeOptions().ShardHealingCooldown, Is.EqualTo(TimeSpan.FromMinutes(5)));

    // --- ShardHealingBackpressureOpsPerSecond: zero disables backpressure --

    [Test]
    public void ShardHealingBackpressureOpsPerSecond_of_zero_is_accepted()
        => Assert.That(Validate(o => o.ShardHealingBackpressureOpsPerSecond = 0d).Succeeded, Is.True,
            "zero heals regardless of load, which an operator repairing a quiet box legitimately wants");

    [TestCase(-1d)]
    [TestCase(double.NaN)]
    public void ShardHealingBackpressureOpsPerSecond_illegal_values_are_rejected(double value)
    {
        var result = Validate(o => o.ShardHealingBackpressureOpsPerSecond = value);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage,
                Does.Contain(nameof(LatticeOptions.ShardHealingBackpressureOpsPerSecond)));
        });
    }

    [TestCase(1d)]
    [TestCase(200d)]
    [TestCase(100_000d)]
    public void ShardHealingBackpressureOpsPerSecond_positive_is_accepted(double value)
        => Assert.That(Validate(o => o.ShardHealingBackpressureOpsPerSecond = value).Succeeded, Is.True);

    [Test]
    public void ShardHealingBackpressureOpsPerSecond_defaults_to_the_hot_shard_threshold()
        => Assert.That(new LatticeOptions().ShardHealingBackpressureOpsPerSecond,
            Is.EqualTo((double)LatticeOptions.DefaultHotShardOpsPerSecondThreshold),
            "healing yields at exactly the load at which a shard would be considered hot, so the two "
            + "loops use one calibration rather than two that could drift apart");

    // --- The shipped defaults are internally consistent -------------------

    [Test]
    public void The_shipped_defaults_validate()
        => Assert.That(new LatticeOptionsValidator().Validate(null, new LatticeOptions()).Succeeded, Is.True);

    [Test]
    public void The_shipped_defaults_keep_the_hysteresis_regions_disjoint()
    {
        // The healing knobs must not be settable into a configuration where the
        // split and consolidation triggers overlap - the validator already
        // rejects that, and this pins that the defaults sit safely inside it.
        var options = new LatticeOptions();
        Assert.That(
            ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(
                options.HotShardConsolidationSkewRatio, options.HotShardMinSkewRatio),
            Is.True);
    }
}
