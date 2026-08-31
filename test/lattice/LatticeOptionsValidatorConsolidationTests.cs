using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Validation rules for the three online shard-consolidation knobs.
/// <para>
/// The repository convention is that an illegal configuration is <b>rejected
/// at startup</b> rather than silently clamped, so an operator learns about a
/// mis-set knob immediately instead of discovering it later as strange
/// behaviour. These tests pin which values are illegal for each knob, and
/// - just as importantly - which are legal, because
/// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/> deliberately
/// admits <c>0</c> as the supported way to switch automated shard healing off.
/// </para>
/// </summary>
public class LatticeOptionsValidatorConsolidationTests
{
    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        return new LatticeOptionsValidator().Validate(null, options);
    }

    // --- ConsolidationDrainBatchSize: no meaningful "disabled" value ---

    [TestCase(0)]
    [TestCase(-1)]
    public void ConsolidationDrainBatchSize_below_one_is_rejected(int value)
    {
        var result = Validate(o => o.ConsolidationDrainBatchSize = value);

        Assert.That(result.Failed, Is.True,
            "A fold that flushes no entries could never drain the donor, so 0 is illegal rather than 'disabled'.");
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ConsolidationDrainBatchSize)));
    }

    [TestCase(1)]
    [TestCase(1024)]
    [TestCase(65536)]
    public void ConsolidationDrainBatchSize_positive_is_accepted(int value)
    {
        Assert.That(Validate(o => o.ConsolidationDrainBatchSize = value).Succeeded, Is.True);
    }

    // --- ConsolidationDrainLeavesPerPass: 0 would livelock, not disable ---

    [TestCase(0)]
    [TestCase(-5)]
    public void ConsolidationDrainLeavesPerPass_below_one_is_rejected(int value)
    {
        var result = Validate(o => o.ConsolidationDrainLeavesPerPass = value);

        Assert.That(result.Failed, Is.True,
            "0 leaves per pass would leave a started fold sitting in its drain phase forever, "
            + "which is worse than either a default or a disabled state.");
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.ConsolidationDrainLeavesPerPass)));
    }

    [TestCase(1)]
    [TestCase(16)]
    [TestCase(4096)]
    public void ConsolidationDrainLeavesPerPass_positive_is_accepted(int value)
    {
        Assert.That(Validate(o => o.ConsolidationDrainLeavesPerPass = value).Succeeded, Is.True);
    }

    // --- MaxConcurrentShardConsolidations: 0 IS legal and means "off" ---

    [Test]
    public void MaxConcurrentShardConsolidations_zero_is_accepted_as_the_disabled_setting()
    {
        Assert.That(Validate(o => o.MaxConcurrentShardConsolidations = 0).Succeeded, Is.True,
            "0 is the supported way to switch automated shard healing off without removing the driver, "
            + "so it must not be rejected as a misconfiguration.");
    }

    [TestCase(1)]
    [TestCase(8)]
    public void MaxConcurrentShardConsolidations_positive_is_accepted(int value)
    {
        Assert.That(Validate(o => o.MaxConcurrentShardConsolidations = value).Succeeded, Is.True);
    }

    [TestCase(-1)]
    [TestCase(-100)]
    public void MaxConcurrentShardConsolidations_negative_is_rejected(int value)
    {
        var result = Validate(o => o.MaxConcurrentShardConsolidations = value);

        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.MaxConcurrentShardConsolidations)));
    }

    // --- Defaults ---

    [Test]
    public void The_shipped_consolidation_defaults_validate()
    {
        var options = new LatticeOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.ConsolidationDrainBatchSize,
                Is.EqualTo(LatticeOptions.DefaultConsolidationDrainBatchSize));
            Assert.That(options.ConsolidationDrainLeavesPerPass,
                Is.EqualTo(LatticeOptions.DefaultConsolidationDrainLeavesPerPass));
            Assert.That(options.MaxConcurrentShardConsolidations,
                Is.EqualTo(LatticeOptions.DefaultMaxConcurrentShardConsolidations));
        });

        Assert.That(new LatticeOptionsValidator().Validate(null, options).Succeeded, Is.True,
            "The shipped defaults must satisfy the rules that guard them.");
    }
}
