using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Validation coverage for the durable materialiser-pin bucketing knob and the
/// durable-pin-latency saturation classifier inputs added for issues #2014 and
/// #2015.
/// <para>
/// All three default to values that leave existing deployments byte-for-byte
/// unchanged - one bucket (the single legacy slot) and a disabled classifier
/// input - so the defaults passing validation is itself part of the
/// compatibility contract.
/// </para>
/// </summary>
public class LatticeOptionsValidatorPinBucketTests
{
    private static ValidateOptionsResult Validate(Action<LatticeOptions> configure)
    {
        var options = new LatticeOptions();
        configure(options);
        var validator = new LatticeOptionsValidator();
        return validator.Validate(null, options);
    }

    [Test]
    public void Pin_bucketing_and_pin_latency_defaults_are_compatible()
    {
        var options = new LatticeOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.WalMaterialiserPinBuckets, Is.EqualTo(1),
                "the default must persist to the single legacy slot so an upgrade changes no durable bytes");
            Assert.That(options.WalSaturationMaterialiserPinLatencyThreshold, Is.Null,
                "the durable-pin latency classifier input must be opt-in so no existing host changes saturation state on upgrade");
            Assert.That(Validate(_ => { }).Succeeded, Is.True);
        });
    }

    [Test]
    public void WalMaterialiserPinShedCeiling_defaults_to_disarmed()
    {
        // The issue #3310 ceiling ships OFF, for the same compatibility reason as
        // the bucket count above: arming it changes when a host writes durable
        // pins, and a library cannot know whether a given deployment's pin store
        // can absorb that. The repocontext container arms it explicitly at its own
        // wiring seam.
        var options = new LatticeOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.WalMaterialiserPinShedCeiling, Is.Null);
            Assert.That(Validate(_ => { }).Succeeded, Is.True);
        });
    }

    [TestCase(1)]
    [TestCase(120)]
    [TestCase(3600)]
    public void A_positive_pin_shed_ceiling_succeeds(int seconds)
        => Assert.That(
            Validate(o => o.WalMaterialiserPinShedCeiling = TimeSpan.FromSeconds(seconds)).Succeeded,
            Is.True);

    [TestCase(0)]
    [TestCase(-1)]
    public void A_non_positive_pin_shed_ceiling_is_rejected_rather_than_read_as_disarmed(int seconds)
    {
        // The distinction this asserts is the whole point of the branch. Reading a
        // zero as "disarmed" would be the lenient choice and would hide an
        // operator's typo; reading it literally would be worse still, because a
        // zero-length ceiling makes every shed run instantly older than it and
        // forces EVERY report through, disabling the issue #2014 back-pressure and
        // re-saturating the pin queue the shedding exists to protect. Null is the
        // only expression of "disarmed", so a non-positive value is always a
        // mistake and is named as one.
        var result = Validate(o => o.WalMaterialiserPinShedCeiling = TimeSpan.FromSeconds(seconds));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalMaterialiserPinShedCeiling)));
            Assert.That(result.FailureMessage, Does.Contain("null to disarm"),
                "the message must name the ONLY way to turn the ceiling off, or an operator who wanted it off will reach for zero again");
        });
    }

    [TestCase(1)]
    [TestCase(8)]
    [TestCase(4096)]
    public void WalMaterialiserPinBuckets_at_or_above_one_succeeds(int value)
    {
        var result = Validate(o => o.WalMaterialiserPinBuckets = value);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void WalMaterialiserPinBuckets_below_one_fails(int value)
    {
        var result = Validate(o => o.WalMaterialiserPinBuckets = value);
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("WalMaterialiserPinBuckets"));
        });
    }

    [Test]
    public void WalSaturationMaterialiserPinLatencyThreshold_null_succeeds()
    {
        var result = Validate(o => o.WalSaturationMaterialiserPinLatencyThreshold = null);
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationMaterialiserPinLatencyThreshold_positive_succeeds()
    {
        var result = Validate(o => o.WalSaturationMaterialiserPinLatencyThreshold = TimeSpan.FromSeconds(2));
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void WalSaturationMaterialiserPinLatencyThreshold_zero_fails()
    {
        var result = Validate(o => o.WalSaturationMaterialiserPinLatencyThreshold = TimeSpan.Zero);
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("WalSaturationMaterialiserPinLatencyThreshold"));
        });
    }

    [Test]
    public void WalSaturationMaterialiserPinLatencyThreshold_negative_fails()
    {
        var result = Validate(o => o.WalSaturationMaterialiserPinLatencyThreshold = TimeSpan.FromMilliseconds(-1));
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("WalSaturationMaterialiserPinLatencyThreshold"));
        });
    }

    [TestCase(1)]
    [TestCase(3)]
    [TestCase(64)]
    public void WalSaturationMaterialiserPinLatencySampleWindows_at_or_above_one_succeeds(int value)
    {
        var result = Validate(o => o.WalSaturationMaterialiserPinLatencySampleWindows = value);
        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void WalSaturationMaterialiserPinLatencySampleWindows_below_one_fails(int value)
    {
        var result = Validate(o => o.WalSaturationMaterialiserPinLatencySampleWindows = value);
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("WalSaturationMaterialiserPinLatencySampleWindows"));
        });
    }
}
