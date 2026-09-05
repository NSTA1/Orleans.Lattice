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
