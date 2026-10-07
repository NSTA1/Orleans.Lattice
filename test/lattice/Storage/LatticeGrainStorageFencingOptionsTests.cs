using NUnit.Framework;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Unit tests for <see cref="LatticeGrainStorageFencingOptions"/> and its validator
/// (issue #4200).
/// </summary>
[TestFixture]
public sealed class LatticeGrainStorageFencingOptionsTests
{
    private static readonly LatticeGrainStorageFencingOptionsValidator Validator = new();

    [Test]
    public void Defaults_reject_with_a_thirty_second_timeout()
    {
        var options = new LatticeGrainStorageFencingOptions();

        Assert.That(options.Mode, Is.EqualTo(LatticeGrainStorageFencingMode.Reject));
        Assert.That(options.ProbeTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(LatticeGrainStorageFencingOptions.DefaultProbeTimeout, Is.EqualTo(options.ProbeTimeout));
        Assert.That(Validator.Validate(null, options).Succeeded, Is.True);
    }

    [TestCase(LatticeGrainStorageFencingMode.Warn)]
    [TestCase(LatticeGrainStorageFencingMode.Reject)]
    [TestCase(LatticeGrainStorageFencingMode.Disabled)]
    public void Validate_every_defined_mode_succeeds(LatticeGrainStorageFencingMode mode)
    {
        var result = Validator.Validate(null, new LatticeGrainStorageFencingOptions { Mode = mode });

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_undefined_mode_fails()
    {
        var result = Validator.Validate(null, new LatticeGrainStorageFencingOptions { Mode = (LatticeGrainStorageFencingMode)99 });

        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeGrainStorageFencingOptions.Mode)));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_non_positive_timeout_fails(int seconds)
    {
        var result = Validator.Validate(
            null,
            new LatticeGrainStorageFencingOptions { ProbeTimeout = TimeSpan.FromSeconds(seconds) });

        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeGrainStorageFencingOptions.ProbeTimeout)));
    }
}
