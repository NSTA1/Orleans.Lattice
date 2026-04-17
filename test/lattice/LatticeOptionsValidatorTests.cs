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
    public void ShardCount_must_be_positive(int value)
    {
        var result = Validate(o => o.ShardCount = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain("ShardCount"));
    }

    [TestCase(0)]
    [TestCase(1)]
    public void MaxLeafKeys_must_be_greater_than_one(int value)
    {
        var result = Validate(o => o.MaxLeafKeys = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain("MaxLeafKeys"));
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public void MaxInternalChildren_must_be_greater_than_two(int value)
    {
        var result = Validate(o => o.MaxInternalChildren = value);
        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain("MaxInternalChildren"));
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
            o.ShardCount = 1;
            o.MaxLeafKeys = 2;
            o.MaxInternalChildren = 3;
            o.KeysPageSize = 1;
        });
        Assert.That(result.Succeeded, Is.True);
    }
}
