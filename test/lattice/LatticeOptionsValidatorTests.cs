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

    [Fact]
    public void Valid_defaults_pass()
    {
        var result = Validate(_ => { });
        Assert.True(result.Succeeded);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ShardCount_must_be_positive(int value)
    {
        var result = Validate(o => o.ShardCount = value);
        Assert.True(result.Failed);
        Assert.Contains("ShardCount", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void MaxLeafKeys_must_be_greater_than_one(int value)
    {
        var result = Validate(o => o.MaxLeafKeys = value);
        Assert.True(result.Failed);
        Assert.Contains("MaxLeafKeys", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void MaxInternalChildren_must_be_greater_than_two(int value)
    {
        var result = Validate(o => o.MaxInternalChildren = value);
        Assert.True(result.Failed);
        Assert.Contains("MaxInternalChildren", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void KeysPageSize_must_be_positive(int value)
    {
        var result = Validate(o => o.KeysPageSize = value);
        Assert.True(result.Failed);
        Assert.Contains("KeysPageSize", result.FailureMessage);
    }

    [Fact]
    public void Valid_custom_values_pass()
    {
        var result = Validate(o =>
        {
            o.ShardCount = 1;
            o.MaxLeafKeys = 2;
            o.MaxInternalChildren = 3;
            o.KeysPageSize = 1;
        });
        Assert.True(result.Succeeded);
    }
}
