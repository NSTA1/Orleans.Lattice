namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeIdentityDirectoryOptionsValidator"/>: the
/// page-size and default-vs-maximum guards.
/// </summary>
public class LatticeIdentityDirectoryOptionsValidatorTests
{
    private static bool Validate(LatticeIdentityDirectoryOptions options) =>
        new LatticeIdentityDirectoryOptionsValidator().Validate(null, options).Succeeded;

    [Test]
    public void Validate_default_options_succeeds()
    {
        Assert.That(Validate(new LatticeIdentityDirectoryOptions()), Is.True);
    }

    [Test]
    public void Validate_custom_valid_options_succeeds()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 10, MaxPageSize = 10 };

        Assert.That(Validate(options), Is.True);
    }

    [Test]
    public void Validate_zero_default_page_size_fails()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 0 };

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_negative_default_page_size_fails()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = -1 };

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_zero_max_page_size_fails()
    {
        var options = new LatticeIdentityDirectoryOptions { MaxPageSize = 0 };

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_negative_max_page_size_fails()
    {
        var options = new LatticeIdentityDirectoryOptions { MaxPageSize = -5 };

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_default_exceeding_max_fails()
    {
        var options = new LatticeIdentityDirectoryOptions { DefaultPageSize = 200, MaxPageSize = 100 };

        Assert.That(Validate(options), Is.False);
    }

    [Test]
    public void Validate_null_options_throws()
    {
        Assert.That(
            () => new LatticeIdentityDirectoryOptionsValidator().Validate(null, null!),
            Throws.ArgumentNullException);
    }
}
