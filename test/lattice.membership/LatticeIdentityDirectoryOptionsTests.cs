namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeIdentityDirectoryOptions"/> default values and
/// the provider-neutral knobs.
/// </summary>
public class LatticeIdentityDirectoryOptionsTests
{
    [Test]
    public void Defaults_are_page_size_25_max_100_and_validation_off()
    {
        var options = new LatticeIdentityDirectoryOptions();

        Assert.That(options.DefaultPageSize, Is.EqualTo(25));
        Assert.That(options.MaxPageSize, Is.EqualTo(100));
        Assert.That(options.ValidationRequired, Is.False);
    }

    [Test]
    public void Validation_required_can_be_enabled()
    {
        var options = new LatticeIdentityDirectoryOptions { ValidationRequired = true };

        Assert.That(options.ValidationRequired, Is.True);
    }
}
