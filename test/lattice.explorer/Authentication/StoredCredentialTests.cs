using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Regression tests for <see cref="StoredCredential"/>'s redacted description.
/// A record's compiler-generated <see cref="object.ToString"/> prints every
/// property, so leaving it in place put the plaintext password into the first
/// log line, exception message or diagnostic dump that formatted the
/// credential - the same hazard
/// <c>Orleans.Lattice.Api.Mcp.RepoContext.Source.RepoContextGitCredential</c>
/// already documents avoiding.
/// </summary>
[TestFixture]
public class StoredCredentialTests
{
    private const string Password = "correct-horse-battery-staple";

    [Test]
    public void ToString_does_not_disclose_the_password()
    {
        var credential = new StoredCredential("dana", Password);

        Assert.That(credential.ToString(), Does.Not.Contain(Password));
    }

    [Test]
    public void ToString_does_not_vary_with_the_password_length()
    {
        var shortest = new StoredCredential("dana", "a").ToString();
        var longest = new StoredCredential("dana", new string('a', 512)).ToString();

        Assert.That(shortest, Is.EqualTo(longest));
    }

    [Test]
    public void ToString_still_names_the_type_and_the_username()
    {
        var description = new StoredCredential("dana", Password).ToString();

        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain(nameof(StoredCredential)));
            Assert.That(description, Does.Contain("dana"));
        });
    }

    [Test]
    public void Redacting_the_description_leaves_value_equality_intact()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new StoredCredential("dana", Password), Is.EqualTo(new StoredCredential("dana", Password)));
            Assert.That(new StoredCredential("dana", Password), Is.Not.EqualTo(new StoredCredential("dana", "other")));
        });
    }

    [Test]
    public void The_password_is_still_readable_through_the_property()
    {
        Assert.That(new StoredCredential("dana", Password).Password, Is.EqualTo(Password));
    }
}
