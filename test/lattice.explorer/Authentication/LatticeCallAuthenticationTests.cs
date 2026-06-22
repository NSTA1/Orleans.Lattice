using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class LatticeCallAuthenticationTests
{
    [Test]
    public void Basic_producesLowerCaseAuthorizationHeader()
    {
        var auth = LatticeCallAuthentication.Basic("alice", "Password1");

        Assert.That(auth.Headers, Is.Not.Null);
        Assert.That(auth.Headers!.ContainsKey("authorization"), Is.True);
    }

    [Test]
    public void Basic_encodesUsernameAndPasswordAsBase64()
    {
        var auth = LatticeCallAuthentication.Basic("alice", "Password1");

        var expected = "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("alice:Password1"));
        Assert.That(auth.Headers!["authorization"], Is.EqualTo(expected));
    }

    [Test]
    public void Basic_setsHasHeadersTrue()
    {
        var auth = LatticeCallAuthentication.Basic("alice", "Password1");

        Assert.That(auth.HasHeaders, Is.True);
    }

    [Test]
    public void Basic_allowsEmptyPassword()
    {
        var auth = LatticeCallAuthentication.Basic("alice", string.Empty);

        var expected = "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("alice:"));
        Assert.That(auth.Headers!["authorization"], Is.EqualTo(expected));
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase(null)]
    public void Basic_emptyUsername_throws(string? username)
    {
        Assert.That(() => LatticeCallAuthentication.Basic(username!, "Password1"), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Basic_nullPassword_throws()
    {
        Assert.That(() => LatticeCallAuthentication.Basic("alice", null!), Throws.ArgumentNullException);
    }
}
