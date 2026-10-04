using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class ExplorerAccessTokenTests
{
    [Test]
    public void ToAuthorizationHeader_defaultsToBearerScheme()
    {
        var token = new ExplorerAccessToken { Token = "abc", ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5) };

        Assert.That(token.ToAuthorizationHeader(), Is.EqualTo("Bearer abc"));
    }

    [Test]
    public void ToString_doesNotDiscloseTheToken()
    {
        var token = new ExplorerAccessToken { Token = "s3cr3t-bearer-value", ExpiresOn = DateTimeOffset.UtcNow };

        Assert.That(token.ToString(), Does.Not.Contain("s3cr3t-bearer-value"));
    }

    [Test]
    public void ToString_doesNotVaryWithTheTokenLength()
    {
        var expiry = DateTimeOffset.UnixEpoch;
        var shortest = new ExplorerAccessToken { Token = "a", ExpiresOn = expiry }.ToString();
        var longest = new ExplorerAccessToken { Token = new string('a', 512), ExpiresOn = expiry }.ToString();

        Assert.That(shortest, Is.EqualTo(longest));
    }

    [Test]
    public void ToString_stillNamesTheTypeTheSchemeAndTheExpiry()
    {
        var token = new ExplorerAccessToken { Token = "abc", ExpiresOn = DateTimeOffset.UnixEpoch, Scheme = "DPoP" };

        Assert.Multiple(() =>
        {
            Assert.That(token.ToString(), Does.Contain(nameof(ExplorerAccessToken)));
            Assert.That(token.ToString(), Does.Contain("DPoP"));
            Assert.That(token.ToString(), Does.Contain("1970"));
        });
    }

    [Test]
    public void Redacting_theDescriptionLeavesValueEqualityIntact()
    {
        var expiry = DateTimeOffset.UnixEpoch;

        Assert.Multiple(() =>
        {
            Assert.That(
                new ExplorerAccessToken { Token = "abc", ExpiresOn = expiry },
                Is.EqualTo(new ExplorerAccessToken { Token = "abc", ExpiresOn = expiry }));
            Assert.That(
                new ExplorerAccessToken { Token = "abc", ExpiresOn = expiry },
                Is.Not.EqualTo(new ExplorerAccessToken { Token = "xyz", ExpiresOn = expiry }));
        });
    }
}
