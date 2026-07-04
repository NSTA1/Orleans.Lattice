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
    public void ToAuthorizationHeader_honoursCustomScheme()
    {
        var token = new ExplorerAccessToken { Token = "abc", ExpiresOn = DateTimeOffset.UtcNow, Scheme = "DPoP" };

        Assert.That(token.ToAuthorizationHeader(), Is.EqualTo("DPoP abc"));
    }
}
