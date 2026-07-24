using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Tests the defaults of <see cref="ExplorerReauthOptions"/>, the configuration
/// seam the core UI reads to drive a graceful re-authentication.
/// </summary>
[TestFixture]
public class ExplorerReauthOptionsTests
{
    [Test]
    public void Defaults_areSafe()
    {
        var options = new ExplorerReauthOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.ChallengePath, Is.Null);
            Assert.That(options.AppendReturnUrl, Is.True);
            Assert.That(options.ReturnUrlParameter, Is.EqualTo("returnUrl"));
            Assert.That(ExplorerReauthOptions.DefaultReturnUrlParameter, Is.EqualTo("returnUrl"));
        });
    }

    [Test]
    public void Properties_roundTrip()
    {
        var options = new ExplorerReauthOptions
        {
            ChallengePath = "/explorer-entra/reauth",
            AppendReturnUrl = false,
            ReturnUrlParameter = "back",
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.ChallengePath, Is.EqualTo("/explorer-entra/reauth"));
            Assert.That(options.AppendReturnUrl, Is.False);
            Assert.That(options.ReturnUrlParameter, Is.EqualTo("back"));
        });
    }
}
