using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Tests the pure challenge-URL builder the re-authentication interstitial uses
/// to decide where to navigate when a sign-in latches into its revoked state.
/// </summary>
[TestFixture]
public class ExplorerReauthChallengeTests
{
    [Test]
    public void BuildUrl_nullOptions_reloadsCurrentPage()
    {
        Assert.That(ExplorerReauthChallenge.BuildUrl(null, "/state/tree"), Is.EqualTo("/state/tree"));
    }

    [Test]
    public void BuildUrl_noChallengePath_reloadsCurrentPage()
    {
        var options = new ExplorerReauthOptions { ChallengePath = null };

        Assert.That(ExplorerReauthChallenge.BuildUrl(options, "/state/tree?x=1"), Is.EqualTo("/state/tree?x=1"));
    }

    [Test]
    public void BuildUrl_emptyChallengePath_reloadsCurrentPage()
    {
        var options = new ExplorerReauthOptions { ChallengePath = "" };

        Assert.That(ExplorerReauthChallenge.BuildUrl(options, "/state/tree"), Is.EqualTo("/state/tree"));
    }

    [Test]
    public void BuildUrl_appendsReturnUrl_encoded()
    {
        var options = new ExplorerReauthOptions { ChallengePath = "/explorer-entra/reauth" };

        Assert.That(
            ExplorerReauthChallenge.BuildUrl(options, "/state/tree?x=1"),
            Is.EqualTo("/explorer-entra/reauth?returnUrl=%2Fstate%2Ftree%3Fx%3D1"));
    }

    [Test]
    public void BuildUrl_challengePathWithQuery_usesAmpersandSeparator()
    {
        var options = new ExplorerReauthOptions { ChallengePath = "/reauth?tenant=contoso" };

        Assert.That(
            ExplorerReauthChallenge.BuildUrl(options, "/state"),
            Is.EqualTo("/reauth?tenant=contoso&returnUrl=%2Fstate"));
    }

    [Test]
    public void BuildUrl_customReturnUrlParameter_isHonoured()
    {
        var options = new ExplorerReauthOptions { ChallengePath = "/reauth", ReturnUrlParameter = "back" };

        Assert.That(
            ExplorerReauthChallenge.BuildUrl(options, "/state"),
            Is.EqualTo("/reauth?back=%2Fstate"));
    }

    [Test]
    public void BuildUrl_appendReturnUrlDisabled_returnsBareChallengePath()
    {
        var options = new ExplorerReauthOptions { ChallengePath = "/reauth", AppendReturnUrl = false };

        Assert.That(ExplorerReauthChallenge.BuildUrl(options, "/state/tree"), Is.EqualTo("/reauth"));
    }

    [Test]
    public void BuildUrl_blankReturnUrlParameter_fallsBackToDefault()
    {
        var options = new ExplorerReauthOptions { ChallengePath = "/reauth", ReturnUrlParameter = "" };

        Assert.That(
            ExplorerReauthChallenge.BuildUrl(options, "/state"),
            Is.EqualTo("/reauth?returnUrl=%2Fstate"));
    }

    [Test]
    public void BuildUrl_nullCurrentLocalPath_throws()
    {
        Assert.That(
            () => ExplorerReauthChallenge.BuildUrl(new ExplorerReauthOptions(), null!),
            Throws.ArgumentNullException);
    }
}
