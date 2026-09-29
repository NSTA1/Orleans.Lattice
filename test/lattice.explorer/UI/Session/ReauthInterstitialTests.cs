using Bunit;
using Bunit.TestDoubles;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The re-authentication interstitial: an undismissable alert that sends the
/// browser through a fresh interactive sign-in with a full page load and resumes
/// at the address the operator was on.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReauthInterstitialTests : SessionTestContext
{
    private const string Address = "/data/a/crm/orders?key=k%201";

    [Test]
    public void It_is_an_alert_that_cannot_be_dismissed()
    {
        var cut = Render<ReauthInterstitial>();

        cut.Find("[role=alertdialog]").KeyDown("Escape");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Is.EqualTo(new[] { "Sign in again" }));
        });
    }

    [Test]
    public void Signing_in_again_goes_to_the_challenge_and_returns_to_the_same_address()
    {
        Services.AddSingleton(new ExplorerReauthOptions { ChallengePath = "/explorer-entra/reauth" });
        var navigation = Navigate(Address);
        var cut = Render<ReauthInterstitial>();

        cut.Find("button").Click();

        var last = navigation.History.First();
        Assert.Multiple(() =>
        {
            Assert.That(last.Uri, Is.EqualTo("/explorer-entra/reauth?returnUrl=" + Uri.EscapeDataString(Address)));
            Assert.That(last.Options.ForceLoad, Is.True, "only a full page load forces a fresh authorization-code redemption");
        });
    }

    [Test]
    public void A_custom_return_url_parameter_is_honoured()
    {
        Services.AddSingleton(new ExplorerReauthOptions { ChallengePath = "/reauth?prompt=login", ReturnUrlParameter = "next" });
        var navigation = Navigate(Address);
        var cut = Render<ReauthInterstitial>();

        cut.Find("button").Click();

        Assert.That(navigation.History.First().Uri, Is.EqualTo("/reauth?prompt=login&next=" + Uri.EscapeDataString(Address)));
    }

    [Test]
    public void With_no_challenge_endpoint_it_reloads_the_current_address()
    {
        var navigation = Navigate(Address);
        var cut = Render<ReauthInterstitial>();

        cut.Find("button").Click();

        var last = navigation.History.First();
        Assert.Multiple(() =>
        {
            Assert.That(last.Uri, Is.EqualTo(Address));
            Assert.That(last.Options.ForceLoad, Is.True);
        });
    }

    private BunitNavigationManager Navigate(string address)
    {
        var navigation = (BunitNavigationManager)Services.GetRequiredService<NavigationManager>();
        navigation.NavigateTo(address);
        return navigation;
    }
}
