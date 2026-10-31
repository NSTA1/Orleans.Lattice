using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Regions;

/// <summary>
/// The Fabrikam Cloud estate (#5362): two regions with tenancy and delegated-access
/// administration on boot, and the bootstrap administrator, signed in through the form, sees
/// the Tenancy area's directory.
/// </summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class FabrikamEstateTests : EstateFixture
{
    /// <inheritdoc />
    protected override JourneyEstateOptions Options => EstateProfile.Fabrikam;

    /// <summary>Fabrikam boots both regions, and hal sees Tenancy.</summary>
    [Test]
    public async Task A_Fabrikam_estate_boots_and_the_bootstrap_administrator_sees_Tenancy()
    {
        Assert.That(Estate.Regions.Keys, Is.EquivalentTo(new[] { JourneyEstate.West, JourneyEstate.East }));

        var page = await OpenAsync(Estate.ExplorerUri);
        await JourneyBrowser.GotoAsync(page, "/");
        await JourneyBrowser.SignInAsync(page, EstateProfile.FabrikamAdministrator, Estate.PasswordOf(EstateProfile.FabrikamAdministrator));

        await JourneyBrowser.Stop(page, "tenancy").ClickAsync();
        await Expect(page).ToHaveURLAsync(new Regex("/tenancy"));
        // The address changes before the page renders: wait for the directory itself.
        await Expect(JourneyBrowser.Heading(page)).ToHaveTextAsync("Tenancy");
        await ScreenshotAsync(page, "fabrikam-01-tenancy-directory.png");
    }
}
