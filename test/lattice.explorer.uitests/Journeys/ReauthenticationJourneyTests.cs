using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// A token sign-in that can no longer be renewed: the Explorer stops, says the session
/// expired, and "Sign in again" brings the user back to the address they were at, where a
/// fresh sign-in resumes the page.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class ReauthenticationJourneyTests : UiTestBase
{
    [Test]
    public async Task Re_authentication_resumes_the_address_the_user_was_at()
    {
        var (head, signIn) = await UiHosts.ReauthAsync();
        signIn.Restore(WorldIdentities.Admin);
        var page = await OpenAsync(head, "/");

        // A token sign-in lives in the circuit, so the rest of the journey moves within it.
        await SignInWithTokenAsync(page);
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync($"/data/{ExplorerWorld.DemoTree}");
        await page.Keyboard.PressAsync("Enter");
        await Expect(Shell.Content(page)).ToContainTextAsync("machine-000");

        // The token can no longer be renewed. The next thing the page asks the cluster -
        // the tree's metrics - raises the interstitial, which cannot be dismissed.
        signIn.Revoke(WorldIdentities.Admin);
        await Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Metrics" }).ClickAsync();
        var expired = page.GetByRole(AriaRole.Alertdialog, new() { Name = "Your session expired" });
        await Expect(expired).ToBeVisibleAsync();
        await page.Keyboard.PressAsync("Escape");
        await Expect(expired).ToBeVisibleAsync();
        var expiredAt = page.Url;

        // Sign in again: the challenge returns the browser to the same address, and a fresh
        // sign-in there resumes it.
        signIn.Restore(WorldIdentities.Admin);
        await Shell.SubmitAndWaitForNextDocumentAsync(page, expired.GetByRole(AriaRole.Button, new() { Name = "Sign in again" }));

        // Signed out, the same page is addressed as a signed-out caller sees it: without the
        // tenant root, which only an operator's addresses carry.
        var expiredUri = new Uri(expiredAt);
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex(
            System.Text.RegularExpressions.Regex.Escape(expiredUri.PathAndQuery.Replace("/t/default", string.Empty, StringComparison.Ordinal)) + "$"));

        await SignInWithTokenAsync(page);
        await Expect(page).ToHaveURLAsync(expiredAt);
        Assert.That(expiredAt, Does.Contain($"/data/{ExplorerWorld.DemoTree}"), "The interstitial was raised somewhere other than the tree page.");
        await Expect(Shell.Heading(page)).ToContainTextAsync(ExplorerWorld.DemoTree);
    }

    private static async Task SignInWithTokenAsync(IPage page)
    {
        await Shell.OpenSignInAsync(page);
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Continue with " + RenewableSignIn.DisplayName }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();
        await Shell.ExpectSignedInAsync(page, WorldIdentities.Admin);
    }
}
