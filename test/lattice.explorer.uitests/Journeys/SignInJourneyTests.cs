using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Signing in and out through the Explorer's own dialog and its server form post, on a
/// desktop and on a phone, where the sign-in lives in the header's overflow menu.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class SignInJourneyTests : UiTestBase
{
    [Test]
    public async Task A_user_signs_in_is_shown_their_estate_and_signs_out()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/access");

        // Signed out: the cluster's access policy is not shown.
        await ExpectAccessWithheldAsync(page);

        await Shell.SignInAsync(page, WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "access")).ToBeVisibleAsync();
        await Shell.Stop(page, "access").ClickAsync();
        await Expect(Shell.Content(page)).ToContainTextAsync("operators-read-factory-floor");

        // Sign out from the session menu.
        await Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = $"Your session: {WorldIdentities.Admin}" }).ClickAsync();
        var session = page.GetByRole(AriaRole.Dialog, new() { Name = "Your session" });
        await Expect(session).ToContainTextAsync(WorldIdentities.Admin);
        await Shell.SubmitAndWaitForNextDocumentAsync(page, session.GetByRole(AriaRole.Button, new() { Name = "Sign out" }));

        await Expect(Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First).ToBeVisibleAsync();
        await Shell.GotoAsync(page, world.Head, "/access");
        await ExpectAccessWithheldAsync(page);
    }

    private static async Task ExpectAccessWithheldAsync(IPage page)
    {
        await Expect(Shell.Content(page)).ToContainTextAsync(new System.Text.RegularExpressions.Regex($"Not available|{ExplorerAreas.NotFoundHeading}"));
        await Expect(Shell.Content(page)).Not.ToContainTextAsync("operators-read-factory-floor");
    }

    [Test]
    public async Task A_wrong_password_is_refused_at_sign_in_and_the_right_one_signs_in()
    {
        // Issue #5415: a password the cluster refuses used to be shown as a signed-in
        // session holding no grants. It is refused at sign-in instead.
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/");
        await CaptureJourneyStepAsync(page, "d5415-01-signed-out");

        await Shell.OpenSignInAsync(page);
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        await dialog.GetByLabel("Username").FillAsync(WorldIdentities.Admin);
        await dialog.GetByLabel("Password").FillAsync(WorldIdentities.WrongPassword);
        await CaptureJourneyStepAsync(page, "d5415-02-wrong-password-entered");
        await Shell.SubmitAndWaitForNextDocumentAsync(page, dialog.Locator("form[method=post] button[type=submit]"));

        // The dialog reopens and says so; no session was established.
        var reopened = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        await Expect(reopened.GetByRole(AriaRole.Alert)).ToContainTextAsync("The cluster did not accept this username and password.");
        await Expect(page).Not.ToHaveURLAsync(new System.Text.RegularExpressions.Regex("signin="));
        await Expect(Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = $"Your session: {WorldIdentities.Admin}" })).ToHaveCountAsync(0);
        await CaptureJourneyStepAsync(page, "d5415-03-wrong-password-refused");

        // The right password signs in, from the reopened dialog.
        await reopened.GetByLabel("Username").FillAsync(WorldIdentities.Admin);
        await reopened.GetByLabel("Password").FillAsync(WorldIdentities.Password);
        await Shell.SubmitAndWaitForNextDocumentAsync(page, reopened.Locator("form[method=post] button[type=submit]"));
        await Shell.ExpectSignedInAsync(page, WorldIdentities.Admin);
        await Expect(page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" })).ToHaveCountAsync(0);
        await Expect(Shell.Stop(page, "access")).ToBeVisibleAsync();
        await CaptureJourneyStepAsync(page, "d5415-04-right-password-signed-in");
    }

    [Test]
    public async Task A_user_who_signs_in_from_a_deep_link_is_returned_to_it()
    {
        // Issue #5481: the server sign-in form carried no return address, so every
        // sign-in, refused or accepted, landed on Home and the user lost their place.
        var world = await UiHosts.WorldAsync();
        var address = $"/cluster/trees/{ExplorerWorld.DemoTree}?tab=storage";
        var page = await OpenAsync(world.Head, address);
        await CaptureJourneyStepAsync(page, "d5481-01-signed-out-deep-link");

        // A refused password reopens the dialog on the same address.
        await Shell.OpenSignInAsync(page);
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        await dialog.GetByLabel("Username").FillAsync(WorldIdentities.Admin);
        await dialog.GetByLabel("Password").FillAsync(WorldIdentities.WrongPassword);
        await Shell.SubmitAndWaitForNextDocumentAsync(page, dialog.Locator("form[method=post] button[type=submit]"));
        var reopened = page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });
        await Expect(reopened.GetByRole(AriaRole.Alert)).ToContainTextAsync("The cluster did not accept this username and password.");
        await Expect(page).Not.ToHaveURLAsync(new System.Text.RegularExpressions.Regex("signin="));
        var afterRefusal = page.Url;
        await CaptureJourneyStepAsync(page, "d5481-02-wrong-password-refused");

        // The right password signs in and stays on the same address.
        await reopened.GetByLabel("Username").FillAsync(WorldIdentities.Admin);
        await reopened.GetByLabel("Password").FillAsync(WorldIdentities.Password);
        await Shell.SubmitAndWaitForNextDocumentAsync(page, reopened.Locator("form[method=post] button[type=submit]"));
        await Shell.ExpectSignedInAsync(page, WorldIdentities.Admin);
        var afterSignIn = page.Url;
        await CaptureJourneyStepAsync(page, "d5481-03-signed-in");

        Assert.Multiple(() =>
        {
            Assert.That(afterRefusal, Is.EqualTo(world.Head.Url(address)), "a refused sign-in returns to the address it was made from");
            Assert.That(afterSignIn, Is.EqualTo(world.Head.Url(address)), "a sign-in returns to the address it was made from");
        });
    }

    [Test]
    public async Task A_user_signs_in_on_a_phone_from_the_overflow_menu()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/", width: Shell.SmallWidth);
        await Expect(Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true })).ToBeVisibleAsync();

        await Shell.SignInAsync(page, WorldIdentities.Alice);

        await Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true }).ClickAsync();
        var directory = page.GetByRole(AriaRole.Dialog, new() { Name = "Directory" });
        await Expect(directory.Locator("a[data-lt-command=\"go.data\"]")).ToBeVisibleAsync();
        await directory.Locator("a[data-lt-command=\"go.data\"]").ClickAsync();
        await Expect(directory).ToBeHiddenAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Data");
    }
}
