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
        var page = await OpenAsync(world.Head, "/");

        // Signed out: nothing of the cluster is shown.
        await Expect(Shell.Stop(page, "access")).ToHaveCountAsync(0);

        await Shell.SignInAsync(page, WorldIdentities.Admin);
        await Expect(Shell.Stop(page, "access")).ToBeVisibleAsync();
        await Expect(Shell.Stop(page, "cluster")).ToBeVisibleAsync();

        // Sign out from the session menu.
        await Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = $"Your session: {WorldIdentities.Admin}" }).ClickAsync();
        var session = page.GetByRole(AriaRole.Dialog, new() { Name = "Your session" });
        await Expect(session).ToContainTextAsync(WorldIdentities.Admin);
        await Shell.SubmitAndWaitForNextDocumentAsync(page, session.GetByRole(AriaRole.Button, new() { Name = "Sign out" }));

        await Expect(Shell.Banner(page).GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First).ToBeVisibleAsync();
        await Expect(Shell.Stop(page, "access")).ToHaveCountAsync(0);
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
