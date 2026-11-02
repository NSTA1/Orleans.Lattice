using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

/// <summary>
/// The Explorer's chrome as a journey drives it (issue #5360): sign-in and sign-out, the
/// identity menu, the directory spine (with the compact <b>Directory</b> and <b>Menu</b>
/// sheets), the address line and its completions, the command palette, the tenant switcher,
/// the appearance controls, the connection indicator, and the denied and hidden states.
/// </summary>
/// <remarks>
/// <para>
/// Every locator is a role, an accessible name or a <c>data-lt-*</c> hook the Explorer ships,
/// never a styling class, so a restyle cannot break a journey and a broken role cannot pass
/// one. Each member works at both widths: where the expanded header shows a control, the
/// compact one folds it into the <b>Menu</b> sheet or the <b>Directory</b> sheet, and the
/// page object opens the sheet itself.
/// </para>
/// <para>Area page objects belong to the journeys, not here.</para>
/// </remarks>
public sealed partial class ShellPage
{
    /// <summary>The heading of the page the Explorer renders for an address nothing lives at.</summary>
    public const string NotFoundHeading = "Nothing lives at this address";

    /// <summary>What the sign-in dialog says when the cluster refuses a username and password (#5415).</summary>
    public const string PasswordRefusedMessage = "The cluster did not accept this username and password.";

    /// <summary>Creates the page object over <paramref name="page"/>.</summary>
    /// <param name="page">A page showing the Explorer.</param>
    public ShellPage(IPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        Page = page;
    }

    /// <summary>The page.</summary>
    public IPage Page { get; }

    /// <summary>The header landmark.</summary>
    public ILocator Banner => Page.GetByRole(AriaRole.Banner);

    /// <summary>The main landmark.</summary>
    public ILocator Content => Page.Locator("main#lt-shell-content");

    /// <summary>The page's level-one heading.</summary>
    public ILocator Heading => Content.Locator("h1").First;

    /// <summary>The compact header's <b>Directory</b> button, which opens the directory sheet.</summary>
    public ILocator DirectoryButton => Banner.GetByRole(AriaRole.Button, new() { Name = "Directory", Exact = true });

    /// <summary>The compact header's <b>Menu</b> button, which opens the overflow sheet.</summary>
    public ILocator MenuButton => Banner.GetByRole(AriaRole.Button, new() { Name = "Menu", Exact = true });

    /// <summary>The compact directory sheet.</summary>
    public ILocator DirectorySheet => Page.GetByRole(AriaRole.Dialog, new() { Name = "Directory" });

    /// <summary>The compact overflow sheet: connection, session and appearance.</summary>
    public ILocator MenuSheet => Page.GetByRole(AriaRole.Dialog, new() { Name = "Menu" });

    /// <summary>The directory spine, wherever it is shown (beside the content, or in its sheet).</summary>
    public ILocator Spine => Page.GetByRole(AriaRole.Navigation, new() { Name = "Estate" });

    /// <summary>The toast messages on screen.</summary>
    public ILocator Toasts => Page.Locator(".lt-toast .lt-toast__message");

    /// <summary>The live region's latest announcement: read by a screen reader, never drawn.</summary>
    public ILocator Announcement => Page.Locator(".lt-toasts > .lt-visually-hidden");

    /// <summary>
    /// The widest viewport (exclusive) at which the Explorer draws its compact chrome. A
    /// journey runs at 1440 (expanded) or 390 (compact), both far from the edge.
    /// </summary>
    public const int CompactBelow = 600;

    /// <summary>
    /// Whether the Explorer is drawing its compact chrome, decided by the viewport and then
    /// awaited: until the circuit has measured the width the expanded frame renders, so a
    /// compact page is not answered until its <b>Menu</b> button is shown.
    /// </summary>
    public async Task<bool> IsCompactAsync()
    {
        if (Page.ViewportSize is not { Width: < CompactBelow })
        {
            return false;
        }

        await Expect(MenuButton).ToBeVisibleAsync();
        return true;
    }

    /// <summary>Navigates to <paramref name="path"/> and waits for the live circuit.</summary>
    /// <param name="path">A head-relative path, such as <c>/data</c>.</param>
    public Task GotoAsync(string path) => JourneyBrowser.GotoAsync(Page, path);

    /// <summary>Waits for the page's heading to read <paramref name="heading"/>.</summary>
    /// <param name="heading">The heading text.</param>
    public Task ExpectHeadingAsync(string heading) => Expect(Heading).ToHaveTextAsync(heading);

    /// <summary>
    /// Activates <paramref name="control"/>, which leaves the current document (a form post and
    /// its redirect), and waits for the next document's live circuit.
    /// </summary>
    /// <param name="control">The control that navigates.</param>
    public async Task SubmitAndWaitForNextDocumentAsync(ILocator control)
    {
        ArgumentNullException.ThrowIfNull(control);
        await Page.EvaluateAsync("() => { window.__ltLeaving = true; }");
        await control.ClickAsync();
        await JourneyBrowser.WaitForInteractiveAsync(Page);
    }

    // ----- Sign-in, sign-out and the identity menu -----

    /// <summary>The sign-in dialog, once open.</summary>
    public ILocator SignInDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "Sign in" });

    /// <summary>The expanded header's session button for <paramref name="user"/>.</summary>
    /// <param name="user">The user name.</param>
    public ILocator SessionButton(string user) => Banner.GetByRole(AriaRole.Button, new() { Name = $"Your session: {user}" });

    /// <summary>Opens the sign-in dialog from the header, or from the <b>Menu</b> sheet when compact; a dialog already open (reopened after a refusal) is used as it is.</summary>
    public async Task<ILocator> OpenSignInAsync()
    {
        if (await SignInDialog.IsVisibleAsync())
        {
            return SignInDialog;
        }

        if (await IsCompactAsync())
        {
            await OpenMenuAsync();
            await MenuSheet.GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First.ClickAsync();
        }
        else
        {
            await Banner.GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First.ClickAsync();
        }

        await Expect(SignInDialog).ToBeVisibleAsync();
        return SignInDialog;
    }

    /// <summary>
    /// Fills and submits the Explorer's own sign-in form (its server form post and redirect)
    /// and waits for the next document's live circuit. Whether the cluster accepts the
    /// credential is the caller's to assert.
    /// </summary>
    /// <param name="user">The user name.</param>
    /// <param name="password">The password.</param>
    public async Task SubmitSignInAsync(string user, string password)
    {
        ArgumentNullException.ThrowIfNull(user);
        ArgumentNullException.ThrowIfNull(password);
        var dialog = await OpenSignInAsync();
        await dialog.GetByLabel("Username").FillAsync(user);
        await dialog.GetByLabel("Password").FillAsync(password);
        await SubmitAndWaitForNextDocumentAsync(dialog.Locator("form[method=post] button[type=submit]"));
    }

    /// <summary>Signs <paramref name="user"/> in through the sign-in form and waits until the chrome names them.</summary>
    /// <param name="user">The user name.</param>
    /// <param name="password">The password.</param>
    public async Task SignInAsync(string user, string password)
    {
        await SubmitSignInAsync(user, password);
        await ExpectSignedInAsync(user);
    }

    /// <summary>Asserts the chrome names <paramref name="user"/> as the signed-in identity.</summary>
    /// <param name="user">The user name.</param>
    public async Task ExpectSignedInAsync(string user)
    {
        if (await IsCompactAsync())
        {
            var session = await OpenIdentityMenuAsync();
            await Expect(session).ToContainTextAsync(user);
            await CloseMenuAsync();
            return;
        }

        await Expect(SessionButton(user)).ToBeVisibleAsync();
    }

    /// <summary>
    /// Asserts the cluster refused the credential just submitted: the sign-in dialog has reopened
    /// and says <see cref="PasswordRefusedMessage"/>, and the refusal notice has left the address.
    /// </summary>
    public async Task ExpectSignInRefusedAsync()
    {
        await Expect(SignInDialog.GetByRole(AriaRole.Alert)).ToContainTextAsync(PasswordRefusedMessage);
        await Expect(Page).Not.ToHaveURLAsync(new Regex("signin="));
    }

    /// <summary>Closes the sign-in dialog with Escape, if it is open.</summary>
    public async Task CloseSignInAsync()
    {
        if (await SignInDialog.IsVisibleAsync())
        {
            await Page.Keyboard.PressAsync("Escape");
            await Expect(SignInDialog).ToBeHiddenAsync();
        }
    }

    /// <summary>Asserts nobody is signed in: the chrome offers <b>Sign in</b>.</summary>
    public async Task ExpectSignedOutAsync()
    {
        if (await IsCompactAsync())
        {
            await OpenMenuAsync();
            await Expect(MenuSheet.GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First).ToBeVisibleAsync();
            await CloseMenuAsync();
            return;
        }

        await Expect(Banner.GetByRole(AriaRole.Button, new() { Name = "Sign in", Exact = true }).First).ToBeVisibleAsync();
    }

    /// <summary>
    /// Opens the identity menu and returns its content: the <b>Your session</b> dialog when
    /// expanded, its section of the <b>Menu</b> sheet when compact.
    /// </summary>
    public async Task<ILocator> OpenIdentityMenuAsync()
    {
        if (await IsCompactAsync())
        {
            await OpenMenuAsync();
            var section = MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Your session" });
            await Expect(section).ToBeVisibleAsync();
            return section;
        }

        await Banner.GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^Your session: ") }).ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "Your session" });
        await Expect(dialog).ToBeVisibleAsync();
        return dialog;
    }

    /// <summary>Signs out from the identity menu (a server form post) and waits until the chrome offers <b>Sign in</b>.</summary>
    public async Task SignOutAsync()
    {
        var session = await OpenIdentityMenuAsync();
        await SubmitAndWaitForNextDocumentAsync(session.GetByRole(AriaRole.Button, new() { Name = "Sign out" }));
        await ExpectSignedOutAsync();
    }

    // ----- The connection indicator -----

    /// <summary>
    /// Asserts the connection indicator reads <paramref name="state"/> (<c>Connected</c>,
    /// <c>Sign-in required</c>, <c>Disconnected</c>, ...): in the header, or in the <b>Menu</b>
    /// sheet when compact.
    /// </summary>
    /// <param name="state">The status in words, or a pattern for it.</param>
    public async Task ExpectConnectionAsync(string state)
    {
        ArgumentNullException.ThrowIfNull(state);
        if (await IsCompactAsync())
        {
            await OpenMenuAsync();
            await Expect(MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Connection" }).GetByRole(AriaRole.Status).First).ToContainTextAsync(state);
            await CloseMenuAsync();
            return;
        }

        await Expect(Banner.GetByRole(AriaRole.Status).First).ToContainTextAsync(state);
    }

    // ----- The compact sheets -----

    /// <summary>Opens the compact <b>Menu</b> sheet, unless it is open.</summary>
    public async Task OpenMenuAsync()
    {
        if (!await MenuSheet.IsVisibleAsync())
        {
            await MenuButton.ClickAsync();
        }

        await Expect(MenuSheet).ToBeVisibleAsync();
    }

    /// <summary>Closes the compact <b>Menu</b> sheet, if it is open.</summary>
    public async Task CloseMenuAsync()
    {
        if (await MenuSheet.IsVisibleAsync())
        {
            await Page.Keyboard.PressAsync("Escape");
            await Expect(MenuSheet).ToBeHiddenAsync();
        }
    }

    /// <summary>Opens the compact <b>Directory</b> sheet, unless it is open.</summary>
    public async Task OpenDirectoryAsync()
    {
        if (!await DirectorySheet.IsVisibleAsync())
        {
            await DirectoryButton.ClickAsync();
        }

        await Expect(DirectorySheet).ToBeVisibleAsync();
    }

    /// <summary>Closes the compact <b>Directory</b> sheet, if it is open.</summary>
    public async Task CloseDirectoryAsync()
    {
        if (await DirectorySheet.IsVisibleAsync())
        {
            await Page.Keyboard.PressAsync("Escape");
            await Expect(DirectorySheet).ToBeHiddenAsync();
        }
    }
}
