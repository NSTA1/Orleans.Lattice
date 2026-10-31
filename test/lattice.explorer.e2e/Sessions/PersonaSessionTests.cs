using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

/// <summary>
/// Personas and their sessions on the Contoso estate (#5360): a wrong password is refused and
/// the right one signs in, through the Explorer's own form; two personas signed in at once,
/// each in a browser context of their own, see the spine their own rights draw; and the shell
/// page object drives the chrome at both widths.
/// </summary>
[TestFixture]
[Journey(JourneyId)]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class PersonaSessionTests
{
    private const string JourneyId = "f2-personas";

    private JourneyEstate _estate = null!;

    /// <summary>Boots the Contoso west region once for the fixture, with the whole Contoso cast in its directory.</summary>
    [OneTimeSetUp]
    public async Task StartEstateAsync() => _estate = await JourneyEstate.StartAsync();

    /// <summary>Renders the journey's manifest beside its screenshots, then stops the estate.</summary>
    [OneTimeTearDown]
    public async Task StopEstateAsync()
    {
        try
        {
            await RenderManifestAsync(JourneyRecorder.For(JourneyId));
        }
        finally
        {
            if (_estate is not null)
            {
                await _estate.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Olivia mistypes her password: the Explorer's form posts it, the cluster refuses it, and
    /// nothing of the estate is opened to her. With the right password she is signed in.
    /// </summary>
    [Test]
    [Order(1)]
    [JourneyStep("sign-in", "Olivia signs in through the Explorer's own form")]
    public async Task A_wrong_password_is_refused_and_the_right_one_signs_in()
    {
        await using var olivia = await _estate.OpenSessionAsync(Personas.Olivia);
        await olivia.Shell.ExpectSignedOutAsync();
        await olivia.SubstepAsync("signed-out", "Olivia arrives at the Explorer, signed out");

        await olivia.Shell.SubmitSignInAsync(olivia.Persona.Id, olivia.Persona.Password + "-mistyped");
        await olivia.Shell.ExpectConnectionAsync("Sign-in required");
        var refused = await olivia.Shell.ReadSpineAsync();
        Assert.That(refused.CanOpen("access"), Is.False, $"a refused password opens nothing: {refused}");
        await olivia.SubstepAsync("wrong-password-refused", "A mistyped password: the cluster refuses it, and the Explorer asks her to sign in");

        // Whichever the chrome offers after a refusal - Sign in, or the session to sign out of.
        if (await olivia.Shell.SessionButton(olivia.Persona.Id).IsVisibleAsync())
        {
            await olivia.Shell.SignOutAsync();
        }

        await olivia.SignInAsync();
        await olivia.Shell.ExpectConnectionAsync("Connected");
        var spine = await olivia.Shell.ReadSpineAsync();
        Assert.That(spine.CanOpen("access"), Is.True, $"the bootstrap administrator administers access: {spine}");
        await olivia.SubstepAsync("signed-in", "With the right password Olivia is signed in, and the whole estate is open to her");
    }

    /// <summary>
    /// Olivia and Nina are signed in at the same time, each in a browser of their own. Olivia,
    /// the bootstrap administrator, can open Access; Nina, a new starter with no grants, cannot.
    /// Neither sign-in disturbs the other.
    /// </summary>
    [Test]
    [Order(2)]
    [JourneyStep("two-personas", "Olivia and Nina signed in at once")]
    public async Task Olivia_and_nina_signed_in_at_once_see_different_spines()
    {
        await using var olivia = await _estate.SignInAsAsync(Personas.Olivia);
        await using var nina = await _estate.SignInAsAsync(Personas.Nina);

        var olivias = await olivia.Shell.ReadSpineAsync();
        var ninas = await nina.Shell.ReadSpineAsync();
        await olivia.SubstepAsync("olivia-console", "Olivia's console: the bootstrap administrator's spine");
        await nina.SubstepAsync("nina-console", "Nina's console, at the same moment: a new starter with no grants");

        Assert.Multiple(() =>
        {
            Assert.That(olivias.CanOpen("access"), Is.True, $"olivia: {olivias}");
            Assert.That(ninas.CanOpen("access"), Is.False, $"nina: {ninas}");
            Assert.That(ninas.Open, Is.Not.EquivalentTo(olivias.Open), "rights that differ draw spines that differ");
        });

        await nina.Shell.ExpectAreaWithheldAsync("access");
        await nina.SubstepAsync("nina-access-withheld", "Nina follows a link to Access: it is not hers to see");

        await olivia.Shell.ExpectSignedInAsync(olivia.Persona.Id);
        await olivia.Shell.GoToAreaAsync("access");
        await olivia.Shell.ExpectHeadingAsync("Access");
        await olivia.SubstepAsync("olivia-access", "Olivia, still signed in beside Nina, opens Access");
    }

    /// <summary>
    /// The shell page object drives the chrome: the identity menu, the address line's
    /// completions, the command palette, a command, the appearance controls and the spine; then,
    /// on a phone, the Directory and Menu sheets, the connection indicator, the full-screen
    /// palette and sign-out.
    /// </summary>
    [Test]
    [Order(3)]
    [JourneyStep("shell-chrome", "The Explorer's chrome")]
    public async Task The_shell_page_object_drives_the_chrome_at_both_widths()
    {
        await using (var olivia = await _estate.SignInAsAsync(Personas.Olivia))
        {
            var shell = olivia.Shell;
            var session = await shell.OpenIdentityMenuAsync();
            await Expect(session).ToContainTextAsync(olivia.Persona.Id);
            await olivia.SubstepAsync("identity-menu", "The identity menu: who Olivia is signed in as, and to which cluster");
            await olivia.Page.Keyboard.PressAsync("Escape");
            await Expect(session).ToBeHiddenAsync();

            var completions = await shell.CompleteAsync("/da");
            Assert.That(completions, Is.Not.Empty, "a partial address completes");
            await olivia.SubstepAsync("address-completions", "The address line completes a partial address");
            await shell.CloseAddressLineAsync();

            var commands = await shell.OpenCommandPaletteAsync();
            Assert.That(commands, Has.Some.Contains("Go to Access"), "the palette lists the areas Olivia may open");
            await olivia.SubstepAsync("command-palette", "The command palette: every command Olivia may run");
            await shell.CloseAddressLineAsync();

            await shell.RunCommandAsync("Go to Cluster");
            await shell.ExpectHeadingAsync("Cluster");

            await shell.ChooseAppearanceAsync(AppearanceCommands.Board);
            await shell.ExpectMaterialAsync(board: true);
            await olivia.SubstepAsync("board-material", "The Cluster area in the Board material, chosen from the appearance menu");
            await shell.ChooseAppearanceAsync(AppearanceCommands.Paper);
            await shell.ExpectMaterialAsync(board: false);

            await shell.GoToAreaAsync("data");
            await shell.ExpectHeadingAsync("Data");
        }

        // A phone: compact chrome. Exercised without screenshots, which a run takes at one viewport.
        await using var phone = await _estate.SignInAsAsync(Personas.Olivia, options: new PersonaSessionOptions { Viewport = JourneyViewport.Narrow });
        Assert.That(await phone.Shell.IsCompactAsync(), Is.True);
        await phone.Shell.ExpectConnectionAsync("Connected");
        var spine = await phone.Shell.ReadSpineAsync();
        Assert.That(spine.CanOpen("access"), Is.True, $"the directory sheet lists the same stops: {spine}");
        await phone.Shell.GoToAreaAsync("access");
        await phone.Shell.ExpectHeadingAsync("Access");

        await phone.Shell.OpenCommandPaletteAsync();
        await Expect(phone.Shell.PaletteSheet).ToBeVisibleAsync();
        await phone.Shell.CloseAddressLineAsync();

        await phone.Shell.SignOutAsync();
        await phone.Shell.ExpectSignedOutAsync();
    }

    private static async Task RenderManifestAsync(JourneyRecorder recorder)
    {
        var browser = await Browser.JourneyBrowser.ChromiumAsync();
        await using var context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize { Width = 1440, Height = 900 },
            DeviceScaleFactor = 1,
        });
        var page = await context.NewPageAsync();
        await page.SetContentAsync(recorder.Manifest().ToHtml());
        await page.ScreenshotAsync(new PageScreenshotOptions
        {
            Path = Path.Combine(JourneyRecorder.Root, recorder.JourneyId + ".manifest.png"),
            FullPage = true,
        });
    }
}
