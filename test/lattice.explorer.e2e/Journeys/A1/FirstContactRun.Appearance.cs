using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>Steps 7 to 9: appearance across a sign-out, a deep link signed out, and Reset view.</summary>
internal sealed partial class FirstContactRun
{
    /// <summary>A deep link into the Cluster area.</summary>
    private const string DeepLink = "/cluster/trees";

    /// <summary>The appearance every persona starts with, and the one Reset view returns to.</summary>
    private const string DefaultAppearance = "system/system/comfortable";

    /// <summary>
    /// A1.7: Olivia chooses the Board theme and compact density from the appearance menu, signs
    /// out from the identity menu, signs in again, and finds her choice kept.
    /// </summary>
    public Task ChooseAppearanceAsync() => StepAsync("7", async () =>
    {
        await Shell.ChooseAppearanceAsync(AppearanceCommands.Board, AppearanceCommands.Compact);
        await Shell.ExpectMaterialAsync(board: true);
        await Appearance.ExpectCompactAsync(compact: true);
        Assert.That(await Appearance.ReadAsync(), Is.EqualTo(ContosoAppearance.OliviaAfterA1), "the appearance menu shows Board and compact pressed");
        Coverage.Hit("use-case:shell/appearance-menu");
        if (!await Shell.IsCompactAsync())
        {
            Coverage.Hit("command:appearance.menu");
        }

        await CheckpointAsync("7", "board-compact", "From the appearance menu, Olivia chooses the Board theme and compact density");

        var session = await Shell.OpenIdentityMenuAsync();
        await Expect(session).ToContainTextAsync(Olivia.Persona.Id);
        await Expect(session.GetByRole(AriaRole.Button, new() { Name = "Sign out" })).ToBeVisibleAsync();
        await CheckpointAsync("7", "identity-menu", "The identity menu names Olivia and offers Reset view and Sign out");
        await Shell.SubmitAndWaitForNextDocumentAsync(session.GetByRole(AriaRole.Button, new() { Name = "Sign out" }));
        await Shell.ExpectSignedOutAsync();
        await Shell.ExpectConnectionAsync("Sign-in required");
        await CheckpointAsync("7", "signed-out", "Signed out: the header offers Sign in again");

        await Olivia.SignInAsync();
        await Shell.ExpectConnectionAsync("Connected");
        await Shell.ExpectMaterialAsync(board: true);
        await Appearance.ExpectCompactAsync(compact: true);
        Assert.That(await Appearance.ReadAsync(), Is.EqualTo(ContosoAppearance.OliviaAfterA1), "the choice survives signing out and in");
        Coverage.Hit("use-case:shell/session-chrome");
        Coverage.Hit("use-case:shell/sign-in-endpoints");
        await CheckpointAsync("7", "kept", "Signed in again, Olivia finds Board and compact as she left them");
    });

    /// <summary>
    /// A1.8: signed out, Olivia follows a link to <c>/cluster/trees</c> and signs in from it. She
    /// should land on that address; today she lands on Home (#5481).
    /// </summary>
    public Task FollowDeepLinkSignedOutAsync() => StepAsync("8", async () =>
    {
        await Shell.SignOutAsync();
        await Shell.GotoAsync(DeepLink);
        await Shell.ExpectSignedOutAsync();
        await ExpectAddressAsync(DeepLink);
        await CheckpointAsync("8", "signed-out-deep-link", "Signed out, Olivia follows a link to /cluster/trees: the Explorer asks her to sign in");

        await Shell.SubmitSignInAsync(Olivia.Persona.Id, Olivia.Persona.Password);
        await Shell.ExpectSignedInAsync(Olivia.Persona.Id);
        await new KnownDefect("#5481", "Signing in from a deep link lands on Home instead of the address asked for").ReproducesAsync(async () =>
        {
            await ExpectHomeAsync();
            await Expect(Page).Not.ToHaveURLAsync(new Regex(Regex.Escape(DeepLink)));
        });
        await CheckpointAsync("8", "signed-in", "Signed in from the deep link, Olivia lands on Home rather than /cluster/trees (#5481)");
    });

    /// <summary>
    /// A1.9: <c>/reset</c> and <b>Reset view</b> forget everything the Explorer remembers about
    /// Olivia's view - her appearance included - and leave her signed in. She then chooses Board
    /// and compact again, which is the appearance A1 leaves her with.
    /// </summary>
    public Task ResetViewAsync() => StepAsync("9", async () =>
    {
        await Shell.GotoAsync("/reset");
        await Shell.ExpectHeadingAsync("Reset view");
        await Expect(Shell.Content).ToContainTextAsync("the theme you chose");
        await CheckpointAsync("9", "reset-page", "/reset lists what the Explorer remembers, and changes nothing until asked");

        await Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Reset view", Exact = true }).ClickAsync();
        await Expect(Shell.Content.GetByText("The Explorer has forgotten your remembered view.")).ToBeVisibleAsync();
        await Shell.ExpectSignedInAsync(Olivia.Persona.Id);
        Coverage.Hit("use-case:shell/reset-view");
        Coverage.Hit("route:/reset");
        await CheckpointAsync("9", "reset", "Reset view: the remembered view is forgotten, and Olivia is still signed in");

        await Shell.Content.GetByRole(AriaRole.Link, new() { Name = "Back to the Explorer" }).ClickAsync();
        await ExpectHomeAsync();
        await Shell.ExpectConnectionAsync("Connected");
        await Expect(Shell.Root).Not.ToHaveAttributeAsync("data-bs-theme", "dark");
        await Appearance.ExpectCompactAsync(compact: false);
        Assert.That(await Appearance.ReadAsync(), Is.EqualTo(DefaultAppearance), "Reset view returns the appearance to its defaults");
        await CheckpointAsync("9", "defaults", "Back in the Explorer: the default appearance, and the credentials intact");

        await Shell.ChooseAppearanceAsync(AppearanceCommands.Board, AppearanceCommands.Compact);
        await Shell.ExpectMaterialAsync(board: true);
        await Appearance.ExpectCompactAsync(compact: true);
        var appearance = await Appearance.ReadAsync();
        Assert.That(appearance, Is.EqualTo(ContosoAppearance.OliviaAfterA1));
        _context.Record(StoryKeys.OliviaAppearance, appearance);
        await CheckpointAsync("9", "appearance-again", "Olivia chooses Board and compact again: how she will work from now on");
    });
}
