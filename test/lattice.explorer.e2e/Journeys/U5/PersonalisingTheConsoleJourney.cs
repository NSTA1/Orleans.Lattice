using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U5;

/// <summary>
/// U5, personalising the console (#5387). Sam, the schema owner, works on a laptop by day and a
/// phone on call. He tries every appearance the console offers and keeps one across a sign-out,
/// uses the console at phone width with its compact navigation, learns the <c>a/</c> completion
/// for the task board U3's team works on and the palette's commands for each area, filtered to
/// what he may do, and finally resets the view. Produces sam's appearance
/// (<see cref="StoryKeys.SamAppearance"/>), read back from the appearance controls.
/// </summary>
[JourneyChapter("U5", After = new[] { "U3" })]
public sealed class PersonalisingTheConsoleJourney : JourneyChapter
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "u5";

    /// <summary>The appearance sam settles on before signing out: Board, high contrast, compact.</summary>
    public static ConsoleAppearance SamsChoice { get; } = new("board", "more", "compact");

    /// <summary>The heading of Home, the estate overview.</summary>
    public const string HomeHeading = "Estate";

    private static readonly JourneyRecorder Recorder = JourneyRecorder.For(JourneyId);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        await using var laptop = await context.Estate.SignInAsAsync(Personas.Sam, JourneyEstate.West);

        await StepAsync("1", () => EveryAppearanceAsync(laptop));
        await StepAsync("2", () => CompactShellAsync(context.Estate));
        await StepAsync("3", () => AppCompletionAsync(laptop));
        await StepAsync("4", () => PaletteCommandsAsync(laptop));
        await StepAsync("5", () => ResetViewAsync(context, laptop));
    }

    /// <summary>U5.1: every theme, contrast and density, each applied at once, and the last kept across sign-out.</summary>
    private static async Task EveryAppearanceAsync(PersonaSession sam)
    {
        const string step = "u5-1";
        var shell = sam.Shell;
        var appearance = new AppearancePage(shell);
        await shell.ExpectHeadingAsync(HomeHeading);
        var address = shell.Page.Url;

        var current = ConsoleAppearance.Default;
        await appearance.ExpectAppliedAsync(current);
        await Checkpoint(sam, step, "default", "Sam signs in on his laptop: System theme, System contrast, Comfortable density");

        await appearance.OpenAsync();
        Assert.That(await appearance.ReadChosenAsync(), Is.EqualTo(current), "the appearance controls show the default pressed");
        await Checkpoint(sam, step, "appearance-controls", "The appearance controls: three themes, three contrasts, two densities");
        await appearance.CloseAsync();

        foreach (var theme in new[] { "paper", "board", "system" })
        {
            current = current with { Theme = theme };
            await shell.ChooseAppearanceAsync(ConsoleAppearance.ThemeCommandOf(theme));
            await appearance.ExpectAppliedAsync(current);
            await Checkpoint(sam, step, "theme-" + theme, $"The {Title(theme)} theme, applied at once: the document is drawn {current.Material()}");
        }

        await shell.Page.EmulateMediaAsync(new() { ColorScheme = ColorScheme.Dark });
        await appearance.ExpectAppliedAsync(current, systemPrefersDark: true);
        await Checkpoint(sam, step, "theme-system-follows-dark", "With the System theme, the operating system turning dark turns the console dark");
        await shell.Page.EmulateMediaAsync(new() { ColorScheme = ColorScheme.Light });
        await appearance.ExpectAppliedAsync(current);
        Coverage.Hit("use-case:shell/appearance-theme");

        foreach (var contrast in new[] { "standard", "more", "system" })
        {
            current = current with { Contrast = contrast };
            await shell.ChooseAppearanceAsync(ConsoleAppearance.ContrastCommandOf(contrast));
            await appearance.ExpectAppliedAsync(current);
            await Checkpoint(sam, step, "contrast-" + contrast, contrast switch
            {
                "standard" => "Standard contrast: the console opts out of the system's high-contrast overlay",
                "more" => "More contrast: the high-contrast overlay, applied at once",
                _ => "System contrast: the console follows the operating system's contrast preference again",
            });
        }

        Coverage.Hit("use-case:shell/appearance-contrast");

        foreach (var density in new[] { "compact", "comfortable" })
        {
            current = current with { Density = density };
            await shell.ChooseAppearanceAsync(ConsoleAppearance.DensityCommandOf(density));
            await appearance.ExpectAppliedAsync(current);
            await Checkpoint(sam, step, "density-" + density, density == "compact"
                ? "Compact density: 28px rows and controls, applied at once"
                : "Comfortable density: 44px rows and controls, the default, again");
        }

        Coverage.Hit("use-case:shell/appearance-density");

        await appearance.ChooseAsync(SamsChoice);
        await appearance.ExpectAppliedAsync(SamsChoice);
        await Expect(shell.Page).ToHaveURLAsync(address);
        await Checkpoint(sam, step, "sams-choice", "Sam settles on Board, More contrast and Compact; the address has not changed");

        await shell.SignOutAsync();
        await Checkpoint(sam, step, "signed-out", "Sam signs out");

        await sam.SignInAsync();
        await appearance.ExpectAppliedAsync(SamsChoice);
        Assert.That(await appearance.ReadChosenAsync(), Is.EqualTo(SamsChoice), "the appearance controls show sam's choice after he signs in again");
        await appearance.CloseAsync();
        await Checkpoint(sam, step, "signed-in-again", "Signed in again, Sam's appearance is back: it is a preference of his, kept outside the address");
        Coverage.Hit("use-case:shell/preferences-outside-address");

        var firstPaint = await appearance.ReloadAndReadFirstPaintAsync();
        Assert.Multiple(() =>
        {
            Assert.That(firstPaint.CircuitApplied, Is.False, $"the first paint is read before the circuit applies an appearance: {firstPaint}");
            Assert.That(firstPaint.Theme, Is.EqualTo(SamsChoice.Material()), $"the first paint is drawn in Board: {firstPaint}");
            Assert.That(firstPaint.Contrast, Is.EqualTo(SamsChoice.Contrast), $"the first paint carries More contrast: {firstPaint}");
            Assert.That(firstPaint.Density, Is.EqualTo(SamsChoice.Density), $"the first paint is compact: {firstPaint}");
        });
        await appearance.ExpectAppliedAsync(SamsChoice);
        await Checkpoint(sam, step, "first-paint", "A reload paints Sam's appearance from the first frame, before the console is interactive");
        Coverage.Hit("use-case:shell/appearance-first-paint");
    }

    /// <summary>U5.2: at 390x844 the compact Directory and Menu replace the spine, the palette fills the screen, and every area stays reachable.</summary>
    private static async Task CompactShellAsync(JourneyEstate estate)
    {
        const string step = "u5-2";
        await using var phone = await estate.SignInAsAsync(Personas.Sam, JourneyEstate.West, new PersonaSessionOptions { Viewport = JourneyViewport.Narrow });
        var shell = phone.Shell;
        Assert.That(await shell.IsCompactAsync(), Is.True, "at 390x844 the console draws its compact chrome");

        await Expect(shell.DirectoryButton).ToBeVisibleAsync();
        await Expect(shell.MenuButton).ToBeVisibleAsync();
        await Expect(shell.Spine).ToBeHiddenAsync();
        await Checkpoint(phone, step, "compact-header", "On his phone the spine gives way to two buttons: Directory and Menu");

        await shell.OpenDirectoryAsync();
        await Expect(shell.DirectorySheet.GetByRole(AriaRole.Navigation, new() { Name = "Estate" })).ToBeVisibleAsync();
        await Checkpoint(phone, step, "directory-sheet", "Directory slides in the spine: every stop sam may open");
        await shell.CloseDirectoryAsync();
        await Expect(shell.DirectoryButton).ToBeFocusedAsync();

        await shell.OpenMenuAsync();
        await Expect(shell.MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Connection" })).ToBeVisibleAsync();
        await Expect(shell.MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Your session" })).ToBeVisibleAsync();
        await Expect(shell.MenuSheet.Locator($"button[data-lt-command=\"{AppearanceCommands.SystemTheme}\"]")).ToBeVisibleAsync();
        await Checkpoint(phone, step, "menu-sheet", "Menu holds the connection, sam's session and the appearance controls");
        await shell.CloseMenuAsync();
        Coverage.Hit("use-case:shell/compact-directory-and-menu");

        var palette = new PalettePage(shell);
        var commands = await palette.CommandsAsync(string.Empty);
        Assert.That(commands, Does.Contain("Go to Home"), "the palette lists the chrome's commands");
        await Expect(shell.PaletteSheet).ToBeVisibleAsync();
        var box = await shell.PaletteSheet.BoundingBoxAsync();
        Assert.That(box, Is.Not.Null, "the palette sheet is laid out");
        Assert.That(box!.Width, Is.GreaterThanOrEqualTo(JourneyViewport.Narrow.Width - 1), "the palette sheet spans the phone's width");
        await Checkpoint(phone, step, "palette-full-screen", "The command palette opens as a full-screen sheet");
        await shell.CloseAddressLineAsync();
        Coverage.Hit("use-case:shell/palette-full-screen");

        var spine = await shell.ReadSpineAsync();
        Assert.That(spine.Open, Does.Contain("home").And.Contain("apps"), $"sam's directory opens Home and Apps: {spine}");
        foreach (var area in spine.Open.Where(area => area != "home"))
        {
            await shell.GoToAreaAsync(area);
            await Checkpoint(phone, step, "area-" + area, $"From the Directory sheet, sam reaches {Title(area)} on his phone");
        }

        await shell.GoToAreaAsync("home");
        await shell.ExpectHeadingAsync(HomeHeading);
        await Checkpoint(phone, step, "home", "And back to Home: every area sam may open is reachable at phone width");
    }

    /// <summary>U5.3: <c>a/</c> lists the apps sam may open, and accepting the task board opens it.</summary>
    private static async Task AppCompletionAsync(PersonaSession sam)
    {
        const string step = "u5-3";
        var shell = sam.Shell;
        var palette = new PalettePage(shell);

        // A fresh load, as when sam opens the console again: the Apps area reads which apps are
        // his once per page, and in isolated mode the viewer binding is only moments old.
        await shell.GotoAsync("/");
        await shell.ExpectHeadingAsync(HomeHeading);
        var apps = await palette.TypeAsync("a/");
        var status = await palette.Status.InnerTextAsync();
        await Checkpoint(sam, step, "app-completions", "Typing a/ lists the apps sam may open: the task board his team works on");
        Assert.That(apps, Does.Contain("a/" + StoryNames.Contoso.Apps.TaskBoard), $"a/ completes the installed task board: [{string.Join(", ", apps)}] ({status})");

        await palette.ChooseAsync("a/" + StoryNames.Contoso.Apps.TaskBoard);
        await Expect(shell.Page).ToHaveURLAsync(new Regex("/apps/" + Regex.Escape(StoryNames.Contoso.Apps.TaskBoard) + "(\\?|$)"));
        await shell.ExpectHeadingAsync("Task board");
        await Checkpoint(sam, step, "task-board-opened", "Accepting a/task-board opens the task board's page");
        Coverage.Hit("use-case:shell/app-completion");
    }

    /// <summary>U5.4: the palette's commands for each area, filtered to the areas sam may use and to what he may do there.</summary>
    private static async Task PaletteCommandsAsync(PersonaSession sam)
    {
        const string step = "u5-4";
        var shell = sam.Shell;
        var palette = new PalettePage(shell);
        await shell.GoToAreaAsync("home");
        var spine = await shell.ReadSpineAsync();
        Assert.Multiple(() =>
        {
            Assert.That(spine.CanOpen("apps"), Is.True, $"sam holds a role in the task board: {spine}");
            Assert.That(spine.CanOpen("access"), Is.False, $"sam administers no access: {spine}");
            Assert.That(spine.CanOpen("cluster"), Is.False, $"sam administers no tree: {spine}");
            Assert.That(spine.CanOpen("tenancy"), Is.False, $"Contoso is not tenanted: {spine}");
        });

        var goTo = await palette.CommandsAsync("go.");
        var expectedGoTo = spine.Open.Select(area => "Go to " + (area == "home" ? "Home" : Display(area))).ToArray();
        Assert.That(goTo, Is.EqualTo(expectedGoTo), "the palette goes to Home and to each area sam may open, and to nothing else");
        await Checkpoint(sam, step, "go-to", "The palette's Go to commands: Home and exactly the areas sam may open");
        await shell.CloseAddressLineAsync();

        foreach (var area in AreaCommands.All)
        {
            var open = spine.CanOpen(area.Area);
            var expected = open ? area.Commands ?? throw new AssertionException($"{area.DisplayName} is open to sam, but U5 pins no commands for it") : [];
            var listed = await palette.CommandsAsync(area.Area + ".");
            Assert.That(listed, Is.EqualTo(expected), $"the palette's {area.DisplayName} commands for sam ({(open ? "open" : "withheld")})");
            await Checkpoint(sam, step, area.Area + "-commands", Describe(area, open, expected));
            await shell.CloseAddressLineAsync();
            if (area.UseCase is { } useCase)
            {
                Coverage.Hit(useCase);
            }
        }

        // Schema opens to sam once a tree he owns the schema of exists (A3, in the story); in
        // isolated mode none does, so the palette's own Data command is run instead.
        if (spine.CanOpen("schema"))
        {
            await shell.RunCommandAsync("Show every tree's schema");
            await Expect(shell.Page).ToHaveURLAsync(new Regex("/schema\\?(.*&)?show=all"));
            await shell.ExpectHeadingAsync("Schema");
            await Checkpoint(sam, step, "schema-all-trees", "Running Show every tree's schema from the palette opens Schema with every tree listed");
        }
        else
        {
            await shell.RunCommandAsync("Refresh the tree directory");
            await Expect(shell.Page).ToHaveURLAsync(new Regex("/data(\\?|$)"));
            await shell.ExpectHeadingAsync("Data");
            await Checkpoint(sam, step, "data-refresh", "Running Refresh the tree directory from the palette opens Data and reloads it");
        }
    }

    /// <summary>U5.5: Reset view at <c>/reset</c> clears the remembered appearance back to its default.</summary>
    private static async Task ResetViewAsync(ChapterContext context, PersonaSession sam)
    {
        const string step = "u5-5";
        var shell = sam.Shell;
        var appearance = new AppearancePage(shell);
        await shell.GotoAsync("/reset");
        await shell.ExpectHeadingAsync("Reset view");
        await appearance.ExpectAppliedAsync(SamsChoice);
        await Expect(shell.Content.GetByRole(AriaRole.Listitem).First).ToBeVisibleAsync();
        await Checkpoint(sam, step, "reset-page", "Reset view lists everything the console remembers, and resets nothing until asked");

        await shell.Content.GetByRole(AriaRole.Button, new() { Name = "Reset view", Exact = true }).ClickAsync();
        await Expect(shell.Content.GetByRole(AriaRole.Status).Filter(new() { HasText = "The Explorer has forgotten your remembered view." })).ToBeVisibleAsync();
        await appearance.ExpectAppliedAsync(ConsoleAppearance.Default);
        await Checkpoint(sam, step, "reset-done", "Reset: the console is back to the System theme, System contrast and Comfortable density");
        Coverage.Hit("use-case:shell/reset-view");

        await shell.GoToAreaAsync("home");
        var chosen = await appearance.ReadChosenAsync();
        Assert.That(chosen, Is.EqualTo(ConsoleAppearance.Default), "the appearance controls show the default after the reset");
        await Checkpoint(sam, step, "defaults-pressed", "The appearance controls agree: System, System, Comfortable");
        await appearance.CloseAsync();

        context.Record(StoryKeys.SamAppearance, chosen.ToString());
    }

    private static async Task StepAsync(string number, Func<Task> step)
    {
        using var scope = Coverage.BeginScope("U5." + number);
        await step();
        scope.Complete();
    }

    private static Task<JourneyManifestEntry> Checkpoint(PersonaSession session, string step, string substep, string title) =>
        session.SubstepAsync(Recorder, step, substep, title);

    private static string Describe(AreaCommands area, bool open, IReadOnlyList<string> expected) => (open, expected.Count) switch
    {
        (false, _) => $"{area.DisplayName} contributes nothing: it is not open to sam",
        (true, 0) => $"{area.DisplayName} is open to sam, but none of its commands is his to run",
        _ => $"{area.DisplayName}'s commands for sam: {string.Join(", ", expected)}",
    };

    private static string Display(string area) =>
        AreaCommands.All.FirstOrDefault(entry => entry.Area == area)?.DisplayName ?? Title(area);

    private static string Title(string word) => word.Length == 0 ? word : char.ToUpperInvariant(word[0]) + word[1..];
}
