using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>Steps 4 to 6: the spine, the address line and the palette; the keyboard alone; and the not-found page.</summary>
internal sealed partial class FirstContactRun
{
    /// <summary>How many Tab presses the keyboard-only round trip allows to reach a spine stop.</summary>
    private const int MaximumTabs = 80;

    /// <summary>An address nothing lives at.</summary>
    private const string NowhereAddress = "/nowhere-in-particular";

    /// <summary>
    /// A1.4: Olivia moves around three ways - the spine to Data; the address line, accepting the
    /// area completion for <c>back</c> and then the <b>Go to</b> entry for
    /// <c>/backups/schedules</c>; and the command palette, running <c>access.explain</c> - and
    /// the browser's back and forward restore each address.
    /// </summary>
    public Task NavigateAsync() => StepAsync("4", async () =>
    {
        await Shell.GoToAreaAsync("data");
        await Shell.ExpectHeadingAsync("Data");
        await ExpectAddressAsync("/data");
        await CheckpointAsync("4", "spine-data", "From the spine, Olivia opens Data");

        await Shell.CompleteAsync("back");
        var area = Shell.Suggestions.Filter(new() { HasText = "Backups" });
        await Expect(area).ToHaveCountAsync(1);
        await CheckpointAsync("4", "area-completion", "The address line completes 'back' to the Backups area");
        await area.ClickAsync();
        await Expect(Shell.AddressInput).ToBeHiddenAsync();
        await ExpectAddressAsync("/backups");
        await Expect(Shell.Heading).ToBeVisibleAsync();
        Coverage.Hit("use-case:shell/address-completions");
        await CheckpointAsync("4", "backups", "Accepting the completion opens Backups");

        await Shell.CompleteAsync("/backups/schedules");
        var goTo = Shell.Suggestions.Filter(new() { HasText = "Go to" }).Filter(new() { HasText = "/backups/schedules" });
        await Expect(goTo).ToHaveCountAsync(1);
        await CheckpointAsync("4", "address-line", "Typing the address /backups/schedules offers to go there");
        await goTo.ClickAsync();
        await Expect(Shell.AddressInput).ToBeHiddenAsync();
        await ExpectAddressAsync("/backups/schedules");
        await Expect(Shell.Heading).ToBeVisibleAsync();
        Coverage.Hit("use-case:shell/address-line");
        await CheckpointAsync("4", "schedules", "The address line opens the backup schedules page");

        var commands = await Shell.OpenCommandPaletteAsync();
        Assert.That(commands, Has.Some.Contains("Explain access"), "the palette offers Access's commands to its administrator");
        await CheckpointAsync("4", "palette", "Typing > turns the address line into the command palette");
        await Shell.RunCommandAsync("Explain access...");
        await ExpectAddressAsync("/access/explain");
        await Shell.ExpectHeadingAsync("Access");
        Coverage.Hit("use-case:shell/palette");
        Coverage.Hit("command:access.explain");
        await CheckpointAsync("4", "explain", "Running access.explain from the palette opens Explain access");

        await HistoryAsync(back: true, "/backups/schedules");
        await HistoryAsync(back: true, "/backups");
        await HistoryAsync(back: true, "/data");
        await CheckpointAsync("4", "back", "Back, back and back: the browser returns through Backups to Data");
        await HistoryAsync(back: false, "/backups");
        await HistoryAsync(back: false, "/backups/schedules");
        await HistoryAsync(back: false, "/access/explain");
        await CheckpointAsync("4", "forward", "Forward three times: the browser restores each address, ending at Explain access");
    });

    /// <summary>
    /// A1.5: the same kind of round trip with the keyboard alone: Tab to the spine and Enter on
    /// Schema; <c>/</c> opens the address line, <c>&gt;Go to Home</c> and Enter run the palette
    /// command that goes Home.
    /// </summary>
    public Task NavigateByKeyboardAsync() => StepAsync("5", async () =>
    {
        var reached = false;
        for (var press = 0; press < MaximumTabs && !reached; press++)
        {
            await Page.Keyboard.PressAsync("Tab");
            reached = await Page.EvaluateAsync<bool>(
                "() => document.activeElement?.getAttribute('data-lt-command') === 'go.schema' && !!document.activeElement.closest('nav[aria-label=\"Estate\"]')");
        }

        Assert.That(reached, Is.True, $"Tab reaches the spine's Schema stop within {MaximumTabs} presses");
        await Expect(Shell.Stop("schema")).ToBeFocusedAsync();
        await CheckpointAsync("5", "spine-focus", "Keyboard only: Tab brings focus to the spine's Schema stop");

        await Page.Keyboard.PressAsync("Enter");
        await ExpectAddressAsync("/schema");
        await Expect(Shell.Heading).ToBeVisibleAsync();
        await CheckpointAsync("5", "schema", "Enter opens Schema");

        await Page.Keyboard.PressAsync("/");
        await Expect(Shell.AddressInput).ToBeFocusedAsync();
        await Page.Keyboard.TypeAsync(">Go to Home");
        await Expect(Shell.AddressInput).ToHaveValueAsync(">Go to Home");
        await Expect(Shell.Suggestions.Filter(new() { HasText = "Go to Home" })).ToHaveCountAsync(1);
        await CheckpointAsync("5", "palette", "/ opens the address line; >Go to Home finds the command");

        await Page.Keyboard.PressAsync("Enter");
        await Expect(Shell.AddressInput).ToBeHiddenAsync();
        await ExpectHomeAsync();
        Coverage.Hit("command:go.home");
        Coverage.Hit("use-case:shell/keyboard-only");
        await CheckpointAsync("5", "home", "Enter runs it: Olivia is Home without touching the mouse");
    });

    /// <summary>A1.6: an address nothing lives at says so, and names the way home.</summary>
    public Task MeetNotFoundAsync() => StepAsync("6", async () =>
    {
        // Typed into the address line, as a person would: a client-side navigation.
        await Shell.CompleteAsync(NowhereAddress);
        var goTo = Shell.Suggestions.Filter(new() { HasText = "Go to" }).Filter(new() { HasText = NowhereAddress });
        await Expect(goTo).ToHaveCountAsync(1);
        await goTo.ClickAsync();
        await Expect(Shell.AddressInput).ToBeHiddenAsync();

        await Shell.ExpectHeadingAsync(ShellPage.NotFoundHeading);
        await Expect(Shell.Content).ToContainTextAsync(NowhereAddress.TrimStart('/'));
        var home = Shell.Content.Locator(".lt-shell-not-found__ancestor a");
        await Expect(home).ToBeVisibleAsync();
        Coverage.Hit("use-case:shell/not-found");
        Coverage.Hit("route:/not-found");
        Coverage.Hit("subroute:shell/not-found");
        await CheckpointAsync("6", "not-found", "An address nothing lives at: the Explorer says so, and names the nearest address that does");

        await home.ClickAsync();
        await ExpectHomeAsync();
        await CheckpointAsync("6", "way-home", "Following that link brings Olivia Home");
    });

    private async Task HistoryAsync(bool back, string path)
    {
        var options = new PageGoBackOptions { WaitUntil = WaitUntilState.Commit };
        if (back)
        {
            await Page.GoBackAsync(options);
        }
        else
        {
            await Page.GoForwardAsync(new PageGoForwardOptions { WaitUntil = WaitUntilState.Commit });
        }

        await ExpectAddressAsync(path);
        await Expect(Shell.Heading).ToBeVisibleAsync();
    }
}
