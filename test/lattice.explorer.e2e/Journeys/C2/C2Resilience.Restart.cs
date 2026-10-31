using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>C2.3: the west Explorer restarts under priya's tab, with no persisted Data Protection keys.</summary>
public sealed partial class C2Resilience
{
    private static readonly TimeSpan ExplorerStart = TimeSpan.FromMinutes(2);

    /// <summary>
    /// C2.3: the harness kills the west Explorer (which persists no Data Protection keys) under
    /// priya's open tab, and starts it again. Her tab says it lost the server; once the head is
    /// back the page reloads itself at the same address, where her old session cookie no longer
    /// decrypts: the console reads <b>Sign-in required</b> and offers <b>Sign in</b>. She signs in
    /// through the sign-in dialog. That she lands on Home rather than back on the address she was
    /// on is #5481.
    /// </summary>
    private static async Task ExplorerRestartAsync(JourneyEstate estate, PersonaSession priya, C2Step step)
    {
        var page = priya.Page;
        var address = new Uri(page.Url);

        // The browser says, truthfully, that the server went away: the circuit's socket closes and
        // the reconnect attempts fail while the head is down. Nothing else is tolerated.
        priya.TolerateConsole(CircuitLost());
        await page.EvaluateAsync("() => { window.__c2BeforeRestart = true; }");

        var explorer = estate.WestRegion.ExplorerProcesses[0];
        try
        {
            await explorer.KillAsync();
            step.Log("killed the west Explorer head");
            await Expect(page.GetByText(Rejoining()).First).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await step.CaptureAsync(priya, "server-lost", "The west Explorer is down: priya's tab says it lost the server and is trying to rejoin");
        }
        finally
        {
            if (!explorer.IsRunning)
            {
                await explorer.StartAsync(ExplorerStart);
                step.Log("started the west Explorer head again, with a fresh Data Protection key ring");
            }
        }

        await WaitForReloadAsync(page, TimeSpan.FromMinutes(2));
        Assert.That(new Uri(page.Url).PathAndQuery, Is.EqualTo(address.PathAndQuery), "the tab reloaded itself at the address it was on");
        await priya.Shell.ExpectSignedOutAsync();
        await priya.Shell.ExpectConnectionAsync("Sign-in required");
        await step.CaptureAsync(priya, "signed-out", "The page reloaded at the same address; the old session no longer decrypts, so the console reads Sign-in required");
        Coverage.Hit("use-case:shell/unresponsive-console");

        var dialog = await priya.Shell.OpenSignInAsync();
        await dialog.GetByLabel("Username").FillAsync(priya.Persona.Id);
        await step.CaptureAsync(priya, "sign-in", "priya signs in again through the sign-in dialog");
        await dialog.GetByLabel("Password").FillAsync(priya.Persona.Password);
        await priya.Shell.SubmitAndWaitForNextDocumentAsync(dialog.Locator("form[method=post] button[type=submit]"));
        await priya.Shell.ExpectSignedInAsync(priya.Persona.Id);
        await priya.Shell.ExpectConnectionAsync("Connected");
        Coverage.Hit("dialog:shell/sign-in");

        await new KnownDefect("#5481", "Signing in returns to Home instead of the address the user was on").ReproducesAsync(async () =>
        {
            await Expect(page).Not.ToHaveURLAsync(new Regex(Regex.Escape(address.PathAndQuery) + "$"));
            Assert.That(new Uri(page.Url).AbsolutePath, Is.EqualTo(new Uri(estate.WestRegion.ExplorerUri, "/").AbsolutePath), "the sign-in redirected to the console's home");
        });
        await step.CaptureAsync(priya, "signed-in", "priya is signed in again, but on Home rather than the Keys address she was reading (#5481)");
        Coverage.Hit("use-case:shell/sign-in-after-restart");
    }

    /// <summary>Waits until the document the restart found has been replaced by a new one, and that one is live.</summary>
    private static async Task WaitForReloadAsync(IPage page, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            try
            {
                if (!await page.EvaluateAsync<bool>("() => window.__c2BeforeRestart === true"))
                {
                    break;
                }
            }
            catch (PlaywrightException)
            {
                // The document is being replaced.
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new System.TimeoutException(
                    $"{timeout.TotalSeconds:0} s after the west Explorer came back, the tab had not reloaded itself: it still shows the old document at {page.Url}.");
            }

            await Task.Delay(500);
        }

        await JourneyBrowser.WaitForInteractiveAsync(page);
    }

    [GeneratedRegex("WebSocket|_blazor|Connection disconnected|ERR_CONNECTION_REFUSED|Failed to load resource|reconnect|Rejoin|circuit", RegexOptions.IgnoreCase)]
    private static partial Regex CircuitLost();

    [GeneratedRegex("rejoin|reconnect", RegexOptions.IgnoreCase)]
    private static partial Regex Rejoining();
}
