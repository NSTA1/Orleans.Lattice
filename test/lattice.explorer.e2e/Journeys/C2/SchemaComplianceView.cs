using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// The Schema area's compliance scan as C2 drives it: <b>Scan compliance...</b> on the
/// directory, the tree picked in its dialog, and the tree's Compliance tab following the
/// scan to its end.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class SchemaComplianceView(PersonaSession session)
{
    /// <summary>What the Compliance tab says while a scan runs, before the tree id.</summary>
    public const string ScanningPrefix = "Scanning every value of ";

    /// <summary>What the Compliance tab says when a scan ended on the cluster without a report.</summary>
    public const string FailedPrefix = "The scan failed.";

    private IPage Page => session.Page;

    private ILocator Content => session.Shell.Content;

    /// <summary>The running scan's status line.</summary>
    public ILocator Scanning => Content.GetByRole(AriaRole.Status).Filter(new() { HasTextString = ScanningPrefix });

    /// <summary>The finished scan's report.</summary>
    public ILocator LastScan => Content.GetByRole(AriaRole.Heading, new() { Name = "Last scan", Exact = true });

    /// <summary>The tab's error line: a failed scan, or a status read that did not answer.</summary>
    public ILocator Error => Content.Locator(".lt-schema-error[role=alert]");

    /// <summary>Starts a scan of <paramref name="tree"/> from the Schema directory's <b>Scan compliance...</b> dialog.</summary>
    /// <param name="tree">The tree id; it must have a policy.</param>
    public async Task StartScanAsync(string tree)
    {
        await session.Shell.GoToAreaAsync("schema");
        await Content.GetByRole(AriaRole.Button, new() { Name = "Scan compliance...", Exact = true }).ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "Scan compliance" });
        await Expect(dialog).ToBeVisibleAsync();
        await dialog.GetByLabel("Tree").SelectOptionAsync(new SelectOptionValue { Label = tree });
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Scan", Exact = true }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();
        await Expect(Page).ToHaveURLAsync(new Regex($"/schema/{Regex.Escape(tree)}\\?(.*&)?tab=compliance"));
    }

    /// <summary>
    /// Waits until the tab shows the scan running. The tree's page can take several seconds to
    /// render while the cluster settles after C2.1's silo restart, so the bound is a minute,
    /// well inside the scan's hold.
    /// </summary>
    public Task ExpectScanningAsync() => Expect(Scanning).ToBeVisibleAsync(new() { Timeout = 60_000 });

    /// <summary>
    /// Waits until the scan the tab follows has ended - a report, or the reason it failed -
    /// and returns which. A tab still saying it is scanning when <paramref name="timeout"/>
    /// runs out is the silent stall C2.2 exists to rule out, and fails the step.
    /// </summary>
    /// <param name="timeout">How long a scan may take to end honestly.</param>
    /// <returns><see langword="true"/> for a report; <see langword="false"/> for a failure the tab named.</returns>
    public async Task<bool> WaitForOutcomeAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (await LastScan.IsVisibleAsync() && !await Scanning.IsVisibleAsync())
            {
                return true;
            }

            if (!await Scanning.IsVisibleAsync() && await Error.IsVisibleAsync() && (await Error.InnerTextAsync()).Contains(FailedPrefix, StringComparison.Ordinal))
            {
                return false;
            }

            if (DateTime.UtcNow > deadline)
            {
                var error = await Error.IsVisibleAsync() ? await Error.InnerTextAsync() : "none";
                throw new System.TimeoutException(
                    $"After {timeout.TotalSeconds:0} s the Compliance tab still shows no outcome (scanning: {await Scanning.IsVisibleAsync()}, error: {error}): the scan stalled silently.");
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }
}
