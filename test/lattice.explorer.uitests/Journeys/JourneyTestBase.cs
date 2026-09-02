using System.Net.Http;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Base class for every end-to-end journey fixture. It points
/// <see cref="UiTestBase"/> at the journey web head and adds the operations a journey
/// needs beyond a single-surface sweep: arriving at an address, signing in as a named
/// identity, and moving the demo cluster's facts between visits.
/// <para>
/// <b>The discipline this class enforces.</b> Every navigation helper here asserts the
/// shell genuinely rendered before it returns. That is not defensive habit: a journey
/// is a chain of steps, and a step that quietly did nothing turns every later
/// assertion into a statement about a blank document - which is precisely the shape in
/// which an accessibility or absence assertion passes hardest. A journey that cannot
/// reach its precondition must fail loudly, never skip and never sweep clean.
/// </para>
/// </summary>
public abstract class JourneyTestBase : UiTestBase
{
    /// <summary>A viewport comfortably inside the expanded band, where every area is inline.</summary>
    private protected const int ExpandedWidth = 1400;

    /// <summary>The narrowest viewport the product supports.</summary>
    private protected const int NarrowWidth = 320;

    /// <summary>The viewport height every journey uses; only width selects a band.</summary>
    private protected const int Height = 900;

    /// <inheritdoc />
    protected override Uri BaseUri => JourneyAppHostSetup.Host.BaseUri;

    /// <inheritdoc />
    protected override ExplorerAppHost FaultSource => JourneyAppHostSetup.Host;

    /// <summary>The demo cluster's facts, which a journey may move between visits.</summary>
    private protected static JourneyWorld World => JourneyAppHostSetup.World;

    /// <summary>
    /// Restores the starting world before each test, so no journey inherits another's
    /// mutation and the fixtures carry no ordering dependence.
    /// </summary>
    [SetUp]
    public void ResetWorld() => World.Reset();

    /// <summary>
    /// Navigates <paramref name="page"/> to <paramref name="relativePath"/> on the
    /// journey head, waits for the interactive circuit, and asserts the shell rendered.
    /// </summary>
    /// <param name="page">The page to navigate.</param>
    /// <param name="relativePath">The address, relative to the head's base address.</param>
    private protected async Task GoToAsync(IPage page, string relativePath)
    {
        await page.GotoAsync(new Uri(BaseUri, relativePath).ToString());
        await ExplorerShell.WaitForShellReadyAsync(page);
        await ExplorerShell.AssertShellRenderedAsync(page);
    }

    /// <summary>
    /// Opens a fresh page at <paramref name="relativePath"/> in its own browser
    /// context, so it carries no cookie or storage from any earlier visit.
    /// </summary>
    /// <param name="relativePath">The address, relative to the head's base address.</param>
    /// <param name="width">The viewport width.</param>
    private protected async Task<IPage> OpenAtAsync(string relativePath, int width = ExpandedWidth)
    {
        var page = await NewPageAsync(width, Height);
        await GoToAsync(page, relativePath);
        return page;
    }

    /// <summary>
    /// Reloads <paramref name="page"/> and waits for the fresh circuit, so an assertion
    /// after it measures a genuinely re-entered shell rather than the pre-reload DOM.
    /// </summary>
    /// <param name="page">The page to reload.</param>
    private protected static async Task ReloadAsync(IPage page)
    {
        await page.ReloadAsync();
        await ExplorerShell.WaitForShellReadyAsync(page);
        await ExplorerShell.AssertShellRenderedAsync(page);
    }

    /// <summary>
    /// Opens a second page in <paramref name="page"/>'s own browser context: a new
    /// circuit and a new server-side scope, sharing the browser's cookies and durable
    /// preference storage. That is what "the user comes back in a new session" means
    /// here - a fresh browser profile would have no remembered anything to restore, so
    /// it would prove the opposite of what it appears to.
    /// </summary>
    /// <param name="page">The page whose context to reuse.</param>
    private protected async Task<IPage> NewSessionAsync(IPage page)
    {
        var next = await page.Context.NewPageAsync();
        await next.GotoAsync(BaseUri.ToString());
        await ExplorerShell.WaitForShellReadyAsync(next);
        await ExplorerShell.AssertShellRenderedAsync(next);
        return next;
    }

    /// <summary>
    /// Withdraws the second demo tenant from every identity's reachable set, out of
    /// band, so the next visit finds a remembered choice that no longer resolves.
    /// </summary>
    private protected async Task WithdrawSecondTenantAsync()
    {
        using var client = new HttpClient { BaseAddress = BaseUri };
        using var response = await client.PostAsync(JourneyAppHostSetup.WithdrawTenantPath, content: null);
        response.EnsureSuccessStatusCode();

        Assert.That(World.IsGlobexWithdrawn, Is.True,
            "The journey world reported the second tenant is still reachable after the withdrawal "
            + "endpoint returned success, so the fail-closed restore this journey is about cannot "
            + "have been set up.");
    }
}
