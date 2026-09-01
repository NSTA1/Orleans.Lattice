using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Pages;

/// <summary>
/// The shell's routable surface, and the one place the URL and the preference
/// contract are arbitrated against each other.
/// </summary>
/// <remarks>
/// <para>
/// A bare <c>/</c> carries no state, so the remembered view is restored into the
/// address bar with a history <em>replace</em>, leaving Back pointing wherever
/// the user came from rather than at the shell's own bookkeeping. Any other
/// address is explicit and wins outright; it is merely remembered, so the next
/// bare visit lands there. The rule itself lives in
/// <see cref="ExplorerShellEntryPolicy"/> - a pure function, tested without a
/// renderer - and this page is its single application point.
/// </para>
/// <para>
/// The restore runs once per session entry. A later, deliberate navigation to
/// <c>/</c> is taken at face value: a user who asks for the plain home surface
/// must be able to reach it, rather than being bounced back to where they were
/// yesterday every time.
/// </para>
/// <para>
/// <b>Keeping the address bar and the route in step is not done here.</b> That
/// binding belongs to <see cref="Layout.ShellRouteBinding"/> on the layout,
/// because this page is unmounted whenever a contributed area takes the working
/// surface - see that type for what went wrong while the binding lived here.
/// </para>
/// </remarks>
public partial class Home
{
    /// <summary>
    /// The area route value. Declared because the templates name it, but never
    /// read: the address is parsed from <see cref="NavigationManager.Uri"/> by
    /// <see cref="ExplorerRoutePath.Parse"/> so an escaped selection id survives,
    /// which Blazor's own segment binding would split.
    /// </summary>
    [Parameter]
    public string? Area { get; set; }

    /// <summary>The selection-kind route value. Declared, not read - see <see cref="Area"/>.</summary>
    [Parameter]
    public string? Kind { get; set; }

    /// <summary>The selection-id route value. Declared, not read - see <see cref="Area"/>.</summary>
    [Parameter]
    public string? Id { get; set; }

    /// <summary>The detail-surface route value. Declared, not read - see <see cref="Area"/>.</summary>
    [Parameter]
    public string? Surface { get; set; }

    [Inject]
    private IExplorerShellRouter Router { get; set; } = default!;

    [Inject]
    private IExplorerShellPreferences Preferences { get; set; } = default!;

    [Inject]
    private IExplorerShellEntryGate EntryGate { get; set; } = default!;

    /// <inheritdoc />
    protected override Task OnInitializedAsync() => HydrateAndSettleAsync();

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (!firstRender)
        {
            return;
        }

        // Browser storage is unreachable during a server prerender, so the first
        // hydration attempt can legitimately have done nothing.
        await HydrateAndSettleAsync();
    }

    private async Task HydrateAndSettleAsync()
    {
        await Preferences.EnsureLoadedAsync();

        if (!Preferences.IsLoaded)
        {
            // Still unhydrated: leave the shell on whatever the address says and
            // try again after the first render. Restoring from an unhydrated
            // mirror would look exactly like "nothing was remembered".
            return;
        }

        // Claimed from the session rather than from a field on this page. The
        // router destroys and recreates this page on every navigation away and
        // back, so a field here would mean "once per page instance" - which reads
        // as "every time you return to '/'", and made Back out of an area bounce
        // straight into it again.
        if (!EntryGate.TryClaimEntry())
        {
            return;
        }

        var entry = ExplorerShellEntryPolicy.Decide(
            Router.Status,
            Router.Current,
            Preferences.GetRememberedRoute());

        switch (entry.Action)
        {
            case ExplorerShellEntryAction.RestoreRemembered:
                // Replace rather than push: the user asked for '/', so Back
                // should return them to wherever they came from, not to the
                // address the shell substituted for them.
                Router.NavigateTo(entry.Route, replace: true);
                return;

            case ExplorerShellEntryAction.Canonicalize:
                Router.Canonicalize();
                break;
        }

        await Preferences.RememberRouteAsync(Router.Current);
    }
}
