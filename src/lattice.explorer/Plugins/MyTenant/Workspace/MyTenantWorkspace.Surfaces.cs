using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// Where the open sub-surface lives: in the address, so a link to the sharing
/// surface reopens on the sharing surface, and in the shell's preference
/// contract, so the caller returns to the surface they left.
/// </summary>
/// <remarks>
/// <para>
/// The address wins. A link somebody sent must show what they saw, so an address
/// naming a surface is an instruction and the remembered value is only consulted
/// when it says nothing - which is the same division of labour the shell itself
/// applies between <c>IExplorerShellRouter</c> and
/// <c>IExplorerShellPreferences</c>.
/// </para>
/// <para>
/// Both seams are optional. A head composed without a router keeps the surface
/// out of the URL; one composed without preferences keeps it for the life of the
/// circuit. Neither absence changes what the area can do.
/// </para>
/// </remarks>
public sealed partial class MyTenantWorkspace
{
    // Cached rather than composed per restore: the predicate runs on the
    // start-up path and a closure there would allocate on every mount.
    private static readonly Func<string, IReadOnlyList<LatticeTabItem>, bool> SurfaceIsOffered =
        IsOffered;

    // Set while this workspace is the cause of a navigation, so the router's own
    // notification is not read back as somebody else moving us.
    private bool _navigating;

    /// <summary>
    /// Why a remembered sub-surface was not restored, when one was abandoned, or
    /// <see langword="null"/>. Announced rather than swallowed, so a surface that
    /// has since been renamed or retired explains itself instead of silently
    /// reopening on the default.
    /// </summary>
    public string? SurfaceRestoreNotice { get; private set; }

    /// <summary>
    /// Opens the sub-surface the address names, or - failing that - the one the
    /// caller last had open. Leaves the default surface open when neither says
    /// anything usable.
    /// </summary>
    private async Task RestoreSurfaceAsync()
    {
        SurfaceRestoreNotice = null;

        if (AddressedSurface() is { } addressed)
        {
            await ActivateSurfaceAsync(addressed).ConfigureAwait(false);

            // Remembered but not re-addressed: the address already says this, and
            // remembering it keeps a later bare visit on the surface the link
            // opened.
            await PersistSurfaceAsync(addressed).ConfigureAwait(false);
            return;
        }

        if (_preferences is null)
        {
            return;
        }

        ExplorerPreferenceResolution<string> resolution;
        try
        {
            await _preferences.EnsureLoadedAsync().ConfigureAwait(false);
            resolution = await _preferences.RestoreAsync(
                MyTenantPluginKeys.SurfacePreference,
                MyTenantSurfaces.Overview,
                MyTenantSurfaces.Tabs,
                SurfaceIsOffered).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The durable store is a convenience layered over the default
            // surface. A prerender pass cannot reach browser storage at all, and
            // that is not a reason to fail the area's first load.
            return;
        }

        SurfaceRestoreNotice = resolution.WasAbandoned ? resolution.Explanation : null;

        if (await ActivateSurfaceAsync(resolution.Value).ConfigureAwait(false))
        {
            // Replace rather than push: the caller asked for the area, so Back
            // should leave the area rather than step through a surface they never
            // chose.
            AddressSurface(resolution.Value, replaceHistoryEntry: true);
        }
    }

    /// <summary>
    /// Records <paramref name="surfaceId"/> as the open surface in both the
    /// address and the preference contract.
    /// </summary>
    private async Task RememberSurfaceAsync(string surfaceId, bool replaceHistoryEntry)
    {
        AddressSurface(surfaceId, replaceHistoryEntry);
        await PersistSurfaceAsync(surfaceId).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes the open surface into the address, when a router is present and the
    /// address is still on this area.
    /// </summary>
    private void AddressSurface(string surfaceId, bool replaceHistoryEntry)
    {
        if (_router is null)
        {
            return;
        }

        var route = _router.Current;
        if (!IsThisArea(route))
        {
            // The caller has already left; writing our surface into somebody
            // else's address would be a navigation they did not ask for.
            return;
        }

        var next = route.WithParameter(MyTenantPluginKeys.SurfaceQueryKey, surfaceId);
        if (route.Equals(next))
        {
            return;
        }

        _navigating = true;
        try
        {
            _router.NavigateTo(next, replaceHistoryEntry);
        }
        finally
        {
            _navigating = false;
        }
    }

    /// <summary>
    /// Remembers the open surface durably. Never fails the activation that
    /// preceded it: the surface is already open, so a durable write that did not
    /// land is a lost convenience rather than a lost action.
    /// </summary>
    private async Task PersistSurfaceAsync(string surfaceId)
    {
        if (_preferences is null)
        {
            return;
        }

        try
        {
            await _preferences.EnsureLoadedAsync().ConfigureAwait(false);
            await _preferences.SetAsync(MyTenantPluginKeys.SurfacePreference, surfaceId)
                .ConfigureAwait(false);
        }
        catch (Exception)
        {
            // As above: a prerender pass cannot reach browser storage, and the
            // surface is open either way.
        }
    }

    /// <summary>
    /// The surface the current address names, or <see langword="null"/> when it
    /// names none, names one this plugin does not offer, or is not on this area
    /// at all.
    /// </summary>
    private string? AddressedSurface()
    {
        if (_router is null)
        {
            return null;
        }

        var route = _router.Current;
        if (!IsThisArea(route))
        {
            return null;
        }

        var surfaceId = route.Parameters.GetValueOrEmpty(MyTenantPluginKeys.SurfaceQueryKey);
        return MyTenantSurfaces.IsKnown(surfaceId) ? surfaceId : null;
    }

    /// <summary>
    /// Follows the address when the caller moves through history, so Back and
    /// Forward walk the surfaces they opened rather than leaving the area showing
    /// one thing and saying another.
    /// </summary>
    private void OnRouteChanged(ExplorerRoute route)
    {
        if (_navigating || !IsThisArea(route))
        {
            return;
        }

        var surfaceId = route.Parameters.GetValueOrEmpty(MyTenantPluginKeys.SurfaceQueryKey);
        if (!MyTenantSurfaces.IsKnown(surfaceId)
            || string.Equals(surfaceId, ActiveSurfaceId, StringComparison.Ordinal))
        {
            return;
        }

        // Fire and forget, exactly as a gate change that opens the surface does:
        // the workspace raises Changed when the load settles.
        _ = ActivateSurfaceAsync(surfaceId);
    }

    private static bool IsThisArea(ExplorerRoute route) =>
        string.Equals(route.Area, MyTenantPluginKeys.AreaSlug, StringComparison.Ordinal);

    private static bool IsOffered(string surfaceId, IReadOnlyList<LatticeTabItem> tabs)
    {
        // Indexed rather than a LINQ probe: this runs on the start-up path, and
        // the list is six entries long.
        for (var i = 0; i < tabs.Count; i++)
        {
            if (string.Equals(tabs[i].Id, surfaceId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
