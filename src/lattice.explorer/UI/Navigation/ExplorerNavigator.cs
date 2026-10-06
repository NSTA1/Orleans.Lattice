using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// Moves the Explorer between addresses, and keeps every address it shows in the
/// canonical form for the caller's tenancy.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tenancy off:</b> no address carries a tenant root; <c>/t/{tenant}/...</c>
/// is redirected to the same address without it.
/// </para>
/// <para>
/// <b>Tenancy on:</b> the active tenant is the root node of Home and of every
/// tenant-scoped area (<see cref="IExplorerArea.IsTenantScopedAt"/>), and
/// cluster-wide areas never carry one. Arriving at another tenant's address is a
/// request to switch tenant: it goes through the operator-gated switcher, and a
/// refusal redirects back to the active tenant's equivalent address with a
/// notice, so a URL can never scope a caller beyond what they may reach.
/// </para>
/// </remarks>
internal sealed class ExplorerNavigator
{
    private readonly NavigationManager _navigation;
    private readonly ExplorerAreaDirectory _directory;
    private readonly ExplorerTenancy _tenancy;

    /// <summary>Creates the navigator.</summary>
    /// <param name="navigation">The circuit's navigation manager.</param>
    /// <param name="directory">The area directory.</param>
    /// <param name="tenancy">The caller's tenancy.</param>
    public ExplorerNavigator(NavigationManager navigation, ExplorerAreaDirectory directory, ExplorerTenancy tenancy)
    {
        ArgumentNullException.ThrowIfNull(navigation);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(tenancy);

        _navigation = navigation;
        _directory = directory;
        _tenancy = tenancy;
    }

    /// <summary>The address the browser is at, or <see langword="null"/> when its URL is not an Explorer address.</summary>
    public ExplorerAddress? Current =>
        ExplorerAddress.TryFromUri(_navigation.Uri, _navigation.BaseUri, out var address) ? address : null;

    /// <summary>The raw path of the browser's URL relative to the base, for naming an address that did not parse.</summary>
    public string CurrentRelativePath => "/" + _navigation.ToBaseRelativePath(_navigation.Uri);

    /// <summary>
    /// <paramref name="address"/> in the canonical form for the caller's tenancy:
    /// without a tenant root when tenancy is off or the area is cluster-wide, and
    /// rooted at the active tenant when it is on and none is given.
    /// </summary>
    /// <param name="address">The address.</param>
    public ExplorerAddress Canonicalize(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        if (!_tenancy.IsActive || !IsTenantScoped(address))
        {
            return address.WithTenant(null);
        }

        return address.Tenant is null && _tenancy.ActiveTenant is { } active
            ? address.WithTenant(active)
            : address;
    }

    /// <summary>
    /// Decides where the browser should be for <paramref name="address"/>: the
    /// address itself when it is canonical, or the canonical address to redirect
    /// to, with a notice when a tenant switch was refused.
    /// </summary>
    /// <param name="address">The address the browser arrived at.</param>
    /// <param name="cancellationToken">Cancels the tenant switch.</param>
    public async Task<ExplorerAddressResolution> ResolveAsync(ExplorerAddress address, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(address);

        string? notice = null;
        var target = address;

        if (_tenancy.IsActive
            && IsTenantScoped(address)
            && address.Tenant is { } requested
            && !string.Equals(requested, _tenancy.ActiveTenant, StringComparison.Ordinal))
        {
            if (await _tenancy.TrySwitchAsync(requested, cancellationToken).ConfigureAwait(false))
            {
                // The operator verdict that decides whether the reserved default
                // tenant is shown was read for the tenant just left, so it is read
                // again before canonicalising: an operator who switches to the
                // default tenant lands on its address rather than being bounced off
                // it by a verdict that belonged to another tenant.
                await _tenancy.RefreshAsync(cancellationToken).ConfigureAwait(false);
                notice = SwitchedNotice(requested);
            }
            else
            {
                target = address.WithTenant(_tenancy.ActiveTenant);
                notice = RefusedNotice(requested, _tenancy.ActiveTenant);
            }
        }

        var canonical = Canonicalize(target);
        return new ExplorerAddressResolution(canonical, canonical.Equals(address) ? null : canonical, notice);
    }

    /// <summary>The notice a switch to <paramref name="tenant"/> that took effect is announced with.</summary>
    /// <param name="tenant">The tenant now active.</param>
    public static string SwitchedNotice(string tenant) => $"Scoped to tenant {tenant}.";

    /// <summary>
    /// The notice a refused switch to <paramref name="requested"/> is announced
    /// with, naming the tenant the caller stays in when there is one.
    /// </summary>
    /// <param name="requested">The tenant asked for.</param>
    /// <param name="active">The tenant still active, or <see langword="null"/>.</param>
    public static string RefusedNotice(string requested, string? active) => active is not null
        ? $"You can't scope to tenant {requested}, so this shows tenant {active} instead."
        : $"You can't scope to tenant {requested}.";

    /// <summary>Navigates to the canonical form of <paramref name="address"/>.</summary>
    /// <param name="address">Where to go.</param>
    /// <param name="replace">Whether to replace the current history entry rather than add one.</param>
    public void NavigateTo(ExplorerAddress address, bool replace = false)
    {
        ArgumentNullException.ThrowIfNull(address);
        _navigation.NavigateTo(Canonicalize(address).ToHref(), new NavigationOptions { ReplaceHistoryEntry = replace });
    }

    /// <summary>
    /// <paramref name="address"/> re-rooted at <paramref name="tenant"/>: the
    /// tenant replaces the root of Home and of a tenant-scoped area, and a
    /// cluster-wide area's address is unchanged.
    /// </summary>
    /// <param name="address">The address.</param>
    /// <param name="tenant">The tenant id.</param>
    public ExplorerAddress ReRoot(ExplorerAddress address, string tenant)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentException.ThrowIfNullOrEmpty(tenant);
        return IsTenantScoped(address) ? address.WithTenant(tenant) : address;
    }

    /// <summary>
    /// The nearest ancestor of <paramref name="address"/> that exists for this
    /// caller: the root of its area when that area is shown and the address is
    /// below it, and otherwise Home.
    /// </summary>
    /// <param name="address">An address that did not resolve, or <see langword="null"/> when the URL was not an address.</param>
    /// <param name="cancellationToken">Cancels the availability check.</param>
    public async Task<ExplorerAddress> GetNearestValidAncestorAsync(ExplorerAddress? address, CancellationToken cancellationToken = default)
    {
        var home = Canonicalize(ExplorerAddress.Home.WithTenant(address?.Tenant));

        if (address?.Area is not { } key || _directory.Find(key) is not { } area)
        {
            return home;
        }

        var root = ExplorerAddress.Create(address.Tenant, key);
        if (StripQuery(address).Equals(root))
        {
            return home;
        }

        var availability = await _directory.GetAvailabilityAsync(area, cancellationToken).ConfigureAwait(false);
        return availability.Kind == AreaAvailabilityKind.Visible ? Canonicalize(root) : home;
    }

    private static ExplorerAddress StripQuery(ExplorerAddress address) =>
        ExplorerAddress.Create(address.Tenant, address.Area, address.Path);

    /// <summary>
    /// Whether <paramref name="address"/> follows the active tenant: Home and every
    /// address in a tenant-scoped area do, a cluster-wide area's do not.
    /// </summary>
    /// <param name="address">The address.</param>
    public bool IsTenantScoped(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Area is not { } key || _directory.Find(key) is not { } area || area.IsTenantScopedAt(address);
    }

    /// <summary>
    /// Whether <paramref name="address"/> renders standalone, without the shell's
    /// header, address line and directory spine: its area's answer, and
    /// <see langword="false"/> for an address in no known area.
    /// </summary>
    /// <param name="address">The address.</param>
    public bool IsStandalone(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Area is { } key && _directory.Find(key) is { } area && area.IsStandaloneAt(address);
    }
}
