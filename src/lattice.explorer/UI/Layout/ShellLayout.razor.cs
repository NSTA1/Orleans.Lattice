using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Slots;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Session;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The Explorer's layout: the frame every page renders inside - skip links, the
/// header, the address line, the directory spine and the main landmark - and the
/// place where the address is made canonical and the width band is measured.
/// </summary>
/// <remarks>
/// <para>
/// On every navigation it resolves the browser's address for the caller's
/// tenancy (redirecting a non-canonical one and announcing a refused tenant
/// switch), asks the directory which areas are shown, and cascades the result as
/// the location every page and chrome component reads.
/// </para>
/// <para>
/// It cascades the measured <see cref="LtBreakpoint"/> as well, so the chrome and
/// the primitives render their compact forms below the small breakpoint. The width
/// is measured, not queried in a stylesheet, which keeps every layout width in the
/// one breakpoint layer.
/// </para>
/// </remarks>
public partial class ShellLayout : IAsyncDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private ExplorerLocation _location = ExplorerLocation.Initial;
    private LtBreakpoint _breakpoint = LtBreakpoint.Expanded;
    private int _version;
    private bool _directoryOpen;
    private bool _menuOpen;
    private ElementReference _root;
    private ElementReference _directory;
    private ElementReference _content;
    private ElementReference _directoryToggle;
    private ElementReference _menuToggle;
    private AddressLine? _addressLine;
    private DotNetObjectReference<ShellLayoutCallbacks>? _callbacks;
    private IJSObjectReference? _shortcuts;
    private IJSObjectReference? _viewport;
    private ILatticeStateConnection? _watchedConnection;
    private LatticeConnectionState _connectionState;
    private bool _sessionReady;
    private bool _tenantProvisional;
    private bool _tenantPending;
    private bool _resolvedOnce;
    private string? _entriesTenant;
    private (bool Authenticated, string? User)? _tenantResolvedFor;

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    [Inject]
    internal ExplorerTenancy Tenancy { get; set; } = default!;

    [Inject]
    internal ShellAppearance Appearance { get; set; } = default!;

    [Inject]
    internal ShellChromeInterop Interop { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal SessionChromeState Session { get; set; } = default!;

    [Inject]
    internal IExplorerSession ExplorerSession { get; set; } = default!;

    [Inject]
    internal IExplorerAuthSession AuthSession { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal SessionConnectionAnnouncer ConnectionAnnouncer { get; set; } = default!;

    [Inject]
    internal ShellAssertedTenant AssertedTenant { get; set; } = default!;

    [Inject]
    internal ExplorerTenantSwitch TenantSwitch { get; set; } = default!;

    [Inject]
    internal ShellHeaderPanels HeaderPanels { get; set; } = default!;

    private bool IsCompact => _breakpoint == LtBreakpoint.Compact;

    // The compact modifier is how a stylesheet reacts to the band without a width
    // query: every .lt-toolbar under it stacks.
    private string RootClass => IsCompact ? "lt-viewport lt-shell lt-shell--compact" : "lt-viewport lt-shell";

    private string DirectoryClass => _breakpoint == LtBreakpoint.Medium
        ? "lt-shell-directory lt-shell-directory--rail"
        : "lt-shell-directory";

    private string HomeHref => Navigator.Canonicalize(ExplorerAddress.Home.WithTenant(_location.Address.Tenant)).ToHref();

    private IExplorerArea? GatedArea => Directory.Find(_location.Address.Area);

    /// <summary>What a signed-in caller is told when their tenant could not be established.</summary>
    internal const string TenantUnresolvedReason =
        "Your tenant could not be established, so nothing scoped to a tenant is shown. Reload the page or sign in again to retry.";

    /// <summary>What a caller whose tenant is not known yet is shown in place of the page.</summary>
    internal const string TenantPendingLabel = "Resolving your tenant";

    // A signed-in caller at a tenant-scoped address with tenancy on and no tenant
    // established: every call would reach the cluster as the reserved default
    // tenant, so the page is withheld rather than served under the wrong tenant.
    // The browser is at an address whose tenant differs from the one the layout
    // last resolved: the switch is still in flight, so the page would ask the
    // cluster under the tenant being left.
    private bool TenantResolving =>
        _sessionReady
        && Navigator.Current is { } current
        && !string.Equals(current.Tenant, _location.Address.Tenant, StringComparison.Ordinal);

    // The page, keyed on the tenant the circuit asserts: a tenant switch disposes
    // it and builds a fresh one, so nothing a page read under one tenant is shown
    // under another even when only its route parameters changed.
    private RenderFragment TenantBody => builder =>
    {
        builder.OpenComponent<ShellTenantBoundary>(0);
        builder.SetKey(AssertedTenant.AssertedTenant ?? string.Empty);
        builder.AddComponentParameter(1, nameof(ShellTenantBoundary.ChildContent), Body);
        builder.CloseComponent();
    };

    private bool TenantUnresolved =>
        AuthSession.IsAuthenticated
        && Tenancy.IsTenantUnresolved
        && Navigator.IsTenantScoped(_location.Address);

    /// <summary>Stops listening to the directory and the browser.</summary>
    public async ValueTask DisposeAsync()
    {
        Directory.Changed -= OnDirectoryChanged;
        Session.OverlayOpening -= OnSessionOverlayOpening;
        TenantSwitch.OpenRequested -= OnTenantSwitchRequested;
        AuthSession.AuthenticationChanged -= OnSessionStateChanged;
        ExplorerSession.ConfigurationChanged -= OnSessionStateChanged;
        if (_watchedConnection is not null)
        {
            _watchedConnection.StatusChanged -= OnConnectionStatusChanged;
        }

        // A synchronisation still suspended sees a newer version and goes no further.
        _version++;
        _lifetime.Leave();

        await DisposeHandleAsync(_shortcuts);
        await DisposeHandleAsync(_viewport);
        _callbacks?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        Directory.Changed += OnDirectoryChanged;
        Session.OverlayOpening += OnSessionOverlayOpening;
        TenantSwitch.OpenRequested += OnTenantSwitchRequested;

        // An area's availability follows the circuit's connection and sign-in,
        // which change without a navigation: the session initialises, the
        // environment credential signs the circuit in, the operator connects or
        // signs in or out. Each asks the directory again, so a probe that ran
        // before the session was ready is never left standing.
        AuthSession.AuthenticationChanged += OnSessionStateChanged;
        ExplorerSession.ConfigurationChanged += OnSessionStateChanged;
        WatchConnection();

        // One announcement per lost connection for the whole circuit, whichever
        // connection indicators happen to be mounted.
        ConnectionAnnouncer.Start();
    }

    /// <inheritdoc />
    protected override Task OnParametersSetAsync() => SyncLocationAsync();

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender)
        {
            _callbacks = DotNetObjectReference.Create(new ShellLayoutCallbacks(OpenAddressLineAsync, OnViewportBandAsync));
            _shortcuts = await Interop.RegisterShortcutsAsync(_callbacks);
            _viewport = await Interop.ObserveViewportAsync(
                _root,
                _callbacks,
                [LtBreakpoints.MediumMinimumWidth, LtBreakpoints.ExpandedMinimumWidth]);
            if (_lifetime.IsLeft)
            {
                // Left while the module answered: DisposeAsync has already run, so the
                // listeners registered since are released here.
                await DisposeHandleAsync(_shortcuts);
                await DisposeHandleAsync(_viewport);
                return;
            }
        }

        if (!_lifetime.IsLeft && !Appearance.IsLoaded)
        {
            await Appearance.EnsureLoadedAsync(_lifetime.Token);
        }
    }

    // The slot outlet is internal, and the Razor compiler resolves only public
    // component tags, so the layout composes it in code.
    private static RenderFragment Slot(string name) => (RenderTreeBuilder builder) =>
    {
        builder.OpenComponent<ShellSlotOutlet>(0);
        builder.AddComponentParameter(1, nameof(ShellSlotOutlet.Name), name);
        builder.CloseComponent();
    };

    private static async ValueTask DisposeHandleAsync(IJSObjectReference? handle)
    {
        if (handle is null)
        {
            return;
        }

        try
        {
            await handle.InvokeVoidAsync("dispose");
            await handle.DisposeAsync();
        }
        catch (Exception ex) when (ex is JSDisconnectedException or JSException or InvalidOperationException or TaskCanceledException)
        {
            // The circuit or the document has gone; so has the listener.
        }
    }

    private async Task SyncLocationAsync()
    {
        var version = ++_version;
        var token = _lifetime.Token;
        _directoryOpen = false;
        _menuOpen = false;

        // Areas probe the circuit's connection and sign-in, so the persisted
        // configuration and any stored credential are loaded before the first
        // probe. It runs once per circuit; every later navigation awaits the
        // same completed task. A failed initialisation leaves the session
        // unconfigured, which every area already answers fail-closed.
        await EnsureSessionInitializedAsync();
        if (version != _version)
        {
            return;
        }

        await ResolveTenantIdentityAsync(token);
        if (version != _version)
        {
            return;
        }

        var arrived = Navigator.Current ?? ExplorerAddress.Home;
        var mayGuess = await MayGuessTenantAsync(token);
        if (version != _version)
        {
            return;
        }

        // Only the address can settle a tenant the prerender would otherwise guess.
        var pending = mayGuess && !(arrived.Tenant is not null && Navigator.IsTenantScoped(arrived));
        _tenantPending = pending;
        Tenancy.IsTenantPending = pending;

        // Only now may a page ask the cluster anything: the configuration, the
        // sign-in and the caller's tenant are all established.
        _sessionReady = true;

        // The operator verdict decides whether a caller scoped to the reserved
        // default tenant sees tenancy chrome, and canonicalisation reads it
        // synchronously, so it is refreshed before the address is resolved.
        await Tenancy.RefreshAsync(token);
        if (version != _version)
        {
            return;
        }

        if (pending)
        {
            // With no tenant active, canonicalising only drops a root a cluster-wide
            // address never carries; nothing is rooted at, or resolved against, the
            // guessed tenant.
            var canonical = Navigator.Canonicalize(arrived);
            if (!canonical.Equals(arrived))
            {
                Navigator.NavigateTo(canonical, replace: true);
                return;
            }

            HoldForTenant(arrived);
            return;
        }

        var resolution = await Navigator.ResolveAsync(arrived, token);
        if (version != _version)
        {
            return;
        }

        if (mayGuess && !string.Equals(resolution.Address.Tenant, arrived.Tenant, StringComparison.Ordinal))
        {
            // The address named a tenant the switch refused, and the fallback it
            // would redirect to is the guess: withheld like any other address.
            _tenantPending = true;
            Tenancy.IsTenantPending = true;
            HoldForTenant(arrived);
            return;
        }

        if (resolution.Notice is { } notice)
        {
            if (resolution.RedirectTo is not null)
            {
                // A refused switch left the caller somewhere they did not ask to
                // be: that stays on screen until it is read and dismissed.
                Toasts.Show(notice, LtToastTone.Warning);
            }
            else if (_resolvedOnce)
            {
                // A switch the address and the header already show is only read
                // out. The circuit's first address merely establishes the tenant
                // (a reload, a bookmark, a pasted link), so it announces nothing.
                Toasts.Announce(notice);
            }
        }

        _resolvedOnce = true;

        if (resolution.RedirectTo is { } redirect)
        {
            Navigator.NavigateTo(redirect, replace: true);
            return;
        }

        var areaChanged = !string.Equals(_location.Address.Area, resolution.Address.Area, StringComparison.Ordinal);

        // The spine's entries answer for the tenant they were asked under; after a
        // tenant switch they are not trusted to gate a page until asked again.
        var tenantChanged = !ShellAssertedTenant.Same(_entriesTenant, AssertedTenant.AssertedTenant);
        _location = _location with
        {
            Address = resolution.Address,
            TenancyActive = Tenancy.IsActive,
            EntriesLoaded = _location.EntriesLoaded && !areaChanged && !tenantChanged,
        };

        await RefreshEntriesAsync(version, token);
    }

    private async Task EnsureSessionInitializedAsync()
    {
        try
        {
            await Session.EnsureInitializedAsync(_lifetime.Token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // The session chrome reports the failure itself; the areas see an
            // unconfigured session and answer for it.
        }

        WatchConnection();
    }

    private void WatchConnection()
    {
        var connection = ExplorerSession.Connection;
        if (ReferenceEquals(connection, _watchedConnection))
        {
            return;
        }

        if (_watchedConnection is not null)
        {
            _watchedConnection.StatusChanged -= OnConnectionStatusChanged;
        }

        _watchedConnection = connection;
        _connectionState = connection.Status.State;
        connection.StatusChanged += OnConnectionStatusChanged;
    }

    // Raised on a thread-pool thread for every status report, including the
    // periodic health checks, so only a change of state asks the areas again.
    private void OnConnectionStatusChanged(LatticeConnectionStatus status)
    {
        if (status.State == _connectionState)
        {
            return;
        }

        _connectionState = status.State;
        if (_sessionReady)
        {
            Directory.Invalidate();
        }
    }

    // A sign-in or configuration change can move the caller's tenant and so the
    // canonical address as well as which areas are shown, so the whole location
    // is synchronised again rather than only the directory.
    private void OnSessionStateChanged()
    {
        WatchConnection();

        // The first synchronisation is still establishing the session, and it
        // reads the configuration, sign-in and tenant only after they settle, so a
        // change raised while it runs (the stored credential signing the circuit
        // in, say) is already accounted for. Re-entering it would only abandon it.
        if (!_sessionReady)
        {
            return;
        }

        _ = InvokeAsync(async () =>
        {
            try
            {
                await SyncLocationAsync();
                StateHasChanged();
            }
            catch (OperationCanceledException)
            {
                // The circuit is ending.
            }
        });
    }

    /// <summary>
    /// Maps the signed-in identity onto the circuit's active tenant through Core's
    /// resolver, once per identity. Core's tenant view is fail-closed: until this
    /// runs, an active view has no tenant and scopes every catalogue to nothing.
    /// A head without tenancy registers no resolver, and this does nothing.
    /// </summary>
    /// <remarks>
    /// The remembered tenant lives in the preference store, so the store is read
    /// first: the resolver then restores it on the circuit's first navigation rather
    /// than establishing a fallback that nothing reconsiders. A server prerender
    /// cannot read the store, so what the resolver establishes there is provisional,
    /// and it is resolved again on the next navigation until the store is readable.
    /// </remarks>
    private async Task ResolveTenantIdentityAsync(CancellationToken token)
    {
        var identity = (AuthSession.IsAuthenticated, AuthSession.Username);
        if ((_tenantResolvedFor == identity && !_tenantProvisional)
            || Services.GetService(typeof(IExplorerTenantIdentityResolver)) is not IExplorerTenantIdentityResolver resolver)
        {
            return;
        }

        var preferences = Services.GetService(typeof(IExplorerShellPreferences)) as IExplorerShellPreferences;
        if (preferences is { IsLoaded: false })
        {
            try
            {
                await preferences.EnsureLoadedAsync(token);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // Unreadable (a prerender): the establishment below is provisional.
            }
        }

        try
        {
            await resolver.ResolveAsync(token);
            _tenantResolvedFor = identity;
            _tenantProvisional = AuthSession.IsAuthenticated
                && preferences is { IsLoaded: false }
                && Services.GetService(typeof(IExplorerTenantView)) is IExplorerTenantView { IsActive: true };
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Unresolved stays fail-closed: no active tenant, nothing scoped in.
        }
    }

    /// <summary>
    /// Whether the tenant the resolver established is one this render could only
    /// have guessed: a server prerender that could not read the caller's remembered
    /// tenant, for a caller who can reach more than one tenant. Only the address can
    /// then settle it; otherwise nothing may render under the guess.
    /// </summary>
    /// <remarks>
    /// The live circuit never guesses: it reads the store before it resolves, and a
    /// store it still cannot read leaves the documented fallback standing rather
    /// than a page that never renders. A reachable list that cannot be read counts
    /// as more than one tenant, so a fault fails closed to the neutral state.
    /// </remarks>
    /// <param name="token">Cancels the reachable-tenant read.</param>
    private async Task<bool> MayGuessTenantAsync(CancellationToken token)
    {
        if (!_tenantProvisional || RendererInfo.IsInteractive)
        {
            return false;
        }

        if (Services.GetService(typeof(IExplorerAccessibleTenantSource)) is not IExplorerAccessibleTenantSource tenants)
        {
            // With no reachable list nothing remembered can be restored, so the
            // fallback is the only tenant the circuit can hold.
            return false;
        }

        try
        {
            return (await tenants.GetAccessibleTenantsAsync(token)).Count > 1;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return true;
        }
    }

    // The tenant is not known: the address stays as the browser gave it, and no
    // stop is asked for its availability under the guess.
    private void HoldForTenant(ExplorerAddress arrived) =>
        _location = _location with { Address = arrived, TenancyActive = Tenancy.IsActive, Entries = [], EntriesLoaded = false };

    private async Task RefreshEntriesAsync(int version, CancellationToken token)
    {
        var tenant = AssertedTenant.AssertedTenant;
        var entries = await Directory.GetEntriesAsync(token);
        if (version == _version && ShellAssertedTenant.Same(tenant, AssertedTenant.AssertedTenant))
        {
            _location = _location with { Entries = entries, EntriesLoaded = true };
            _entriesTenant = tenant;
        }
    }

    private void OnDirectoryChanged() => _ = InvokeAsync(async () =>
    {
        await RefreshEntriesAsync(_version, _lifetime.Token);
        StateHasChanged();
    });

    private Task OpenAddressLineAsync() => InvokeAsync(() => _addressLine?.OpenAsync() ?? Task.CompletedTask);

    private Task OnViewportBandAsync(int band) => InvokeAsync(() =>
    {
        var next = band switch
        {
            <= 0 => LtBreakpoint.Compact,
            1 => LtBreakpoint.Medium,
            _ => LtBreakpoint.Expanded,
        };

        if (next != _breakpoint)
        {
            _breakpoint = next;
            _directoryOpen = false;
            _menuOpen = false;
            StateHasChanged();
        }
    });

    private async Task SkipToDirectoryAsync()
    {
        if (IsCompact)
        {
            OpenDirectory();
        }
        else
        {
            await Interop.FocusAsync(_directory);
        }
    }

    private async Task SkipToAddressAsync()
    {
        if (_addressLine is not null)
        {
            await _addressLine.FocusAsync();
        }
    }

    private async Task SkipToContentAsync() => await Interop.FocusAsync(_content);

    private void OpenDirectory() => _directoryOpen = true;

    private void CloseDirectory() => _directoryOpen = false;

    private void OnDirectoryOpenChanged(bool open) => _directoryOpen = open;

    private void OpenMenu() => _menuOpen = true;

    private void OnMenuOpenChanged(bool open) => _menuOpen = open;

    // Anything activated in the overflow menu - an appearance choice, or a session
    // slot's own control such as Sign in - closes the sheet first, so a session
    // overlay it opens never stacks on top of it.
    private void CloseMenu() => _menuOpen = false;

    // Any session modal - asked for from anywhere, or the re-authentication
    // interstitial raised off the renderer by Core - closes both compact sheets
    // and every header panel before it opens, so modals never stack.
    private void OnSessionOverlayOpening(SessionOverlayKind kind) => _ = InvokeAsync(() =>
    {
        HeaderPanels.Opening(Session);
        if (_menuOpen || _directoryOpen)
        {
            _menuOpen = false;
            _directoryOpen = false;
            StateHasChanged();
        }
    });

    // Compact, the tenant switcher lives in the directory sheet, so the palette's
    // Switch tenant opens the sheet; the switcher it mounts takes the request and
    // the focus. Wider, the header's switcher answers the request itself.
    private void OnTenantSwitchRequested() => _ = InvokeAsync(() =>
    {
        if (IsCompact && !_directoryOpen)
        {
            _menuOpen = false;
            _directoryOpen = true;
            StateHasChanged();
        }
    });
}
