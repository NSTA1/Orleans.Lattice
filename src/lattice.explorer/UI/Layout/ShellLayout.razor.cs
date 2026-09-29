using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Slots;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Session;

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
    private readonly CancellationTokenSource _lifetime = new();
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

    private bool IsCompact => _breakpoint == LtBreakpoint.Compact;

    // The compact modifier is how a stylesheet reacts to the band without a width
    // query: every .lt-toolbar under it stacks.
    private string RootClass => IsCompact ? "lt-viewport lt-shell lt-shell--compact" : "lt-viewport lt-shell";

    private string DirectoryClass => _breakpoint == LtBreakpoint.Medium
        ? "lt-shell-directory lt-shell-directory--rail"
        : "lt-shell-directory";

    private string HomeHref => Navigator.Canonicalize(ExplorerAddress.Home.WithTenant(_location.Address.Tenant)).ToHref();

    private IExplorerArea? GatedArea => Directory.Find(_location.Address.Area);

    /// <summary>Stops listening to the directory and the browser.</summary>
    public async ValueTask DisposeAsync()
    {
        Directory.Changed -= OnDirectoryChanged;
        Session.OverlayOpening -= OnSessionOverlayOpening;
        AuthSession.AuthenticationChanged -= OnSessionStateChanged;
        ExplorerSession.ConfigurationChanged -= OnSessionStateChanged;
        if (_watchedConnection is not null)
        {
            _watchedConnection.StatusChanged -= OnConnectionStatusChanged;
        }

        await _lifetime.CancelAsync();
        _lifetime.Dispose();

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

        // An area's availability follows the circuit's connection and sign-in,
        // which change without a navigation: the session initialises, the
        // environment credential signs the circuit in, the operator connects or
        // signs in or out. Each asks the directory again, so a probe that ran
        // before the session was ready is never left standing.
        AuthSession.AuthenticationChanged += OnSessionStateChanged;
        ExplorerSession.ConfigurationChanged += OnSessionStateChanged;
        WatchConnection();
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
        }

        if (!Appearance.IsLoaded)
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

        // The operator verdict decides whether a caller scoped to the reserved
        // default tenant sees tenancy chrome, and canonicalisation reads it
        // synchronously, so it is refreshed before the address is resolved.
        await Tenancy.RefreshAsync(token);
        if (version != _version)
        {
            return;
        }

        var arrived = Navigator.Current ?? ExplorerAddress.Home;
        var resolution = await Navigator.ResolveAsync(arrived, token);
        if (version != _version)
        {
            return;
        }

        if (resolution.Notice is { } notice)
        {
            Toasts.Show(notice, resolution.RedirectTo is null ? LtToastTone.Info : LtToastTone.Warning);
        }

        if (resolution.RedirectTo is { } redirect)
        {
            Navigator.NavigateTo(redirect, replace: true);
            return;
        }

        var areaChanged = !string.Equals(_location.Address.Area, resolution.Address.Area, StringComparison.Ordinal);
        _location = _location with
        {
            Address = resolution.Address,
            TenancyActive = Tenancy.IsActive,
            EntriesLoaded = _location.EntriesLoaded && !areaChanged,
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
        Directory.Invalidate();
    }

    private void OnSessionStateChanged()
    {
        WatchConnection();
        Directory.Invalidate();
    }

    private async Task RefreshEntriesAsync(int version, CancellationToken token)
    {
        var entries = await Directory.GetEntriesAsync(token);
        if (version == _version)
        {
            _location = _location with { Entries = entries, EntriesLoaded = true };
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
            await _directory.FocusAsync();
        }
    }

    private async Task SkipToAddressAsync()
    {
        if (_addressLine is not null)
        {
            await _addressLine.FocusAsync();
        }
    }

    private async Task SkipToContentAsync() => await _content.FocusAsync();

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
    // before it opens, so modals never stack.
    private void OnSessionOverlayOpening(SessionOverlayKind kind) => _ = InvokeAsync(() =>
    {
        if (_menuOpen || _directoryOpen)
        {
            _menuOpen = false;
            _directoryOpen = false;
            StateHasChanged();
        }
    });
}
