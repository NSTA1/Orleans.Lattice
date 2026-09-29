using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.Logging;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Framing.Broker;

namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// Hosts one Lattice App's UI in a sandboxed, fully untrusted frame (epic #3807, E4 and E5):
/// the per-launch workspace gate, the handshake, verified bundle delivery over the frame's
/// port, and relaying the frame's bridge requests through the circuit's broker.
/// </summary>
/// <remarks>
/// <para>
/// The frame is <c>&lt;iframe sandbox="allow-scripts" referrerpolicy="no-referrer"&gt;</c> with
/// no other sandbox token and no <c>allow</c> attribute, loading the app-agnostic bootstrap
/// document. The bootstrap <c>src</c> is rendered only after the host module has installed its
/// listeners, so the frame's <c>lattice.ready</c> can never be missed.
/// </para>
/// <para>
/// Every failure replaces the frame with a shell-native error; an app's UI is never shown
/// partially. A "Leave app" control sits before and after the frame, and Esc in the host
/// chrome returns focus to the address line.
/// </para>
/// </remarks>
public sealed partial class AppFrame : IAsyncDisposable
{
    /// <summary>How long the frame's bootstrap has to complete its handshake.</summary>
    internal static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(15);

    private readonly string _frameId = "appframe-" + Guid.NewGuid().ToString("N");
    private readonly CancellationTokenSource _disposal = new();

    private Phase _phase = Phase.Authorizing;
    private AppFrameFailure _failure;
    private AppFrameLaunch? _launch;
    private AppBridgeSession? _session;
    private string? _authorizedSlug;
    private string? _deliveredPath;
    private bool _loaded;
    private ElementReference _frame;
    private bool _attachRequested;
    private bool _srcArmed;
    private bool _ready;
    private string _src = AppFrameRoute.BootstrapRelativeUrl;
    private IJSObjectReference? _module;
    private DotNetObjectReference<AppFrameInterop>? _interop;
    private ITimer? _handshakeTimer;
    private int _generation;

    /// <summary>The slug of the app whose UI to host.</summary>
    [Parameter, EditorRequired]
    public string AppSlug { get; set; } = string.Empty;

    /// <summary>
    /// The app's in-frame path from the address line (the <c>{*path}</c> of <c>/apps/{slug}/open</c>),
    /// or <see langword="null"/> for the app's own start. A change is sent to the frame as
    /// <c>nav.changed</c>.
    /// </summary>
    [Parameter]
    public string? Path { get; set; }

    /// <summary>Raised when the frame reports its in-app path through <c>nav.sync</c>; the path starts with <c>/</c>.</summary>
    [Parameter]
    public EventCallback<string> OnNavSync { get; set; }

    /// <summary>Raised when the frame fails; the component has already replaced it with an error.</summary>
    [Parameter]
    public EventCallback<AppFrameFailure> OnFailure { get; set; }

    /// <summary>Raised by either "Leave app" control. When unset, the control navigates to <see cref="LeaveHref"/>.</summary>
    [Parameter]
    public EventCallback OnLeave { get; set; }

    /// <summary>Where "Leave app" navigates when <see cref="OnLeave"/> is unset, relative to the base URL.</summary>
    [Parameter]
    public string LeaveHref { get; set; } = "apps";

    /// <summary>
    /// Raised by Esc in the host chrome. When unset, focus moves to the element carrying
    /// <c>data-lt-address-line</c>.
    /// </summary>
    [Parameter]
    public EventCallback OnEscape { get; set; }

    /// <summary>The failure being shown, or <see langword="null"/> while the frame is not failed.</summary>
    public AppFrameFailure? Failure => _phase == Phase.Failed ? _failure : null;

    [Inject]
    private AppFrameBundleLoader Loader { get; set; } = default!;

    [Inject]
    private AppBridgeBroker Broker { get; set; } = default!;

    [Inject]
    private IAppFrameHostContext HostContext { get; set; } = default!;

    [Inject]
    private IJSRuntime JS { get; set; } = default!;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    [Inject]
    private TimeProvider Time { get; set; } = default!;

    [Inject]
    private ILogger<AppFrame> Logger { get; set; } = default!;

    private enum Phase
    {
        Authorizing,
        Framed,
        Running,
        Failed,
    }

    private string AppName => _launch?.DisplayName ?? AppSlug;

    /// <summary>Tells a running frame that the appearance changed (<c>context.changed</c>).</summary>
    /// <returns>A task that completes once the event was posted, or immediately when the frame is not running.</returns>
    public async Task NotifyContextChangedAsync()
    {
        if (_phase == Phase.Running && _module is not null)
        {
            await PostAsync(AppFrameMessages.ContextChanged(HostContext.Appearance)).ConfigureAwait(true);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _generation++;
        if (!_disposal.IsCancellationRequested)
        {
            await _disposal.CancelAsync().ConfigureAwait(true);
        }

        _session?.Close();
        _handshakeTimer?.Dispose();
        await DetachAsync().ConfigureAwait(true);
        if (_module is not null)
        {
            try
            {
                await _module.DisposeAsync().ConfigureAwait(true);
            }
            catch (JSDisconnectedException)
            {
            }
        }

        _interop?.Dispose();
        _disposal.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (!string.Equals(_authorizedSlug, AppSlug, StringComparison.Ordinal))
        {
            await OpenAsync().ConfigureAwait(true);
            return;
        }

        if (_launch is null || _phase is Phase.Failed or Phase.Authorizing)
        {
            return;
        }

        // Re-navigation within the same app re-checks the launch, so a frame never outlives
        // a disable, uninstall, upgrade or revision change the user navigates past.
        var generation = _generation;
        var reason = await Loader.GetRevocationAsync(_launch, _disposal.Token).ConfigureAwait(true);
        if (generation != _generation)
        {
            return;
        }

        if (reason is not null)
        {
            await RevokeAsync(reason).ConfigureAwait(true);
            return;
        }

        await SyncPathAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_phase != Phase.Framed || _attachRequested)
        {
            return;
        }

        _attachRequested = true;
        var generation = _generation;
        try
        {
            _module ??= await JS.InvokeAsync<IJSObjectReference>("import", _disposal.Token, AppFrameAssets.HostModuleImport).ConfigureAwait(true);
            _interop ??= DotNetObjectReference.Create(new AppFrameInterop(this));
            var attached = _module is not null
                && await _module.InvokeAsync<bool>("attach", _disposal.Token, _frameId, _frame, _interop).ConfigureAwait(true);
            if (generation != _generation)
            {
                return;
            }

            if (!attached)
            {
                await FailAsync(AppFrameFailure.Unavailable).ConfigureAwait(true);
                return;
            }
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or InvalidOperationException or TaskCanceledException)
        {
            if (generation == _generation)
            {
                await FailAsync(AppFrameFailure.Unavailable).ConfigureAwait(true);
            }

            return;
        }

        _src = Navigation.ToAbsoluteUri(AppFrameRoute.BootstrapRelativeUrl).AbsolutePath;
        _srcArmed = true;
        _handshakeTimer = Time.CreateTimer(
            static state => ((AppFrame)state!).OnHandshakeTimerFired(),
            this,
            HandshakeTimeout,
            Timeout.InfiniteTimeSpan);
        StateHasChanged();
    }

    internal async Task HandleFrameReadyAsync(long protocol)
    {
        if (_phase != Phase.Framed || _ready || _launch is not { } launch)
        {
            return;
        }

        _ready = true;
        _handshakeTimer?.Dispose();
        _handshakeTimer = null;

        if (protocol != AppFrameProtocol.Version || protocol < launch.Ui.MinProtocol)
        {
            await FailAsync(AppFrameFailure.ProtocolUnsupported).ConfigureAwait(true);
            return;
        }

        var generation = _generation;
        AppFrameBundleResult loaded;
        try
        {
            loaded = await Loader.LoadAsync(launch, _disposal.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (generation != _generation)
        {
            return;
        }

        if (loaded.Bundle is not { } bundle)
        {
            await FailAsync(loaded.Failure).ConfigureAwait(true);
            return;
        }

        if (!await DeliverAsync(bundle).ConfigureAwait(true))
        {
            if (generation == _generation)
            {
                await FailAsync(AppFrameFailure.Unavailable).ConfigureAwait(true);
            }

            return;
        }

        if (generation != _generation)
        {
            return;
        }

        _session = Broker.Open(launch);
        _phase = Phase.Running;
        await SyncPathAsync().ConfigureAwait(true);
        StateHasChanged();
    }

    internal async Task<string?> HandlePortMessageAsync(string? message)
    {
        if (_phase != Phase.Running || _session is not { } session)
        {
            return null;
        }

        var generation = _generation;
        AppBridgeOutcome outcome;
        try
        {
            outcome = await Broker.HandleAsync(session, message, _disposal.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            return null;
        }

        if (generation != _generation)
        {
            return null;
        }

        switch (outcome.Effect)
        {
            case AppBridgeEffect.NavSync when outcome.Argument is { } path:
                _deliveredPath = path;
                await OnNavSync.InvokeAsync(path).ConfigureAwait(true);
                break;

            case AppBridgeEffect.Revoked:
                await RevokeAsync(outcome.Argument ?? AppFrameProtocol.RevokedClosed).ConfigureAwait(true);
                return null;
        }

        return outcome.Reply;
    }

    internal Task HandleFrameFailedAsync(string? code)
    {
        if (_phase is Phase.Failed)
        {
            return Task.CompletedTask;
        }

        LogFrameFailed(Logger, AppSlug, code is not null && AppFrameProtocol.FailureCodes.Contains(code) ? code : "internal");
        return FailAsync(AppFrameFailure.FrameFailed, detach: false);
    }

    internal Task HandleFrameReloadedAsync() =>
        _phase is Phase.Failed ? Task.CompletedTask : FailAsync(AppFrameFailure.Reloaded, detach: false);

    /// <summary>
    /// The frame's last script loaded, so every handler the app registers while it loads is
    /// listening now. The address's in-app path was first posted when the bundle was
    /// delivered, before any app script ran and so before any app could listen for it; it is
    /// posted once more now, so a deep link reaches the app. Only the first report per launch
    /// counts, so a frame cannot make the host repeat itself.
    /// </summary>
    /// <returns>A task that completes when the path has been posted.</returns>
    internal async Task HandleFrameLoadedAsync()
    {
        if (_phase != Phase.Running || _loaded)
        {
            return;
        }

        _loaded = true;
        _deliveredPath = null;
        await SyncPathAsync().ConfigureAwait(true);
    }

    private async Task OpenAsync()
    {
        await ResetAsync().ConfigureAwait(true);
        _authorizedSlug = AppSlug;
        var generation = _generation;

        AppFrameLaunchResult result;
        try
        {
            result = await Loader.AuthorizeAsync(AppSlug, _disposal.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (generation != _generation)
        {
            return;
        }

        if (result.Launch is not { } launch)
        {
            await FailAsync(result.Failure).ConfigureAwait(true);
            return;
        }

        _launch = launch;
        _phase = Phase.Framed;
    }

    private async Task ResetAsync()
    {
        _generation++;
        _session?.Close();
        _session = null;
        _handshakeTimer?.Dispose();
        _handshakeTimer = null;
        await DetachAsync().ConfigureAwait(true);
        _launch = null;
        _phase = Phase.Authorizing;
        _failure = default;
        _attachRequested = false;
        _srcArmed = false;
        _ready = false;
        _deliveredPath = null;
        _loaded = false;
    }

    private async Task<bool> DeliverAsync(AppFrameBundle bundle)
    {
        if (_module is null)
        {
            return false;
        }

        try
        {
            foreach (var asset in bundle.Assets)
            {
                var stream = MemoryMarshal.TryGetArray(asset.Bytes, out var segment) && segment.Array is not null
                    ? new MemoryStream(segment.Array, segment.Offset, segment.Count, writable: false)
                    : new MemoryStream(asset.Bytes.ToArray(), writable: false);
                using var reference = new DotNetStreamReference(stream, leaveOpen: false);
                if (!await _module.InvokeAsync<bool>("stageAsset", _disposal.Token, _frameId, asset.Path, reference).ConfigureAwait(true))
                {
                    return false;
                }
            }

            return await _module.InvokeAsync<bool>(
                "sendBundle",
                _disposal.Token,
                _frameId,
                AppFrameMessages.Bundle(bundle, HostContext.Appearance)).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException)
        {
            return false;
        }
    }

    private async Task SyncPathAsync()
    {
        if (_phase != Phase.Running || string.IsNullOrEmpty(Path))
        {
            return;
        }

        var path = Path[0] == '/' ? Path : "/" + Path;
        if (string.Equals(path, _deliveredPath, StringComparison.Ordinal) || !AppBridgeBroker.IsSafeText(path, AppFrameProtocol.MaxPathLength))
        {
            return;
        }

        _deliveredPath = path;
        await PostAsync(AppFrameMessages.NavChanged(path)).ConfigureAwait(true);
    }

    private async Task PostAsync(string message)
    {
        if (_module is null)
        {
            return;
        }

        try
        {
            await _module.InvokeAsync<bool>("post", _disposal.Token, _frameId, message).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException)
        {
        }
    }

    private async Task RevokeAsync(string reason)
    {
        _session?.Close();
        if (_module is not null)
        {
            try
            {
                await _module.InvokeAsync<bool>("revoke", _disposal.Token, _frameId, reason).ConfigureAwait(true);
            }
            catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException)
            {
            }
        }

        await FailAsync(AppFrameFailure.Revoked, detach: false).ConfigureAwait(true);
    }

    private async Task FailAsync(AppFrameFailure failure, bool detach = true)
    {
        _generation++;
        _session?.Close();
        _session = null;
        _handshakeTimer?.Dispose();
        _handshakeTimer = null;
        if (detach)
        {
            await DetachAsync().ConfigureAwait(true);
        }

        _phase = Phase.Failed;
        _failure = failure;
        _srcArmed = false;
        StateHasChanged();
        await OnFailure.InvokeAsync(failure).ConfigureAwait(true);
    }

    private async Task DetachAsync()
    {
        if (_module is null || !_attachRequested)
        {
            return;
        }

        try
        {
            await _module.InvokeAsync<bool>("detach", CancellationToken.None, _frameId).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or ObjectDisposedException)
        {
        }
    }

    private void OnHandshakeTimerFired()
    {
        var generation = _generation;
        _ = InvokeAsync(async () =>
        {
            if (generation == _generation && _phase == Phase.Framed && !_ready)
            {
                await FailAsync(AppFrameFailure.HandshakeTimeout).ConfigureAwait(true);
            }
        });
    }

    private async Task LeaveAsync()
    {
        if (OnLeave.HasDelegate)
        {
            await OnLeave.InvokeAsync().ConfigureAwait(true);
            return;
        }

        Navigation.NavigateTo(LeaveHref);
    }

    private async Task HandleKeyDownAsync(KeyboardEventArgs args)
    {
        if (!string.Equals(args.Key, "Escape", StringComparison.Ordinal))
        {
            return;
        }

        if (OnEscape.HasDelegate)
        {
            await OnEscape.InvokeAsync().ConfigureAwait(true);
            return;
        }

        if (_module is null)
        {
            return;
        }

        try
        {
            await _module.InvokeAsync<bool>("focusAddressLine", _disposal.Token).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException)
        {
        }
    }

    private static string FailureTitle(AppFrameFailure failure) => failure switch
    {
        AppFrameFailure.NoUi => "This app has no interface",
        AppFrameFailure.DigestMismatch or AppFrameFailure.BundleDigestMismatch => "The app's interface failed verification",
        AppFrameFailure.BundleInvalid => "The app's interface could not be loaded",
        AppFrameFailure.HandshakeTimeout => "The app did not start",
        AppFrameFailure.ProtocolUnsupported => "This app needs a newer Explorer",
        AppFrameFailure.FrameFailed => "The app failed to start",
        AppFrameFailure.Reloaded => "The app was closed",
        AppFrameFailure.Revoked => "This app has changed",
        AppFrameFailure.Unavailable => "The app could not be opened",
        _ => "This app is not available",
    };

    private static string FailureText(AppFrameFailure failure) => failure switch
    {
        AppFrameFailure.NoUi => "The installed version of this app does not ship a user interface. Its other pages are still available.",
        AppFrameFailure.DigestMismatch => "A file in the app's interface does not match the digest its manifest pins, so nothing was loaded.",
        AppFrameFailure.BundleDigestMismatch => "The app's interface bundle does not match its declared digest, so nothing was loaded.",
        AppFrameFailure.BundleInvalid => "The app's interface bundle is malformed or too large, so nothing was loaded.",
        AppFrameFailure.HandshakeTimeout => "The app's frame did not respond in time.",
        AppFrameFailure.ProtocolUnsupported => "The app requires a frame protocol this Explorer does not provide.",
        AppFrameFailure.FrameFailed => "The app's frame reported that it could not load its interface.",
        AppFrameFailure.Reloaded => "The app's frame loaded a new page, so it was closed.",
        AppFrameFailure.Revoked => "The app was disabled, uninstalled or upgraded since it was opened. Open it again to continue.",
        AppFrameFailure.Unavailable => "The Explorer could not reach the cluster or the browser. Try again.",
        _ => "You do not have access to this app, or it is not enabled.",
    };

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "The frame for '{AppSlug}' reported failure '{Code}'.")]
    private static partial void LogFrameFailed(ILogger logger, string appSlug, string code);
}
