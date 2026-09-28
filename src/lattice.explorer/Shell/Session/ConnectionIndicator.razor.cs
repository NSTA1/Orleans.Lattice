using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// The connection indicator, rendered in the <c>header.connection</c> chrome
/// slot: the connected endpoint, its state drawn in a health state role and
/// always named in words, a Reconnect affordance when the connection is down, a
/// Sign in affordance when the endpoint refused an anonymous call, and the way
/// into the connection settings.
/// </summary>
/// <remarks>
/// <para>
/// Below the small breakpoint the header folds into its overflow menu, and the
/// indicator renders there folded: the state and endpoint as a definition list,
/// with the actions beneath them.
/// </para>
/// <para>
/// When the connection falls into its faulted state the endpoint's own
/// explanation is posted once as a toast, so a sighted keyboard user sees why and
/// not only that. A repeat of the same fault posts nothing.
/// </para>
/// </remarks>
public partial class ConnectionIndicator
{
    private bool _reconnecting;
    private LatticeConnectionState _lastState;

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    [Inject]
    private IExplorerAuthSession Auth { get; set; } = default!;

    [Inject]
    private SessionChromeState State { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    private LatticeConnectionStatus Status => Explorer.Connection.Status;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    private LtBreakpoint? Breakpoint { get; set; }

    private bool Folded => SessionPresentation.IsFolded(Breakpoint);

    private SessionConnectionPresentation Presentation => SessionConnectionPresentation.For(Status, Explorer.IsConfigured);

    /// <inheritdoc />
    public void Dispose()
    {
        Explorer.Connection.StatusChanged -= OnStatusChanged;
        Explorer.ConfigurationChanged -= OnChanged;
        Auth.AuthenticationChanged -= OnChanged;
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _lastState = Status.State;
        Explorer.Connection.StatusChanged += OnStatusChanged;
        Explorer.ConfigurationChanged += OnChanged;
        Auth.AuthenticationChanged += OnChanged;
    }

    private void OpenSignIn() => State.OpenSignIn();

    private void OpenConfiguration() => State.OpenConfiguration();

    private async Task ReconnectAsync()
    {
        if (_reconnecting)
        {
            return;
        }

        _reconnecting = true;
        try
        {
            await Explorer.Connection.ReconnectAsync();
        }
        finally
        {
            _reconnecting = false;
        }
    }

    private void OnStatusChanged(LatticeConnectionStatus status) => _ = InvokeAsync(() => Announce(status));

    private void Announce(LatticeConnectionStatus status)
    {
        if (status.State == LatticeConnectionState.Faulted && _lastState != LatticeConnectionState.Faulted)
        {
            var where = status.Endpoint is { Length: > 0 } endpoint ? $" from {endpoint}" : string.Empty;
            var why = status.Message is { Length: > 0 } message ? $": {message}" : ".";
            Toasts.Show($"Disconnected{where}{why}", LtToastTone.Danger);
        }

        _lastState = status.State;
        StateHasChanged();
    }

    private void OnChanged() => _ = InvokeAsync(StateHasChanged);
}
