using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Session;

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
/// explanation is posted once as a toast by the circuit's
/// <see cref="SessionConnectionAnnouncer"/>, and withdrawn when it recovers.
/// </para>
/// </remarks>
public partial class ConnectionIndicator
{
    private bool _reconnecting;

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    [Inject]
    private IExplorerAuthSession Auth { get; set; } = default!;

    [Inject]
    private SessionChromeState State { get; set; } = default!;

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

    // The circuit's SessionConnectionAnnouncer raises the one outage toast; the
    // indicator only re-renders its own state.
    private void OnStatusChanged(LatticeConnectionStatus status) => _ = InvokeAsync(StateHasChanged);

    private void OnChanged() => _ = InvokeAsync(StateHasChanged);
}
