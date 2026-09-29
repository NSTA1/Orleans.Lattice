using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// The session overlay, rendered in the <c>overlay.session</c> chrome slot: the
/// connection gate on first run and whenever no valid endpoint is configured,
/// the connection settings and sign-in dialogs when the circuit asks for them,
/// and the re-authentication interstitial when a sign-in can no longer be
/// renewed.
/// </summary>
/// <remarks>
/// <para>
/// It shows at most one surface, in priority order: re-authentication first
/// (nothing else can succeed until it does), then the mandatory connection gate,
/// then whatever the circuit asked for through <see cref="SessionChromeState"/>.
/// </para>
/// <para>
/// It also drives the circuit's one-time initialisation - loading the persisted
/// configuration and any stored credential - exactly as the old
/// <c>ConfigurationGate</c> did, so every other session control can simply read
/// Core's state and re-render on its change events.
/// </para>
/// </remarks>
public partial class SessionOverlay
{
    private readonly EventCallback _close;

    /// <summary>Creates the overlay, binding its one close callback once rather than per render.</summary>
    public SessionOverlay() => _close = EventCallback.Factory.Create(this, Close);

    [Inject]
    private SessionChromeState State { get; set; } = default!;

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        State.Changed -= OnChanged;
        Explorer.ConfigurationChanged -= OnChanged;
    }

    /// <inheritdoc />
    protected override Task OnInitializedAsync()
    {
        State.Changed += OnChanged;
        Explorer.ConfigurationChanged += OnChanged;
        return State.EnsureInitializedAsync();
    }

    private void Close() => State.CloseOverlay();

    private void OnChanged() => _ = InvokeAsync(StateHasChanged);
}
