using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;

/// <summary>
/// The telemetry surface's view state and operations, lifted out of the panels'
/// code-behind so the area panel and the My Tenant metrics section are two
/// mounts of one implementation rather than two copies of one.
/// <para>
/// Everything it does runs against its single controlled domain contract
/// (<see cref="ITelemetryDomain"/>) plus the keyed plugin access store; it holds
/// no connection, no channel, and no container (epic decision D3).
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Nothing here polls.</b> Every load is caused by something that happened -
/// a mount, a control the caller moved, a Refresh they pressed, a gate that
/// opened, a tenant the shell switched to - so the surface is driven by
/// dispatched values and never by a timer. A metrics panel on a repeating timer
/// is the classic way a UI test becomes flaky and a circuit keeps working after
/// its tab is gone; there is no timer to leak here because there is no timer.
/// </para>
/// <para>
/// <b>The clock is injected.</b> Building a concrete window needs an instant,
/// and reading it from <see cref="DateTimeOffset.UtcNow"/> would make every
/// assertion about the window a race. It comes from a
/// <see cref="TimeProvider"/>, so a test states the instant and the window is
/// exact.
/// </para>
/// <para>
/// <b>The requested visibility is read from the domain on every request.</b> It
/// is whatever the shell's own tenant switcher is asking for - this surface adds
/// no visibility control of its own, because a second control asking the same
/// question is a second thing that can disagree with the shell. What was
/// actually applied comes back on the response and is what the panel renders.
/// </para>
/// </remarks>
public sealed partial class TelemetryWorkspace : IDisposable
{
    private readonly ITelemetryDomain _domain;
    private readonly IExplorerPluginAccessStore _store;
    private readonly TimeProvider _clock;
    private readonly bool _pinnedToOwnTenant;
    private readonly IExplorerShellPreferences? _preferences;
    private readonly IExplorerShellRouter? _router;

    /// <summary>
    /// Creates the workspace over the plugin's domain contract and the keyed
    /// access store its gate publishes into. Reads the current gate decision
    /// immediately, so a view rendered before the first probe completes is
    /// fail-closed rather than optimistic.
    /// </summary>
    /// <param name="domain">The plugin's controlled domain contract. Must not be <see langword="null"/>.</param>
    /// <param name="store">The keyed plugin access store. Must not be <see langword="null"/>.</param>
    /// <param name="clock">
    /// The clock a concrete window is measured from. Defaults to
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <param name="pinnedToOwnTenant">
    /// Whether this mount is the My Tenant metrics section, which is pinned to
    /// the caller's own tenant: it always requests
    /// <see cref="ExplorerTelemetryVisibility.ActiveTenant"/> regardless of what
    /// the shell's switcher is asking for, so a platform operator's cross-tenant
    /// intent cannot leak into a surface that says "your tenant" on it.
    /// </param>
    /// <param name="preferences">
    /// The shell's declared preference contract, when this mount should remember
    /// which panel the caller was on. Omitted by a mount that is a section of
    /// somebody else's surface: two mounts writing one key would each keep
    /// overwriting the other's answer.
    /// </param>
    /// <param name="router">
    /// The shell's route model, when this mount should make the selected panel
    /// addressable. Omitted for the same reason as
    /// <paramref name="preferences"/>: a section has no claim on the address.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="domain"/> or <paramref name="store"/> is <see langword="null"/>.
    /// </exception>
    public TelemetryWorkspace(
        ITelemetryDomain domain,
        IExplorerPluginAccessStore store,
        TimeProvider? clock = null,
        bool pinnedToOwnTenant = false,
        IExplorerShellPreferences? preferences = null,
        IExplorerShellRouter? router = null)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(store);

        _domain = domain;
        _store = store;
        _clock = clock ?? TimeProvider.System;
        _pinnedToOwnTenant = pinnedToOwnTenant;
        _preferences = preferences;
        _router = router;

        // Composed up front rather than left at its default, so a view rendered
        // before the first evaluation shows the fail-closed scope in words
        // instead of an empty caption.
        Caption = TelemetryScopeCaptions.For(ExplorerTelemetryScope.None, domain.IsTenancyEnabled);

        ReadAccess();
        _store.Changed += OnAccessChanged;
    }

    /// <summary>Raised whenever the observable state changes, so the views re-render.</summary>
    public event Action? Changed;

    /// <summary>Whether a request is in flight, so every control renders disabled.</summary>
    public bool Busy { get; private set; }

    /// <summary>Whether the plugin's own gate currently admits the caller.</summary>
    public bool Allowed { get; private set; }

    /// <summary>
    /// Whether the gate refused because the connection carries no accepted
    /// credential, rather than because an authenticated caller was denied. The
    /// panel prompts a sign-in for this state instead of greying out.
    /// </summary>
    public bool AuthenticationRequired { get; private set; }

    /// <summary>
    /// Whether the cluster serves no telemetry facade at all, in which case the
    /// whole surface renders nothing rather than an error.
    /// </summary>
    public bool Unavailable { get; private set; }

    /// <summary>The reason the gate gave, when it gave one.</summary>
    public string? AccessReason { get; private set; }

    /// <summary>
    /// Whether this mount is pinned to the caller's own tenant, so a view can
    /// omit anything that would imply a wider scope is reachable from here.
    /// </summary>
    public bool IsPinnedToOwnTenant => _pinnedToOwnTenant;

    /// <summary>
    /// Whether the head has tenant scoping at all. With no tenancy add-on the
    /// same catalogue and the same panels render, unscoped, and the tenant
    /// wording disappears - there are deliberately no tenancy-on and
    /// tenancy-off panel variants.
    /// </summary>
    public bool IsTenancyEnabled => _domain.IsTenancyEnabled;

    /// <inheritdoc />
    public void Dispose() => _store.Changed -= OnAccessChanged;

    private void ReadAccess()
    {
        var access = _store.Get(TelemetryPluginKeys.PluginId);
        Allowed = access.IsAllowed;
        AuthenticationRequired = access.State == ExplorerPluginAccessState.AuthenticationRequired;
        Unavailable = access.State == ExplorerPluginAccessState.Unavailable;
        AccessReason = access.Reason;
    }

    private void OnAccessChanged(ExplorerPluginAccessChange change)
    {
        // Only this plugin's own key matters: a sibling plugin's probe
        // completing must not re-render this one.
        if (!string.Equals(change.Key.PluginId, TelemetryPluginKeys.PluginId, StringComparison.Ordinal))
        {
            return;
        }

        var wasAllowed = Allowed;
        ReadAccess();

        // A gate that has just opened for the first time wants its catalogue
        // read without the caller reaching for Refresh. An already-initialized
        // surface falls through to the re-render instead: InitializeAsync is
        // idempotent and would announce nothing, so a gate that closed and
        // re-opened - a token expiring, then a sign-in - would otherwise leave
        // the denied message on screen with a full catalogue already in hand.
        if (!wasAllowed && Allowed && !_initialized)
        {
            _ = InitializeAsync();
            return;
        }

        RaiseChanged();
    }

    private void RaiseChanged() => Changed?.Invoke();
}
