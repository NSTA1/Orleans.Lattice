using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The circuit's top-bar tenant switch: what the switcher offers this caller,
/// the switch itself, and the palette's request to open the switcher.
/// </summary>
/// <remarks>
/// <para>
/// <b>The offer fails closed.</b> The switcher is offered only to a signed-in
/// caller whose tenancy is on, whom the operator-gated switcher lets switch, and
/// who can reach at least two tenants. The tenants are read from the same
/// accessible-tenant source as the address line's <c>t/</c> completions and the
/// Tenancy directory, so the three can never disagree. Any fault reads as
/// "nothing offered".
/// </para>
/// <para>
/// <b>The offer is filed under who asked.</b> The last offer is remembered with
/// the caller (<see cref="ShellCaller"/>: the sign-in, the endpoint and the
/// asserted tenant) and the active tenant it was read for, and
/// <see cref="Current"/> hands it out only while both still hold, so a sign-in, a
/// sign-out, a new connection or a switch never shows the previous caller's list.
/// </para>
/// <para>
/// <b>The switch is the address line's switch.</b> At a tenant-scoped address it
/// navigates to the address re-rooted at the chosen tenant, and the layout
/// resolves it through the operator-gated switch exactly as a typed
/// <c>t/{tenant}</c> address, announcing a refusal with the same notice. A
/// cluster-wide address has no tenant root, so the caller stays where they are
/// and the tenant is switched in place through the same switch.
/// </para>
/// </remarks>
internal sealed class ExplorerTenantSwitch
{
    private readonly ExplorerTenancy _tenancy;
    private readonly ExplorerNavigator _navigator;
    private readonly LtToastService _toasts;
    private readonly IExplorerAuthSession? _auth;
    private readonly ShellCaller _caller;
    private Snapshot? _last;
    private bool _openRequested;

    /// <summary>Creates the switch over the circuit's tenancy.</summary>
    /// <param name="tenancy">The caller's tenancy.</param>
    /// <param name="navigator">Moves the Explorer between addresses.</param>
    /// <param name="toasts">Where a switch made in place is announced.</param>
    /// <param name="auth">The circuit's sign-in, or <see langword="null"/> when none is registered.</param>
    /// <param name="asserted">The tenant the circuit's calls assert, or <see langword="null"/>.</param>
    /// <param name="caller">The circuit's caller; when <see langword="null"/>, a caller over <paramref name="auth"/> and <paramref name="asserted"/>.</param>
    public ExplorerTenantSwitch(
        ExplorerTenancy tenancy,
        ExplorerNavigator navigator,
        LtToastService toasts,
        IExplorerAuthSession? auth = null,
        ShellAssertedTenant? asserted = null,
        ShellCaller? caller = null)
    {
        ArgumentNullException.ThrowIfNull(tenancy);
        ArgumentNullException.ThrowIfNull(navigator);
        ArgumentNullException.ThrowIfNull(toasts);

        _tenancy = tenancy;
        _navigator = navigator;
        _toasts = toasts;
        _auth = auth;
        _caller = caller ?? ShellCaller.Unobserved(auth, tenant: asserted);
    }

    /// <summary>Raised when the palette asks for the switcher to open.</summary>
    public event Action? OpenRequested;

    /// <summary>
    /// The last offer read, while the identity and the tenant it was read for
    /// still hold; otherwise <see cref="TenantSwitchChoices.None"/>.
    /// </summary>
    public TenantSwitchChoices Current => _last is { } last && last.Key == Key() ? last.Choices : TenantSwitchChoices.None;

    /// <summary>Whether an open request is waiting for a switcher to take it.</summary>
    public bool IsOpenRequested => _openRequested;

    /// <summary>
    /// Reads what the switcher offers this caller now. An unchanged offer is
    /// handed back as the same instance, so a field bound to it keeps its state.
    /// </summary>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The offer; <see cref="TenantSwitchChoices.None"/> on any fault.</returns>
    public async ValueTask<TenantSwitchChoices> RefreshAsync(CancellationToken cancellationToken = default)
    {
        var key = Key();
        TenantSwitchChoices read;
        try
        {
            read = await LoadAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            read = TenantSwitchChoices.None;
        }

        if (Key() != key)
        {
            // The caller or the tenant moved while the offer was read: it answers
            // for a caller who is no longer here, so it is neither kept nor shown.
            return TenantSwitchChoices.None;
        }

        if (_last is { } last && last.Key == key && last.Choices.SameAs(read))
        {
            return last.Choices;
        }

        _last = new Snapshot(key, read);
        return read;
    }

    /// <summary>
    /// Switches to <paramref name="tenant"/>: re-roots a tenant-scoped address at
    /// it, or switches in place at a cluster-wide one.
    /// </summary>
    /// <param name="tenant">The tenant id.</param>
    /// <param name="cancellationToken">Cancels an in-place switch.</param>
    /// <returns>What happened.</returns>
    public async Task<TenantSwitchOutcome> SwitchAsync(string tenant, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenant);

        if (string.Equals(tenant, _tenancy.ActiveTenant, StringComparison.Ordinal))
        {
            return TenantSwitchOutcome.Unchanged;
        }

        var current = _navigator.Current ?? ExplorerAddress.Home;
        if (_navigator.IsTenantScoped(current))
        {
            // The layout resolves the re-rooted address through the operator-gated
            // switch, and announces a refusal, exactly as for a typed t/{tenant}.
            _navigator.NavigateTo(_navigator.ReRoot(current, tenant));
            return TenantSwitchOutcome.Navigated;
        }

        if (!await _tenancy.TrySwitchAsync(tenant, cancellationToken).ConfigureAwait(true))
        {
            _toasts.Show(ExplorerNavigator.RefusedNotice(tenant, _tenancy.ActiveTenant), LtToastTone.Warning);
            return TenantSwitchOutcome.Refused;
        }

        // The operator verdict that decides whether the reserved default tenant is
        // shown belongs to the tenant just left, so it is read again, and the
        // layout is asked to synchronise with the new tenant without moving.
        await _tenancy.RefreshAsync(cancellationToken).ConfigureAwait(true);
        _toasts.Announce(ExplorerNavigator.SwitchedNotice(tenant));
        _navigator.NavigateTo(current, replace: true);
        return TenantSwitchOutcome.Switched;
    }

    /// <summary>Asks the switcher to open, as the palette's <c>Switch tenant</c> command does.</summary>
    public void RequestOpen()
    {
        _openRequested = true;
        OpenRequested?.Invoke();
    }

    /// <summary>Takes a waiting open request, so exactly one switcher answers it.</summary>
    /// <returns>Whether a request was waiting.</returns>
    public bool TryTakeOpenRequest()
    {
        var requested = _openRequested;
        _openRequested = false;
        return requested;
    }

    private async ValueTask<TenantSwitchChoices> LoadAsync(CancellationToken cancellationToken)
    {
        // A signed-out caller never sees it, whatever tenancy says.
        if (_auth is not { IsAuthenticated: true } || !await _tenancy.CanSwitchAsync(cancellationToken).ConfigureAwait(true))
        {
            return TenantSwitchChoices.None;
        }

        var tenants = await _tenancy.GetAccessibleTenantsAsync(cancellationToken).ConfigureAwait(true);
        var choices = TenantSwitchChoices.Of(_tenancy.ActiveTenant, tenants);
        return choices.Offered ? choices : TenantSwitchChoices.None;
    }

    private SnapshotKey Key() => new(_caller.Current, _tenancy.ActiveTenant);

    private readonly record struct SnapshotKey(ShellCallerKey Caller, string? Active);

    private sealed record Snapshot(SnapshotKey Key, TenantSwitchChoices Choices);
}
