using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default <see cref="IExplorerTenantIdentityResolver"/>. When the tenant view
/// is active it <em>establishes</em> the circuit's active tenant from the current
/// sign-in, fail-closed: an anonymous caller is scoped to no tenant at all, and a
/// signed-in caller is scoped to the tenant they were last using - re-validated
/// against the tenants they can currently reach - or to the documented
/// single-tenant and development default
/// (<see cref="ExplorerTenantId.Default"/>) when nothing usable is remembered.
/// </summary>
/// <remarks>
/// <para>
/// <b>It establishes; it does not re-assert.</b> This resolver runs on mount, on
/// every sign-in change, and on every tenant-control refresh - including the
/// refresh that immediately follows an operator switching tenant. It therefore
/// only ever writes an active tenant when there is not already one standing for
/// the current sign-in. Unconditionally assigning here is what silently reverted
/// every switch: the switch set the tenant, the refresh that was supposed to read
/// it back overwrote it first, and the caller was told nothing.
/// </para>
/// <para>
/// <b>Signing out still clears, unconditionally.</b> Leaving a previously
/// established tenant in place for an anonymous caller would let the
/// active-tenant view keep revealing that tenant's trees after sign-out, so the
/// anonymous path always clears - an explicit switch survives a refresh, never a
/// sign-out. Signing in as somebody else re-establishes for the same reason: the
/// new identity inherits nothing from the old one.
/// </para>
/// <para>
/// This is the documented default for single-tenant and development deployments.
/// A production multi-tenant head registers its own
/// <see cref="IExplorerTenantIdentityResolver"/> (before or after
/// <see cref="ExplorerTenantServiceCollectionExtensions.AddExplorerTenantView"/>,
/// using a non-<c>TryAdd</c> replacement) that reads the authenticated
/// principal's tenant claim and maps it onto
/// <see cref="IExplorerTenantContext.ActiveTenant"/>. The switch that turns this
/// resolver on and off is the same one that registers the active
/// <see cref="IExplorerTenantView"/>; there is no separate flag.
/// </para>
/// </remarks>
internal sealed class DefaultExplorerTenantIdentityResolver(
    IExplorerTenantView view,
    IExplorerAuthSession session,
    IExplorerTenantContext context,
    IExplorerAccessibleTenantSource? accessibleTenants = null,
    IExplorerShellPreferences? preferences = null,
    IExplorerTenantScopeNotices? notices = null) : IExplorerTenantIdentityResolver
{
    // Cached so validating a remembered tenant against the reachable list costs
    // no closure allocation on the resolve path.
    private static readonly Func<string, IReadOnlyList<ExplorerTenantId>, bool> ReachablePredicate = IsReachable;

    private readonly IExplorerTenantView _view =
        view ?? throw new ArgumentNullException(nameof(view));

    private readonly IExplorerAuthSession _session =
        session ?? throw new ArgumentNullException(nameof(session));

    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    private readonly IExplorerAccessibleTenantSource? _accessibleTenants = accessibleTenants;
    private readonly IExplorerShellPreferences? _preferences = preferences;
    private readonly IExplorerTenantScopeNotices? _notices = notices;

    private string? _establishedFor;
    private bool _hasEstablished;

    // The value this resolver wrote before the preference store had hydrated, so
    // it can be reconsidered once the remembered tenant is actually readable -
    // and only ever that value, never one the caller has switched to since.
    private ExplorerTenantId? _provisional;

    /// <inheritdoc />
    public async ValueTask ResolveAsync(CancellationToken cancellationToken = default)
    {
        // Tenancy disabled: never touch the context, so a non-tenant deployment is
        // byte-for-byte unchanged (no active tenant is ever established).
        if (!_view.IsActive)
        {
            return;
        }

        if (!_session.IsAuthenticated)
        {
            // Fail-closed: an anonymous caller is scoped to no tenant, so the
            // active-tenant view reveals nothing. This overrides an established
            // scope, including an explicit switch, because signing out must not
            // leave the previous tenant readable.
            _context.ActiveTenant = null;
            _establishedFor = null;
            _hasEstablished = false;
            _provisional = null;
            return;
        }

        if (!NeedsEstablishing())
        {
            return;
        }

        _establishedFor = _session.Username;
        _hasEstablished = true;
        _context.ActiveTenant = await EstablishAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Whether an active tenant has to be written now, as opposed to one already
    /// standing for this sign-in.
    /// </summary>
    private bool NeedsEstablishing()
    {
        if (_context.ActiveTenant is not { } current)
        {
            return true;
        }

        // A different identity inherits nothing from the previous one.
        if (!_hasEstablished || !string.Equals(_establishedFor, _session.Username, StringComparison.Ordinal))
        {
            return true;
        }

        // A scope already stands. Reconsider it only when it is still exactly the
        // provisional value written before the preference store had hydrated;
        // anything else is either a settled establishment or an explicit switch,
        // and overwriting either is the defect this resolver exists to avoid.
        return _provisional is { } provisional && provisional.Equals(current);
    }

    /// <summary>
    /// Works out which tenant this sign-in should be scoped to: the remembered
    /// one when it is still reachable, otherwise a reachable fallback, with any
    /// abandonment announced rather than applied silently.
    /// </summary>
    private async ValueTask<ExplorerTenantId> EstablishAsync(CancellationToken cancellationToken)
    {
        var accessible = _accessibleTenants is null
            ? Array.Empty<ExplorerTenantId>()
            : await _accessibleTenants.GetAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);

        var fallback = ChooseFallback(accessible);

        if (_preferences is null)
        {
            _provisional = null;
            return fallback;
        }

        // The shared restore path: it re-validates the remembered id against the
        // live reachable set and forgets one that no longer resolves, so a
        // revoked, suspended or deleted tenant is never restored and never has to
        // be explained twice.
        var resolution = await _preferences.RestoreAsync(
            ExplorerPreferenceKeys.ActiveTenant,
            fallback.Value,
            accessible,
            ReachablePredicate,
            cancellationToken).ConfigureAwait(false);

        // Before the store hydrates there is nothing to restore from, so this
        // establishment is provisional: remember what was written so a later
        // resolve can reconsider exactly that value, and nothing else.
        _provisional = resolution.Reason == ExplorerPreferenceFallbackReason.NotLoaded
            ? fallback
            : null;

        if (resolution.WasAbandoned && _notices is not null && resolution.Explanation is { } explanation)
        {
            _notices.Publish(ExplorerTenantScopeNotice.RestoreAbandoned(explanation, fallback));
        }

        return string.IsNullOrEmpty(resolution.Value) ? fallback : new ExplorerTenantId(resolution.Value);
    }

    /// <summary>
    /// The tenant to use when nothing usable is remembered: the documented
    /// default when it is reachable (or when nothing is known about
    /// reachability), otherwise the first tenant the caller can reach.
    /// </summary>
    private static ExplorerTenantId ChooseFallback(IReadOnlyList<ExplorerTenantId> accessible)
    {
        if (accessible.Count == 0)
        {
            return ExplorerTenantId.Default;
        }

        for (var i = 0; i < accessible.Count; i++)
        {
            if (accessible[i] == ExplorerTenantId.Default)
            {
                return ExplorerTenantId.Default;
            }
        }

        return accessible[0];
    }

    private static bool IsReachable(string remembered, IReadOnlyList<ExplorerTenantId> accessible)
    {
        if (string.IsNullOrEmpty(remembered))
        {
            return false;
        }

        for (var i = 0; i < accessible.Count; i++)
        {
            if (string.Equals(accessible[i].Value, remembered, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
