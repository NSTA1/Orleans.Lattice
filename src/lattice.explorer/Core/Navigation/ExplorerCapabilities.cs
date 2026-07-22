namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The cached, advisory capability map probed once after sign-in / reconnect and
/// consulted by the shell to enable or disable areas and per-scope actions. It is
/// a UX affordance only: the server stays the fail-closed enforcement point, so
/// every action must still handle a runtime denial regardless of what this map
/// says. Discard and re-probe on an auth or endpoint change rather than mutating
/// in place.
/// </summary>
public sealed record ExplorerCapabilities
{
    /// <summary>The empty map: the backup, access, and schema areas are unreachable and every scope is denied.</summary>
    public static ExplorerCapabilities Empty { get; } = new();

    /// <summary>
    /// The coarse top-level gate for the Backups area: <see langword="true"/> when
    /// the endpoint reports at least list / read backup access. Computed once from
    /// the catalog-level probe so the area entry can be enabled without a per-scope
    /// probe.
    /// </summary>
    public bool BackupListAllowed { get; init; }

    /// <summary>
    /// The coarse top-level gate for the Access (membership &amp; access-control)
    /// area: <see langword="true"/> when the auth-admin control plane accepts the
    /// caller as an administrator (a light, side-effect-free list probe
    /// succeeds). Every admin operation is gated twice more on the server, so this
    /// flag is a UX affordance only.
    /// </summary>
    public bool AuthAdminAllowed { get; init; }

    /// <summary>
    /// <see langword="true"/> when the coarse Access probe failed specifically
    /// because the cluster connection is <em>unauthenticated</em> - the caller
    /// carried no accepted credential (a gRPC <c>Unauthenticated</c> status, or a
    /// denial while no sign-in is applied) - rather than an authenticated caller
    /// being denied administrator access. This distinguishes a recoverable
    /// "sign-in required" connection state from an advisory "not permitted"
    /// grey-out, so the shell can prompt a (re-)sign-in instead of collapsing both
    /// into the same silent deny. Never <see langword="true"/> together with
    /// <see cref="AuthAdminAllowed"/>.
    /// </summary>
    public bool AuthAdminAuthenticationRequired { get; init; }

    /// <summary>
    /// <see langword="true"/> when the cluster reports a searchable identity
    /// directory (so candidate principal ids can be validated before access is
    /// granted). Read from the access-model probe; <see langword="false"/> when
    /// the probe was denied, unreachable, or the cluster has no directory. Advisory
    /// only - the create forms still handle an unavailable directory at call time.
    /// </summary>
    public bool AuthDirectoryAvailable { get; init; }

    /// <summary>
    /// The cluster's best-effort active authentication mode, read from the
    /// access-model probe. <see cref="ExplorerAccessAuthenticationMode.Unknown"/>
    /// when the probe was denied, unreachable, or has not run. Advisory only,
    /// surfaced so the Access area can render the right create-form guidance.
    /// </summary>
    public ExplorerAccessAuthenticationMode AuthAuthenticationMode { get; init; }
        = ExplorerAccessAuthenticationMode.Unknown;

    /// <summary>
    /// The coarse top-level gate for the Schema area: <see langword="true"/> when the
    /// schema control plane is reachable (a side-effect-free capability probe
    /// completes without a transport fault). The per-tree, per-action grey-out inside
    /// the panel is driven by a separate per-tree probe, so this flag is a UX
    /// affordance only and every schema operation is still gated on the server.
    /// </summary>
    public bool SchemaAllowed { get; init; }

    /// <summary>
    /// The per-scope (per-tree) capability snapshots gathered as the user opens
    /// scopes. Keyed by tree id. A scope absent from the map falls back to
    /// <see cref="BackupScopeCapabilitySnapshot.None"/> (deny) until probed.
    /// </summary>
    public IReadOnlyDictionary<string, BackupScopeCapabilitySnapshot> BackupByScope { get; init; }
        = new Dictionary<string, BackupScopeCapabilitySnapshot>(StringComparer.Ordinal);

    /// <summary>
    /// Returns the capability snapshot for <paramref name="treeId"/>, or
    /// <see cref="BackupScopeCapabilitySnapshot.None"/> when the scope has not been
    /// probed. Never returns <see langword="null"/>.
    /// </summary>
    /// <param name="treeId">The tree id whose scope capabilities to read. Must not be <see langword="null"/>.</param>
    public BackupScopeCapabilitySnapshot ForScope(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return BackupByScope.TryGetValue(treeId, out var snapshot)
            ? snapshot
            : BackupScopeCapabilitySnapshot.None;
    }
}
