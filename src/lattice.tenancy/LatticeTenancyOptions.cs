namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Tenancy</c> registry add-on: the
/// durable per-key history retention over the <c>sys-tenant-registry</c>
/// definition tree, whether
/// the durable history materialised view is created, and whether the reserved
/// default tenant is seeded. Resolved through the standard options system and
/// configured via <c>AddLatticeTenancy(...)</c> or
/// <c>ConfigureLatticeTenancy(...)</c>.
/// </summary>
public sealed class LatticeTenancyOptions
{
    /// <summary>
    /// The retention mode for the durable per-key history captured on the
    /// <c>sys-tenant-registry</c> definition tree (the usage and overage trees
    /// carry no durable history). Defaults to
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>; history is never disabled
    /// by default.
    /// </summary>
    public HistoryRetentionMode HistoryRetentionMode { get; set; } = HistoryRetentionMode.MetadataOnly;

    /// <summary>
    /// The age after which a tenant-registry history revision row expires, or
    /// <c>null</c> for no age bound (the default). Must be strictly positive when
    /// supplied.
    /// </summary>
    public TimeSpan? HistoryRetentionWindow { get; set; }

    /// <summary>
    /// Whether to create the durable history materialised view over the
    /// <c>sys-tenant-registry</c> definition tree. Defaults to <c>true</c> so tenant
    /// definition history is queryable without a process restart.
    /// </summary>
    public bool EnableDurableHistoryView { get; set; } = true;

    /// <summary>
    /// Whether to seed the reserved <see cref="TenantId.Default"/> tenant with an
    /// unbounded quota at startup when it is absent. Defaults to <c>true</c>.
    /// The seed is create-if-absent, so it never clobbers an operator's later
    /// edits on restart.
    /// </summary>
    public bool SeedDefaultTenant { get; set; } = true;

    /// <summary>
    /// How long a silo may treat its compiled tenant-policy snapshot as
    /// authoritative without renewing its lease from the cluster-wide tenant-policy
    /// epoch grain. Defaults to 10 seconds; must be strictly positive and no longer
    /// than a timer can wait (<c>0xFFFFFFFE</c> milliseconds, about 49.7 days).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A silo renews every third of this duration. The cluster-wide tenant-policy
    /// epoch pushes each registry change to every leased silo; the live per-silo
    /// lease then lets the compiled policy, residency, and placement snapshots
    /// answer from memory only while they have been rebuilt for the latest epoch.
    /// Once the lease lapses (the epoch grain is unreachable), policy and residency
    /// checks confirm against the tenant registry or deny, and placement resolution
    /// refuses a tenant-tree registration rather than seed a WAL placement from a
    /// stale snapshot, until a renewal succeeds.
    /// </para>
    /// <para>
    /// It is also the worst case a tenant-registry write can be delayed by after a
    /// steady-state advance has started: the write completes only once every leased
    /// silo has acknowledged the change or its lease has lapsed, so a silo that
    /// cannot be reached costs a registry write up to about 1.1 times this duration
    /// (the lease plus the ledger's clock-rate margin). A freshly restarted epoch
    /// grain may also wait the same grace for leases granted by the previous
    /// incarnation, unless cluster membership proves every live silo has leased from
    /// the new one. Keep the duration well below the Orleans response timeout.
    /// Shorter values bound that delay more tightly at the cost of more frequent
    /// renewals (one small grain call per silo per third of a lease).
    /// </para>
    /// </remarks>
    public TimeSpan PolicySnapshotLeaseDuration { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Whether delegated tenant access administration is enabled: tenant groups,
    /// tenant member sets, group entries in a tenant's admin set, and tenant-tier
    /// rules. Defaults to <c>false</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// While <c>false</c>, every one of those is inert: active-tenant validation is
    /// exactly the exact-subject-id admin check it has always been (a member entry or
    /// a group entry never admits anyone), the compiled tenant-policy snapshot builds
    /// no member or group index, the authorization engine never enters the tenant
    /// rule layer, and asserted tenant-group claims are not filtered. Turning the
    /// flag off deletes nothing; existing member entries, groups and rules are
    /// retained and become effective again when it is turned back on.
    /// </para>
    /// <para>
    /// A change to this value, observed through the options monitor, invalidates
    /// the silo's compiled tenant-policy snapshot and schedules a rebuild, so the
    /// new posture applies without a restart. Until the rebuild lands, decisions
    /// that consume an asserted active tenant are confirmed against the tenant
    /// registry under the new value.
    /// </para>
    /// </remarks>
    public bool DelegatedAccessAdministrationEnabled { get; set; }
}
