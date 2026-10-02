namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The confinement rule a delegated tenant-access write violated, carried by
/// <see cref="TenantAccessConfinementException.Rule"/> so a transport binding or
/// a user interface can explain the refusal without parsing its message.
/// </summary>
public enum TenantAccessConfinementRule
{
    /// <summary>
    /// A tenant group would become a member of a cluster group or of another
    /// tenant's group. A tenant group may contain users, groups of the same
    /// tenant, and cluster groups, but may never be nested into a group the
    /// tenant does not own.
    /// </summary>
    GroupNesting = 0,

    /// <summary>
    /// An entry names a group of another tenant: a group member, a member-set or
    /// admin-set entry, or the subject of a tenant-tier rule. Only the tenant's own
    /// groups and cluster groups may be named.
    /// </summary>
    ForeignTenantGroup = 1,

    /// <summary>
    /// A tenant-tier rule targets a tree the tenant may not author rules for: one
    /// of its app-owned trees, a reserved or system tree, or a tree outside its
    /// namespace.
    /// </summary>
    RuleTree = 2,

    /// <summary>
    /// A tenant-tier rule covers an operation outside the data-plane mask, such as
    /// telemetry, replication, tree lifecycle, or app installation.
    /// </summary>
    RuleOperations = 3,

    /// <summary>
    /// A tenant-tier rule's local id carries a reserved rule-id prefix, which only
    /// the system may write.
    /// </summary>
    ReservedRuleId = 4,
}
