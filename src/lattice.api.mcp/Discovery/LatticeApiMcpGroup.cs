namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The transport-agnostic API facade groups the MCP surface can expose as
/// tools, plus the scopeless telemetry group. Each group maps to a cluster
/// capability and is
/// advertised to a caller only when the caller's effective permissions grant an
/// operation the group covers, so an agent discovers exactly the groups it may
/// use rather than being offered a tool it would then be denied.
/// </summary>
public enum LatticeApiMcpGroup
{
    /// <summary>
    /// The read-only state facade (<c>ILatticeStateQuery</c>): tree, shard, and
    /// cluster introspection. Advertised when the caller holds a read grant.
    /// </summary>
    State,

    /// <summary>
    /// The read/write data facade (<c>ILatticeDataApi</c>): entry reads and
    /// opt-in mutations. Advertised when the caller holds a read or write grant.
    /// </summary>
    Data,

    /// <summary>
    /// The backup control facade (<c>ILatticeBackupControl</c>): capture and
    /// restore. Advertised when the caller holds a backup or restore grant.
    /// </summary>
    Backup,

    /// <summary>
    /// The auth-admin control-plane facade (<c>ILatticeAuthAdmin</c>): membership
    /// and policy administration. Advertised when the caller holds an
    /// administrator grant.
    /// </summary>
    Auth,

    /// <summary>
    /// The cluster-wide telemetry group: cluster-level operational telemetry. It is
    /// scopeless (not attached to any tree, prefix, or key) and is advertised only
    /// when the caller holds a <see cref="LatticeOperation.Telemetry"/> grant, which
    /// no other operation - not even administrator - confers. It is served either by
    /// a routable per-region telemetry facade, like every sibling group, or by the
    /// companion telemetry package's tool module co-hosted with this server.
    /// </summary>
    Telemetry,

    /// <summary>
    /// The replication control facade (<c>ILatticeReplicationControl</c>):
    /// runtime per-tree cross-cluster replication configuration - inspect the
    /// replicated-tree set and, when opted in, enable or disable replication for
    /// a tree. Advertised only when the caller holds a
    /// <see cref="LatticeOperation.Replication"/> grant, which no other operation
    /// - not even administrator - confers.
    /// </summary>
    Replication,

    /// <summary>
    /// The tree-administration control-plane facade (<c>ILatticeTreeAdmin</c>):
    /// whole-tree lifecycle administration, presented by composing the existing
    /// single-responsibility facades (it wraps <c>ILatticeSchemaControl</c>).
    /// Advertised when the caller holds an administrator grant
    /// (<c>Admin</c>) or the distinct destructive / structural
    /// <c>TreeLifecycle</c> grant. This foundation
    /// group is discoverable but ships no operations yet; the whole-tree lifecycle
    /// tools land in later work.
    /// </summary>
    TreeAdmin,

    /// <summary>
    /// The repository-context group supplied by the companion
    /// <c>Orleans.Lattice.Api.Mcp.RepoContext</c> package: a durable,
    /// conflict-free store of structural facts, notes, and working memory about a
    /// codebase, served as MCP tools over dedicated Lattice trees. It is a
    /// data-plane surface, so it is advertised when the caller holds a data
    /// read-or-write grant (the same mask that makes the <see cref="Data"/> group
    /// usable); its mutating tools are contributed only when the host opts writes
    /// in. This foundation group is discoverable but ships only a health probe
    /// tool yet; the capture, maintenance, and retrieval tools land in later work.
    /// </summary>
    RepoContext,

    /// <summary>
    /// The tenant-administration control-plane facade
    /// (<c>ILatticeTenantAdmin</c>): the tenant lifecycle - create, suspend,
    /// resume, and delete (delete cascading the tenant's trees). Advertised only
    /// when the caller holds an administrator grant (<c>Admin</c>). The tenant
    /// lifecycle is all-mutating, so the group contributes tools only when the
    /// host opts tenant-admin control in; a cluster that does not opt in exposes
    /// no tenant-admin capability and no tenant-admin tools at all (fail-closed).
    /// </summary>
    TenantAdmin,
}
