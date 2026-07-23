namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The transport-agnostic API facade groups the MCP surface can expose as
/// tools, plus the scopeless telemetry group supplied by the companion package.
/// Each group maps to a cluster capability and is
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
    /// The cluster-wide telemetry group: cluster-level operational telemetry
    /// exposed by the companion telemetry package. It is scopeless (not attached
    /// to any tree, prefix, or key) and is advertised only when the caller holds
    /// a <see cref="LatticeOperation.Telemetry"/> grant, which no other operation
    /// - not even administrator - confers.
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
}
