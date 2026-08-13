namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The fixed mapping from each <see cref="LatticeApiMcpGroup"/> to the
/// data-plane <see cref="LatticeOperation"/> capabilities that make the group
/// usable, plus its human-readable name. A caller may use a group when their
/// effective permissions grant at least one operation in the group's mask; this
/// is the coarse, discovery-time projection of the authorization policy - the
/// per-tree / per-key verdict at call time remains authoritative and is enforced
/// by the same access gate the data path uses.
/// </summary>
internal static class LatticeApiMcpGroupCapabilityMap
{
    /// <summary>
    /// The facade groups in <see cref="LatticeApiMcpGroup"/> declaration
    /// order. Used to project a stable, deterministic capability list.
    /// </summary>
    public static readonly IReadOnlyList<LatticeApiMcpGroup> AllGroups = new[]
    {
        LatticeApiMcpGroup.State,
        LatticeApiMcpGroup.Data,
        LatticeApiMcpGroup.Backup,
        LatticeApiMcpGroup.Auth,
        LatticeApiMcpGroup.Telemetry,
        LatticeApiMcpGroup.Replication,
        LatticeApiMcpGroup.TreeAdmin,
        LatticeApiMcpGroup.RepoContext,
    };

    /// <summary>
    /// Returns the operation mask that makes <paramref name="group"/> usable: a
    /// caller holding an <c>Allow</c> grant for any operation in the mask may use
    /// the group.
    /// </summary>
    public static LatticeOperation RequiredOperations(LatticeApiMcpGroup group) => group switch
    {
        // Read-only state introspection.
        LatticeApiMcpGroup.State => LatticeOperation.Read | LatticeOperation.RangeRead,

        // Reads plus the full mutation surface the data facade can expose.
        LatticeApiMcpGroup.Data => LatticeOperation.Read
            | LatticeOperation.Write
            | LatticeOperation.Delete
            | LatticeOperation.RangeRead
            | LatticeOperation.RangeDelete
            | LatticeOperation.CrdtApply
            | LatticeOperation.AtomicWrite
            | LatticeOperation.BulkLoad,

        // Capture / restore control.
        LatticeApiMcpGroup.Backup => LatticeOperation.Backup | LatticeOperation.Restore,

        // Administrator control plane.
        LatticeApiMcpGroup.Auth => LatticeOperation.Admin,

        // Cluster-wide, scopeless operational telemetry.
        LatticeApiMcpGroup.Telemetry => LatticeOperation.Telemetry,

        // Runtime per-tree replication configuration.
        LatticeApiMcpGroup.Replication => LatticeOperation.Replication,

        // Whole-tree administration control plane: routine administration (Admin)
        // or the distinct irreversible / structural lifecycle capability
        // (TreeLifecycle). A caller holding either may use the group; the per-verb
        // gate at call time still separates routine from destructive operations.
        LatticeApiMcpGroup.TreeAdmin => LatticeOperation.Admin | LatticeOperation.TreeLifecycle,

        // Repository-context store: a data-plane surface over dedicated Lattice
        // trees, so it shares the data group's read/write capability mask.
        LatticeApiMcpGroup.RepoContext => LatticeOperation.Read
            | LatticeOperation.Write
            | LatticeOperation.Delete
            | LatticeOperation.RangeRead
            | LatticeOperation.RangeDelete
            | LatticeOperation.CrdtApply
            | LatticeOperation.AtomicWrite
            | LatticeOperation.BulkLoad,

        _ => LatticeOperation.None,
    };

    /// <summary>Returns the stable lower-case display name for <paramref name="group"/>.</summary>
    public static string DisplayName(LatticeApiMcpGroup group) => group switch
    {
        LatticeApiMcpGroup.State => "state",
        LatticeApiMcpGroup.Data => "data",
        LatticeApiMcpGroup.Backup => "backup",
        LatticeApiMcpGroup.Auth => "auth",
        LatticeApiMcpGroup.Telemetry => "telemetry",
        LatticeApiMcpGroup.Replication => "replication",
        LatticeApiMcpGroup.TreeAdmin => "treeadmin",
        LatticeApiMcpGroup.RepoContext => "repocontext",
        _ => group.ToString().ToLowerInvariant(),
    };
}
