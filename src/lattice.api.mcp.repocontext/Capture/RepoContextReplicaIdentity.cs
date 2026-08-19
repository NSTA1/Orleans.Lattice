namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Supplies the stable replica identity every agent-memory write is authored under
/// in the memory <see cref="Orleans.Lattice.Primitives.MvRegister"/>. The identity
/// is the CRDT dot's replica id: each cluster must present a distinct, stable value
/// so two clusters' concurrent writes mint distinct dots and both survive the merge,
/// while a single cluster's repeated writes reuse one id and advance one dot lineage.
/// <para>
/// The base store resolves a stable local id (single-cluster deployments have one
/// writer per key, so a constant is safe). The replication companion overrides this
/// with the replication cluster id, so cross-cluster concurrent writes are
/// dot-distinct and converge.
/// </para>
/// </summary>
internal interface IRepoContextReplicaIdentity
{
    /// <summary>
    /// The stable, non-empty replica id authored onto every memory CRDT write.
    /// </summary>
    string ReplicaId { get; }
}

/// <summary>
/// The default <see cref="IRepoContextReplicaIdentity"/> for a single-cluster
/// deployment: a stable constant. A single cluster has exactly one activation per
/// memory key (Orleans single-activation), so all of its writes serialize through
/// one grain turn and a constant replica id advances one dot lineage without loss.
/// The replication companion replaces this with the cluster id.
/// </summary>
internal sealed class LocalRepoContextReplicaIdentity : IRepoContextReplicaIdentity
{
    /// <summary>The stable local replica id used when replication is not enabled.</summary>
    internal const string LocalReplicaId = "local";

    /// <inheritdoc />
    public string ReplicaId => LocalReplicaId;
}
