namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The indexing role a repository-context cluster plays in a multi-cluster
/// hub-and-spoke topology. It decides whether this cluster's per-repository
/// self-index grain is the authoritative writer of source-derived index state or
/// a read-only replica of it.
/// <para>
/// Exactly one cluster indexes a repository. A <see cref="Hub"/> walks the working
/// tree, reconciles and prunes the structural, symbol, content, cross-reference,
/// and vector projections, and drives the embedding gap scan - the full,
/// mutating index pass. A <see cref="Spoke"/> serves the replicated index records
/// for reads but never walks, reconciles, prunes, or re-embeds; its self-index
/// grain is inert so two clusters can never race to mutate the same
/// source-derived index state.
/// </para>
/// </summary>
internal enum RepoContextIndexingRole
{
    /// <summary>
    /// The authoritative indexer (the default, preserving single-cluster
    /// behaviour): the self-index grain arms its scan timer, drives the initial
    /// and periodic reconcile, and re-drives the embedding back-fill.
    /// </summary>
    Hub = 0,

    /// <summary>
    /// A read-only replica: the self-index grain activates and serves reads but
    /// never arms its scan timer, reconciles, prunes, or re-embeds. Source-derived
    /// index state arrives only by cross-cluster replication from the hub.
    /// </summary>
    Spoke = 1,
}
