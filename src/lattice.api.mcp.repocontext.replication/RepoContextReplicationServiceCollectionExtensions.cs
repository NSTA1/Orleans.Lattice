using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication;

/// <summary>
/// Extension methods that turn on cross-cluster replication for the
/// <c>Orleans.Lattice.Api.Mcp.RepoContext</c> trees.
/// </summary>
public static class RepoContextReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Enables multi-cluster replication for the repository-context store: registers
    /// <c>Orleans.Lattice.Replication</c> with the caller's replication settings and
    /// enrols every repository-context tree into
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> under the correct
    /// per-tree <see cref="LatticeMergeMode"/>, so an operator turns cross-cluster
    /// repo-context on with one call and cannot misconfigure the convergence rules.
    /// <para>
    /// The caller configures its real replication settings (cluster id, transport,
    /// peers, secrets, per-tree options) through <paramref name="configure"/> exactly
    /// as it would for
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>,
    /// which this method calls. The reserved repository-context tree-mode map (see
    /// <see cref="RepoContextReplicatedTrees"/>) is then merged in afterwards through a
    /// <c>PostConfigureAll</c>, so the correct modes win regardless of the order in
    /// which the host configures its own replicated-trees map.
    /// </para>
    /// <para>
    /// <b>Membership is pinned.</b> The vector-membership presence tree
    /// (<see cref="RepoContextTrees.VectorMembership"/>) is <b>force-enrolled</b> under
    /// the add-wins <see cref="LatticeMergeMode.OrFlag"/> even if the host declared it
    /// under a different mode: it is authored as an <see cref="Orleans.Lattice.OrFlag"/>
    /// and must converge add-wins by CRDT merge, because a source embedded on one
    /// cluster and pruned on another would otherwise resolve delete-wins and silently
    /// lose the embedding. Every other repository-context tree defaults to
    /// <see cref="LatticeMergeMode.LwwRegister"/> - the mode matching its whole-value
    /// authoring - but a deliberate per-tree host override is respected.
    /// </para>
    /// <para>
    /// The cross-cluster embedding-gap scanner stays local to each cluster; this helper
    /// only governs which trees ship and how they converge, so the two common
    /// topologies both work: a single-indexer deployment that computes the expensive
    /// embedding index once and replicates it, and a fully active-active deployment
    /// where every cluster embeds and the membership CRDT reconciles presence.
    /// </para>
    /// <para>
    /// Requires <c>Orleans.Lattice</c> to be registered first (via
    /// <c>AddLattice(...)</c>), like any other replication add-on.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder. Must not be <see langword="null"/>.</param>
    /// <param name="configure">
    /// Delegate that populates the caller's <see cref="LatticeReplicationOptions"/>
    /// (cluster id, transport, peers, and any per-tree settings). Must not be
    /// <see langword="null"/>. The repository-context tree-mode map is merged in on top
    /// of whatever this delegate sets.
    /// </param>
    /// <param name="enableRuntimeConfig">
    /// Forwarded to
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>.
    /// When <see langword="true"/>, also enrols the reserved runtime
    /// replication-configuration tree and installs the dynamic replication-config
    /// control plane. Defaults to <see langword="false"/>, leaving replication
    /// configured purely from the static map.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="System.ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    public static ISiloBuilder EnableRepoContextMultiCluster(
        this ISiloBuilder builder,
        Action<LatticeReplicationOptions> configure,
        bool enableRuntimeConfig = false)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        // Register the replication engine with the caller's own settings. This is
        // what makes the receiver apply seam and the merge-mode resolver run, so we
        // own the ordering by calling it ourselves rather than requiring the host to.
        builder.AddLatticeReplication(configure, enableRuntimeConfig);

        var reserved = RepoContextReplicatedTrees.BuildEnrolmentMap();

        // PostConfigureAll so the reserved entries are merged after every
        // Configure/ConfigureAll action on LatticeReplicationOptions has run,
        // including a host that sets ReplicatedTrees after this call. Applied to
        // every named (per-tree) options instance because the commit-time observer
        // and the merge-mode resolver read options via Get(treeId).
        builder.Services.PostConfigureAll<LatticeReplicationOptions>(options =>
        {
            var merged = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
            if (options.ReplicatedTrees is not null)
            {
                foreach (var kv in options.ReplicatedTrees)
                {
                    merged[kv.Key] = kv.Value;
                }
            }

            foreach (var kv in reserved)
            {
                if (kv.Key == RepoContextTrees.VectorMembership || kv.Key == RepoContextTrees.Memory)
                {
                    // Pinned: force the add-wins membership mode and the multi-value
                    // memory mode over any host declaration. A LwwRegister membership
                    // tree silently loses an embedding embedded on one cluster and
                    // pruned on another; a LwwRegister memory tree silently loses one
                    // of two concurrent cross-cluster writes to the same key. Both are
                    // the exact misconfigurations these guardrails prevent.
                    merged[kv.Key] = kv.Value;
                }
                else
                {
                    // Default the tree to its authoring-consistent mode, but respect a
                    // deliberate host override already present in the map.
                    merged.TryAdd(kv.Key, kv.Value);
                }
            }

            options.ReplicatedTrees = merged;
        });

        // Author every agent-memory CRDT write under this cluster's replication id, so
        // two clusters' concurrent writes to the same memory key mint distinct dots and
        // both survive the merge. Appended after the base local-identity registration,
        // so it wins for the single resolve regardless of the two calls' order.
        builder.Services.AddSingleton<IRepoContextReplicaIdentity, ClusterRepoContextReplicaIdentity>();

        // Fail startup fast when the resolved enrolment is inconsistent with the
        // hub-and-spoke topology (a memory tree that is not multi-master, a membership
        // tree that is not add-wins, or an index-plane tree enrolled under a CRDT mode
        // that implies active-active indexing), rather than silently racing at runtime.
        builder.Services.AddSingleton<IValidateOptions<LatticeReplicationOptions>, RepoContextTopologyOptionsValidator>();

        return builder;
    }
}
