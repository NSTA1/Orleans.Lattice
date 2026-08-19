using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication;

/// <summary>
/// Fails startup fast when the resolved repository-context replication topology is
/// inconsistent with the hub-and-spoke invariant, rather than letting two clusters
/// silently race to mutate the same source-derived index state or lose concurrent
/// memory writes. Registered by
/// <see cref="RepoContextReplicationServiceCollectionExtensions.EnableRepoContextMultiCluster(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>
/// as an <see cref="IValidateOptions{TOptions}"/> over
/// <see cref="LatticeReplicationOptions"/>, so the check runs at first resolve of the
/// replication options for every named (per-tree) instance.
/// <para>
/// The guard only engages once at least one repository-context tree is enrolled (so a
/// host that replicates unrelated trees is unaffected). It then asserts, for the
/// enrolled repository-context trees:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <see cref="RepoContextTrees.Memory"/> converges multi-master
///     (<see cref="LatticeMergeMode.MvRegister"/>). A last-writer-wins memory tree
///     drops one of two concurrent cross-cluster writes.
///   </description></item>
///   <item><description>
///     <see cref="RepoContextTrees.VectorMembership"/> converges add-wins
///     (<see cref="LatticeMergeMode.OrFlag"/>). A last-writer-wins membership tree
///     drops an embedding present on one cluster and pruned on another.
///   </description></item>
///   <item><description>
///     Every single-writer index-plane tree (structural, symbol, content,
///     cross-reference, and the vector payload and metadata projections) stays
///     last-writer-wins (<see cref="LatticeMergeMode.LwwRegister"/>). Enrolling one
///     under a CRDT merge mode implies more than one concurrent indexer - active-active
///     indexing - which the single-indexer hub-and-spoke topology forbids: only the
///     hub walks, reconciles, prunes, and re-embeds.
///   </description></item>
/// </list>
/// </summary>
internal sealed class RepoContextTopologyOptionsValidator : IValidateOptions<LatticeReplicationOptions>
{
    /// <summary>
    /// The single-writer index-plane trees that must stay last-writer-wins under
    /// hub-and-spoke; a CRDT merge mode on any of these implies active-active
    /// indexing.
    /// </summary>
    private static readonly string[] IndexPlaneTrees =
    [
        RepoContextTrees.Structural,
        RepoContextTrees.Symbol,
        RepoContextTrees.Content,
        RepoContextTrees.CrossReference,
        RepoContextTrees.VectorPayload,
        RepoContextTrees.VectorMetadata,
    ];

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeReplicationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var trees = options.ReplicatedTrees;
        if (trees is null || !EnrolsRepoContext(trees))
        {
            // No repository-context tree is enrolled here: this validator has nothing
            // to say about an unrelated replication configuration.
            return ValidateOptionsResult.Success;
        }

        var failures = new List<string>();

        RequireMode(trees, RepoContextTrees.Memory, RepoContextReplicatedTrees.MemoryMode,
            "the agent-memory tree must converge multi-master so two clusters' concurrent memory writes " +
            "both survive and fold; a last-writer-wins memory tree silently drops one whole record",
            failures);

        RequireMode(trees, RepoContextTrees.VectorMembership, RepoContextReplicatedTrees.MembershipMode,
            "the vector-membership presence tree must converge add-wins so an embedding present on one " +
            "cluster and pruned on another is not lost delete-wins",
            failures);

        foreach (var tree in IndexPlaneTrees)
        {
            RequireMode(trees, tree, RepoContextReplicatedTrees.DefaultMode,
                "it is a single-writer index-plane tree under hub-and-spoke; enrolling it under a CRDT merge " +
                "mode implies active-active indexing (more than one cluster mutating source-derived index " +
                "state), which the single-indexer topology forbids - only the hub indexes",
                failures);
        }

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }

    private static bool EnrolsRepoContext(IReadOnlyDictionary<string, LatticeMergeMode> trees)
    {
        foreach (var id in RepoContextTrees.All)
        {
            if (trees.ContainsKey(id))
            {
                return true;
            }
        }

        return false;
    }

    private static void RequireMode(
        IReadOnlyDictionary<string, LatticeMergeMode> trees,
        string treeId,
        LatticeMergeMode expected,
        string because,
        List<string> failures)
    {
        if (trees.TryGetValue(treeId, out var actual) && actual != expected)
        {
            failures.Add(
                $"Repository-context tree '{treeId}' is enrolled under {actual} but must be {expected}: {because}.");
        }
    }
}
