using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Incoming grain-call filter that feeds <see cref="LatticeTreeCallCounter"/>.
/// Counts calls whose interface method is declared on <see cref="ILattice"/>, so
/// the shard and leaf grains a single facade call fans out to are never
/// double-counted, and restricts them to the counter's tree when it names one.
/// A tree-scoped counter that sets
/// <see cref="LatticeTreeCallCounter.IncludeShardGrains"/> also counts calls that
/// land on that tree's shard grains, which is the only way to observe a page read:
/// a range scan executes entirely on the shards and the facade never sees one.
/// </summary>
/// <param name="counter">The tally the filter records into.</param>
internal sealed class LatticeTreeCallCountingFilter(LatticeTreeCallCounter counter) : IIncomingGrainCallFilter
{
    /// <inheritdoc />
    public Task Invoke(IIncomingGrainCallContext context)
    {
        var method = context.InterfaceMethod;
        if (method is null)
        {
            return context.Invoke();
        }

        // The shard grain's interface is internal to Orleans.Lattice, so a shard
        // call is identified the way LatticeTreeFaultInjector identifies one - by
        // its grain key - rather than by its declaring type. Both sides of a
        // perturbation must select the same population.
        var treeId = context.TargetContext.GrainId.Key.ToString() ?? string.Empty;
        var shardCall = counter.IncludeShardGrains
            && counter.TreeId is not null
            && treeId.StartsWith(counter.TreeId + "/", StringComparison.Ordinal);

        if (shardCall
            || (method.DeclaringType == typeof(ILattice)
                && (counter.TreeId is null
                    || string.Equals(treeId, counter.TreeId, StringComparison.Ordinal))))
        {
            counter.Record(method.Name, treeId, KeysIn(context));
        }

        return context.Invoke();
    }

    /// <summary>
    /// Infers how many keys a call addresses from its first argument, so a batched
    /// call is charged its batch size rather than one. A point call whose first
    /// argument is the key, and any shape not recognised, is charged one - the
    /// conservative direction for an assertion that a path got <b>cheaper</b>.
    /// </summary>
    private static int KeysIn(IIncomingGrainCallContext context)
    {
        var arguments = context.Request?.GetArgumentCount() > 0 ? context.Request : null;
        if (arguments is null)
        {
            return 1;
        }

        var first = arguments.GetArgument(0);
        return first switch
        {
            string => 1,
            System.Collections.ICollection collection => collection.Count,
            _ => 1,
        };
    }
}
