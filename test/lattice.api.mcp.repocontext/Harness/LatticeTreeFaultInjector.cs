using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Fails selected <see cref="ILattice"/> calls so a test can reproduce the
/// vector-plane fault that actually happens in production - a write to the
/// membership tree timing out while the tree is under load - at the real seam,
/// rather than by substituting a collaborator the runtime never uses.
/// <para>
/// The writer the ingestor depends on is a concrete sealed type, so the honest
/// injection point is the grain call itself. Registered as an
/// <see cref="IIncomingGrainCallFilter"/> through
/// <see cref="RepoContextMcpHarnessOptions.ConfigureSilo"/>, paired with
/// <see cref="LatticeTreeFaultInjectingFilter"/>.
/// </para>
/// </summary>
public sealed class LatticeTreeFaultInjector
{
    private int matched;
    private int failed;

    /// <summary>The tree id to fault, or <see langword="null"/> for every tree.</summary>
    public string? TreeId { get; init; }

    /// <summary>
    /// The <see cref="ILattice"/> method name to fault, for example
    /// <c>ApplyCrdtDeltaManyAsync</c>. Required; nothing is faulted without it.
    /// </summary>
    public string? Method { get; init; }

    /// <summary>
    /// How many matching calls to fail, counted from the first. Set to
    /// <see cref="int.MaxValue"/> to fail every one.
    /// </summary>
    public int FailFirst { get; set; }

    /// <summary>How many matching calls have been seen.</summary>
    public int Matched => Volatile.Read(ref matched);

    /// <summary>How many matching calls were actually failed.</summary>
    public int Failed => Volatile.Read(ref failed);

    internal bool ShouldFail(string method, string? treeId)
    {
        if (!string.Equals(method, Method, StringComparison.Ordinal))
        {
            return false;
        }

        // A tree's work is split across a facade grain keyed by the tree id and
        // shard/leaf grains keyed by "{treeId}/{index}", so a fault aimed at a tree
        // has to match both shapes to reach a call that only ever lands on a shard.
        if (TreeId is not null
            && !string.Equals(treeId, TreeId, StringComparison.Ordinal)
            && !(treeId is not null && treeId.StartsWith(TreeId + "/", StringComparison.Ordinal)))
        {
            return false;
        }

        Interlocked.Increment(ref matched);
        if (Volatile.Read(ref failed) >= FailFirst)
        {
            return false;
        }

        Interlocked.Increment(ref failed);
        return true;
    }
}
