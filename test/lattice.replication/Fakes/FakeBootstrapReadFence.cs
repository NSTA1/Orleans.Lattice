using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Records the receiver bootstrap read fence operations the coordinator drives
/// (issue #4526), in order, and lets a test hold the drain or fail a lift.
/// </summary>
internal sealed class FakeBootstrapReadFence : IBootstrapReadFence
{
    public static readonly TreeBootstrapReadFence.Shards DefaultShards = new("physical-tree", [0, 1]);

    public TreeBootstrapReadFence.Shards Shards { get; set; } = DefaultShards;

    /// <summary>The blocker reported after arming, or <see langword="null"/>.</summary>
    public string? Blocker { get; set; }

    /// <summary>When set, a lift throws it.</summary>
    public Exception? LiftFault { get; set; }

    /// <summary>Whether the fence is currently armed on the fake shards.</summary>
    public bool Armed { get; private set; }

    /// <summary>The operations in order: <c>arm</c>, <c>lift</c>, <c>blocker</c>.</summary>
    public List<string> Calls { get; } = [];

    public Task<TreeBootstrapReadFence.Shards> ResolveAsync(string treeName) => Task.FromResult(Shards);

    public Task SetAsync(TreeBootstrapReadFence.Shards shards, bool fenced)
    {
        if (!fenced && LiftFault is { } fault)
        {
            return Task.FromException(fault);
        }

        Calls.Add(fenced ? "arm" : "lift");
        Armed = fenced;
        return Task.CompletedTask;
    }

    public Task<string?> FindBlockerAsync(string treeName, TreeBootstrapReadFence.Shards shards)
    {
        Calls.Add("blocker");
        return Task.FromResult(Blocker);
    }
}
