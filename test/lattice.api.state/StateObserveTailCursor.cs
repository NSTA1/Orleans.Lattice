using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Captures a tree's current per-partition write-ahead-log tail and encodes it
/// as a change-observation continuation token, so a test can pin the exact
/// window a subscription observes instead of racing the subscription's own
/// fresh-tail seeding.
/// <para>
/// A fresh subscription (no continuation token) seeds its cursor from the live
/// WAL tail inside the first <c>MoveNextAsync</c>, which resolves the tree,
/// authorizes the caller, and round-trips every WAL partition grain. A test
/// that starts collecting and then mutates is therefore racing that setup: if
/// the mutation lands first, the tail is seeded past it and the change is never
/// delivered. Sleeping before the mutation only narrows the window - it does not
/// close it, and it reopens under any slowdown (the coverage job's
/// instrumentation being the reliable one).
/// </para>
/// <para>
/// Pinning the token removes the race outright rather than making it rarer:
/// the observed window is fixed by an explicit position captured <em>before</em>
/// the mutation, so the subscription delivers exactly the changes committed
/// after that point no matter how long it takes to establish.
/// </para>
/// </summary>
internal static class StateObserveTailCursor
{
    /// <summary>
    /// The continuation-token version prefix, matching the encoding
    /// <c>LatticeStateObserver</c> emits and parses.
    /// </summary>
    private const string TokenVersion = "1";

    /// <summary>
    /// Reads the next unused sequence of every WAL partition backing
    /// <paramref name="treeId"/> and encodes them as a continuation token
    /// positioned at the current tail.
    /// </summary>
    public static async Task<string> CaptureAsync(
        IClusterClient client,
        IServiceProvider siloServices,
        string treeId)
    {
        var registry = client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(treeId) ?? treeId;
        var entry = await registry.GetEntryAsync(treeId);

        // Resolved exactly as LatticeStateObserver resolves it, so the token's
        // partition count always matches the topology the observer decodes
        // against (a mismatch is a hard cursor-expired fault, not a silent skew).
        var partitions = Math.Max(
            1,
            entry?.WalPartitions
                ?? siloServices.GetRequiredService<IOptionsMonitor<LatticeOptions>>().Get(treeId).WalPartitions);

        var payload = new StringBuilder(TokenVersion);
        for (var partition = 0; partition < partitions; partition++)
        {
            var wal = client.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");
            var nextSequence = await wal.GetNextSequenceAsync(CancellationToken.None);
            payload.Append('|').Append(nextSequence);
        }

        return Convert.ToBase64String(Encoding.ASCII.GetBytes(payload.ToString()));
    }
}
