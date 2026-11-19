using System.Collections.Concurrent;
using System.Globalization;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Wires a substitute <see cref="IGrainFactory"/> so every
/// <see cref="IShardRootGrain"/> it resolves is a per-key substitute whose
/// <see cref="IShardRootGrain.GetShardProjectionDigestAsync"/> delegates to an
/// <see cref="IShardDigestStub"/> by the shard index parsed from the
/// <c>{physicalTreeId}/{shardIndex}</c> key.
/// </summary>
internal static class ShardRootDigestStubs
{
    /// <summary>
    /// Installs the per-key shard-root substitutes and returns the map of every
    /// shard root resolved so far, keyed by grain key.
    /// </summary>
    /// <param name="grainFactory">The substitute grain factory to wire.</param>
    /// <param name="physicalTreeId">The physical tree id every resolved shard-root key must be prefixed with; a key outside it fails the test.</param>
    /// <param name="digests">The per-shard digest source the roots delegate to.</param>
    /// <param name="configureRoot">Optional extra setup applied to each new root (for example its root node ref).</param>
    public static ConcurrentDictionary<string, IShardRootGrain> Wire(
        IGrainFactory grainFactory,
        string physicalTreeId,
        IShardDigestStub digests,
        Action<IShardRootGrain>? configureRoot = null)
    {
        var roots = new ConcurrentDictionary<string, IShardRootGrain>(StringComparer.Ordinal);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>())
            .Returns(call => roots.GetOrAdd(call.ArgAt<string>(0), key =>
            {
                var prefix = physicalTreeId + "/";
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Shard root '{key}' is outside physical tree '{physicalTreeId}'.");
                }

                var shardIndex = int.Parse(key[prefix.Length..], CultureInfo.InvariantCulture);
                var root = Substitute.For<IShardRootGrain>();
                root.GetRootNodeRefAsync().Returns(Task.FromResult<ShardRootNodeRef?>(null));
                root.GetShardProjectionDigestAsync(Arg.Any<CancellationToken>())
                    .Returns(c => digests.GetLeafProjectionDigestAsync(shardIndex, c.Arg<CancellationToken>()));
                configureRoot?.Invoke(root);
                return root;
            }));
        return roots;
    }
}
