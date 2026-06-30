using System.Text;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless routing helper that maps a leaf-materialiser consumer id to one of
/// <see cref="LatticeOptions.WalMaterialiserPinShards"/> durable
/// <see cref="IWalMaterialiserPinGrain"/> activations, spreading the per-tree
/// pin-store write fan-in (previously a single hot grain) across a deterministic
/// set of shard grains. The grain key is <c>{treeName}#s{shard}</c>; when the
/// shard count is <c>1</c> the legacy unsuffixed <c>{treeName}</c> key is used so
/// a host that pins the shard count to one stays byte-for-byte wire compatible
/// with the pre-sharding layout.
/// <para>
/// Routing is by a stable, process-independent hash of the full consumer id, so
/// a pin written in one process is read back from the same shard after a restart.
/// The WAL GC fan-in (<see cref="EnumerateReadKeys"/>) reads every shard
/// <em>and</em> the legacy key so durable pins written before the upgrade (or by
/// a host that later raised the shard count) are never silently dropped from the
/// trim floor.
/// </para>
/// </summary>
internal static class WalMaterialiserPinRouting
{
    /// <summary>
    /// Resolves the configured shard count from the global (unkeyed) options,
    /// clamped to at least one. The shard count is a cluster-wide structural
    /// constant read from <see cref="IOptionsMonitor{TOptions}.Get"/> with the
    /// empty key, matching how the other WAL fan-out knobs are read.
    /// </summary>
    public static int ResolveShardCount(IOptionsMonitor<LatticeOptions>? options)
    {
        if (options is null)
        {
            return 1;
        }

        return Math.Max(1, options.Get(string.Empty).WalMaterialiserPinShards);
    }

    /// <summary>
    /// Returns the durable pin grain key for <paramref name="consumerId"/> under
    /// <paramref name="treeName"/> given <paramref name="shardCount"/>. A shard
    /// count of one returns the legacy <paramref name="treeName"/> key.
    /// </summary>
    public static string ShardKey(string treeName, string consumerId, int shardCount)
    {
        if (shardCount <= 1)
        {
            return treeName;
        }

        var shard = (int)(StableHash(consumerId) % (uint)shardCount);
        return string.Concat(treeName, "#s", shard.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Enumerates every grain key the WAL GC must read to reconstruct the full
    /// durable pin floor for <paramref name="treeName"/>: each shard key plus the
    /// legacy unsuffixed key (a dual-read migration so pre-upgrade pins still
    /// count). When <paramref name="shardCount"/> is one this yields only the
    /// legacy key.
    /// </summary>
    public static IReadOnlyList<string> EnumerateReadKeys(string treeName, int shardCount)
    {
        if (shardCount <= 1)
        {
            return new[] { treeName };
        }

        var keys = new string[shardCount + 1];
        for (var shard = 0; shard < shardCount; shard++)
        {
            keys[shard] = string.Concat(treeName, "#s", shard.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        // Legacy key last so a pre-upgrade pin participates in the union.
        keys[shardCount] = treeName;
        return keys;
    }

    /// <summary>
    /// FNV-1a 32-bit hash over the UTF-8 bytes of <paramref name="value"/>.
    /// Deterministic across processes and platforms (unlike
    /// <see cref="string.GetHashCode()"/>, which is randomised per process), so a
    /// consumer always routes to the same shard across restarts.
    /// </summary>
    private static uint StableHash(string value)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;

        var hash = offsetBasis;
        var byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> buffer = byteCount <= 256 ? stackalloc byte[byteCount] : new byte[byteCount];
        Encoding.UTF8.GetBytes(value, buffer);
        for (var i = 0; i < buffer.Length; i++)
        {
            hash ^= buffer[i];
            hash *= prime;
        }

        return hash;
    }
}
