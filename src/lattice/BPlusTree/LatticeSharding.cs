using System.Buffers;
using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Public utility for computing which shard a key belongs to.
/// Uses the same XxHash32-based algorithm used internally by <see cref="ILattice"/>
/// grain routing, so callers can pre-partition data for bulk operations.
/// </summary>
public static class LatticeSharding
{
    /// <summary>
    /// Computes a stable shard index for the given key using XxHash32.
    /// </summary>
    public static int GetShardIndex(string key, int shardCount)
    {
        var maxByteCount = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = Encoding.UTF8.GetBytes(key, buffer);
            var hash = XxHash32.HashToUInt32(buffer[..written]);
            return (int)(hash % (uint)shardCount);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
