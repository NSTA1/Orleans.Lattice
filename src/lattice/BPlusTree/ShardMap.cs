using System.Buffers;
using System.ComponentModel;
using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice;

/// <summary>
/// Persistent indirection layer mapping a fixed virtual shard space onto
/// physical <c>ShardRootGrain</c> indices. Keys are first hashed into a
/// virtual slot in <c>[0, VirtualShardCount)</c>, then routed to the
/// physical shard recorded for that slot in <see cref="Slots"/>.
/// <para>
/// The map decouples logical key routing from the physical shard count,
/// enabling adaptive shard splits to retarget ranges of virtual slots to
/// new physical shards without rehashing existing keys.
/// </para>
/// <para>
/// The default identity map produced by <see cref="CreateDefault"/> assigns
/// <c>Slots[i] = i % physicalShardCount</c>. When the virtual shard count is
/// an integer multiple of the physical shard count, this preserves the legacy
/// <c>XxHash32(key) % physicalShardCount</c> routing exactly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardMap)]
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class ShardMap
{
    /// <summary>
    /// Backing array indexed by virtual slot; each value is a physical shard index.
    /// Length equals <see cref="VirtualShardCount"/>.
    /// </summary>
    [Id(0)]
    public int[] Slots { get; set; } = [];

    /// <summary>
    /// Lazily-computed cache of the sorted, distinct physical shard indices
    /// referenced by <see cref="Slots"/>. Recomputed on first access in any
    /// new activation; <c>[NonSerialized]</c> excludes it from the wire format.
    /// </summary>
    [NonSerialized]
    private int[]? _physicalShards;

    /// <summary>Number of virtual slots in this map.</summary>
    public int VirtualShardCount => Slots.Length;

    /// <summary>
    /// Computes the virtual slot for <paramref name="key"/> by hashing it into
    /// <c>[0, virtualShardCount)</c> using the same XxHash32 function used for
    /// legacy shard routing.
    /// </summary>
    public static int GetVirtualSlot(string key, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        var maxByteCount = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = Encoding.UTF8.GetBytes(key, buffer);
            var hash = XxHash32.HashToUInt32(buffer[..written]);
            return (int)(hash % (uint)virtualShardCount);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Resolves the physical shard index that owns <paramref name="key"/>.
    /// </summary>
    public int Resolve(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (Slots.Length == 0)
            throw new InvalidOperationException("Shard map is empty.");

        var slot = GetVirtualSlot(key, Slots.Length);
        return Slots[slot];
    }

    /// <summary>
    /// Returns the sorted, distinct set of physical shard indices referenced
    /// by this map. Used by enumerations that fan out across all shards.
    /// The result is memoized on first call.
    /// </summary>
    public IReadOnlyList<int> GetPhysicalShardIndices()
    {
        var cached = _physicalShards;
        if (cached is not null) return cached;

        var set = new HashSet<int>(Slots.Length);
        foreach (var idx in Slots)
            set.Add(idx);
        var result = new int[set.Count];
        var i = 0;
        foreach (var idx in set)
            result[i++] = idx;
        Array.Sort(result);

        _physicalShards = result;
        return result;
    }

    /// <summary>
    /// Creates an identity shard map of length <paramref name="virtualShardCount"/>
    /// where <c>Slots[i] = i % physicalShardCount</c>. When
    /// <paramref name="virtualShardCount"/> is an integer multiple of
    /// <paramref name="physicalShardCount"/>, this routing is bit-for-bit
    /// equivalent to the legacy <c>hash % physicalShardCount</c> formula.
    /// </summary>
    public static ShardMap CreateDefault(int virtualShardCount, int physicalShardCount)
    {
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");
        if (physicalShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(physicalShardCount), "Must be greater than 0.");
        if (virtualShardCount < physicalShardCount)
            throw new ArgumentException(
                $"{nameof(virtualShardCount)} must be greater than or equal to {nameof(physicalShardCount)}.",
                nameof(virtualShardCount));

        var slots = new int[virtualShardCount];
        for (int i = 0; i < virtualShardCount; i++)
            slots[i] = i % physicalShardCount;
        return new ShardMap { Slots = slots };
    }
}
