using System.Buffers;
using System.Collections.Immutable;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// The keys a write-ahead-log reader owns: a half-open key range and,
/// optionally, the set of virtual shard slots routed to one physical shard. A
/// reader that discards every key-scoped record outside its ownership hands one
/// of these to <see cref="IWalStorageProvider.ReadFilteredAsync"/> so storage
/// can drop those records before it materialises their payloads (issue #3565).
/// <para>
/// <b>Ownership.</b> A key is owned when
/// <c><see cref="LowKeyInclusive"/> &lt;= key &lt; <see cref="HighKeyExclusive"/></c>
/// under ordinal comparison - a <see langword="null"/> bound means no
/// constraint on that side - and, when the filter carries a shard constraint,
/// the key's virtual slot (<see cref="ShardMap.GetVirtualSlot"/> over
/// <see cref="VirtualShardCount"/>) is a member of <see cref="OwnedSlots"/>.
/// This is exactly the leaf replay filter's ownership rule, so a record the
/// filter excludes is one the replaying leaf would reject anyway.
/// </para>
/// <para>
/// <b>Exclusion.</b> Only key-scoped records can be excluded:
/// <see cref="MutationKind.Set"/>, <see cref="MutationKind.Delete"/> and
/// <see cref="MutationKind.Tombstone"/> whose key the filter does not own. Range
/// deletes, saga terminals and kinds this binary does not know are never
/// excluded, because their ownership is not a property of a single key.
/// </para>
/// <para>
/// The <see langword="default"/> filter is unbounded - it owns every key - and
/// a filtered read with it is indistinguishable from an unfiltered one.
/// </para>
/// <para>
/// A filter whose shard constraint is malformed (a bitmap that does not cover
/// <see cref="VirtualShardCount"/> slots) does not constrain the shard axis at
/// all. Excluding a record is only ever an optimisation, so an unprovable
/// exclusion is not made.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalKeyFilter)]
[Immutable]
public readonly record struct WalKeyFilter
{
    /// <summary>Keys of at most this many UTF-8 bytes are decoded on the stack.</summary>
    private const int StackDecodeBytes = 256;

    /// <summary>
    /// Creates a filter owning the half-open key range
    /// <c>[<paramref name="lowKeyInclusive"/>, <paramref name="highKeyExclusive"/>)</c>
    /// with no shard constraint.
    /// </summary>
    /// <param name="lowKeyInclusive">Inclusive low bound, or <see langword="null"/> for no lower bound.</param>
    /// <param name="highKeyExclusive">Exclusive high bound, or <see langword="null"/> for no upper bound.</param>
    public WalKeyFilter(string? lowKeyInclusive, string? highKeyExclusive)
    {
        LowKeyInclusive = lowKeyInclusive;
        HighKeyExclusive = highKeyExclusive;
    }

    /// <summary>
    /// Creates a filter owning the half-open key range
    /// <c>[<paramref name="lowKeyInclusive"/>, <paramref name="highKeyExclusive"/>)</c>
    /// intersected with the virtual slots <paramref name="shardMap"/> routes to
    /// <paramref name="shardIndex"/>. When the map routes every slot to that
    /// shard the shard axis constrains nothing, so the filter carries no shard
    /// constraint and is unbounded if the range is.
    /// </summary>
    /// <param name="lowKeyInclusive">Inclusive low bound, or <see langword="null"/> for no lower bound.</param>
    /// <param name="highKeyExclusive">Exclusive high bound, or <see langword="null"/> for no upper bound.</param>
    /// <param name="shardMap">The routing map whose slot assignment defines shard ownership. Must not be <see langword="null"/> or empty.</param>
    /// <param name="shardIndex">The physical shard whose slots the filter owns.</param>
    /// <exception cref="ArgumentNullException"><paramref name="shardMap"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="shardMap"/> has no slots.</exception>
    public WalKeyFilter(string? lowKeyInclusive, string? highKeyExclusive, ShardMap shardMap, int shardIndex)
    {
        ArgumentNullException.ThrowIfNull(shardMap);
        var slots = shardMap.Slots;
        if (slots is null || slots.Length == 0)
        {
            throw new ArgumentException("The shard map has no slots, so it cannot define shard ownership.", nameof(shardMap));
        }

        // One bit per virtual slot: 512 bytes at the default 4096 slots, built
        // once per replay and shared by reference with every read it issues.
        var bits = new ulong[WordsFor(slots.Length)];
        var owned = 0;
        for (var slot = 0; slot < slots.Length; slot++)
        {
            if (slots[slot] == shardIndex)
            {
                bits[slot >> 6] |= 1UL << (slot & 63);
                owned++;
            }
        }

        LowKeyInclusive = lowKeyInclusive;
        HighKeyExclusive = highKeyExclusive;

        // A shard that owns every slot constrains nothing, and carrying no
        // constraint lets a single-shard tree's unbounded leaf take the
        // unfiltered read rather than classify records it can never exclude.
        if (owned < slots.Length)
        {
            VirtualShardCount = slots.Length;
            OwnedSlots = ImmutableCollectionsMarshal.AsImmutableArray(bits);
        }
    }

    /// <summary>Inclusive low bound of the owned key range, or <see langword="null"/> for no lower bound.</summary>
    [Id(0)] public string? LowKeyInclusive { get; init; }

    /// <summary>Exclusive high bound of the owned key range, or <see langword="null"/> for no upper bound.</summary>
    [Id(1)] public string? HighKeyExclusive { get; init; }

    /// <summary>
    /// The virtual slot count the shard constraint was built for, or <c>0</c>
    /// when the filter has no shard constraint.
    /// </summary>
    [Id(2)] public int VirtualShardCount { get; init; }

    /// <summary>
    /// The owned virtual slots as a bitmap: slot <c>s</c> is owned when bit
    /// <c>s % 64</c> of word <c>s / 64</c> is set. Default when the filter has
    /// no shard constraint.
    /// </summary>
    [Id(3)] public ImmutableArray<ulong> OwnedSlots { get; init; }

    /// <summary>
    /// <see langword="true"/> when the filter constrains the shard axis: a
    /// positive <see cref="VirtualShardCount"/> with a bitmap that covers it.
    /// </summary>
    public bool HasShardConstraint =>
        VirtualShardCount > 0
        && !OwnedSlots.IsDefault
        && OwnedSlots.Length == WordsFor(VirtualShardCount);

    /// <summary>
    /// <see langword="true"/> when the filter owns every key, so it excludes
    /// nothing.
    /// </summary>
    public bool IsUnbounded =>
        LowKeyInclusive is null && HighKeyExclusive is null && !HasShardConstraint;

    /// <summary>Reports whether the filter owns <paramref name="key"/>.</summary>
    /// <param name="key">The key to test. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    public bool Owns(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!SplitBoundary.Owns(key, LowKeyInclusive, HighKeyExclusive))
        {
            return false;
        }

        return !HasShardConstraint || OwnsSlot(ShardMap.GetVirtualSlot(key, VirtualShardCount));
    }

    /// <summary>
    /// Reports whether the filter excludes a record of <paramref name="kind"/>
    /// keyed by <paramref name="key"/>: a key-scoped kind whose key the filter
    /// does not own. A record with no key is never excluded.
    /// </summary>
    /// <param name="kind">The record's mutation kind.</param>
    /// <param name="key">The record's key, or <see langword="null"/>.</param>
    public bool Excludes(MutationKind kind, string? key) =>
        IsKeyScoped(kind) && key is not null && !Owns(key);

    /// <summary>
    /// <see cref="Excludes(MutationKind, string)"/> over the key's UTF-8 bytes,
    /// so a storage provider can decide exclusion from an encoded record without
    /// allocating the key string. The shard axis hashes the bytes directly,
    /// which is what <see cref="ShardMap.GetVirtualSlot"/> does after encoding;
    /// the range axis decodes into a stack or pooled buffer.
    /// </summary>
    internal bool ExcludesUtf8(MutationKind kind, ReadOnlySpan<byte> utf8Key)
    {
        if (!IsKeyScoped(kind))
        {
            return false;
        }

        if (HasShardConstraint)
        {
            var slot = (int)(XxHash32.HashToUInt32(utf8Key) % (uint)VirtualShardCount);
            if (!OwnsSlot(slot))
            {
                return true;
            }
        }

        if (LowKeyInclusive is null && HighKeyExclusive is null)
        {
            return false;
        }

        // A UTF-8 sequence never decodes to more UTF-16 code units than it has
        // bytes, so the byte count bounds the buffer.
        char[]? rented = null;
        Span<char> buffer = utf8Key.Length <= StackDecodeBytes
            ? stackalloc char[StackDecodeBytes]
            : (rented = ArrayPool<char>.Shared.Rent(utf8Key.Length));
        try
        {
            var written = Encoding.UTF8.GetChars(utf8Key, buffer);
            ReadOnlySpan<char> key = buffer[..written];
            var owned = (LowKeyInclusive is null || key.CompareTo(LowKeyInclusive, StringComparison.Ordinal) >= 0)
                && (HighKeyExclusive is null || key.CompareTo(HighKeyExclusive, StringComparison.Ordinal) < 0);
            return !owned;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<char>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="kind"/> is scoped to a single key, and so is the
    /// only kind a filter can exclude.
    /// </summary>
    internal static bool IsKeyScoped(MutationKind kind) =>
        kind is MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone;

    /// <summary>
    /// Compares two filters by what they own: the bounds ordinally, and the
    /// shard constraint by its slot count and bitmap contents. The generated
    /// record equality would compare the bitmap by reference, so a filter that
    /// crossed a serialization boundary would never equal itself.
    /// </summary>
    public bool Equals(WalKeyFilter other) =>
        string.Equals(LowKeyInclusive, other.LowKeyInclusive, StringComparison.Ordinal)
        && string.Equals(HighKeyExclusive, other.HighKeyExclusive, StringComparison.Ordinal)
        && HasShardConstraint == other.HasShardConstraint
        && (!HasShardConstraint
            || (VirtualShardCount == other.VirtualShardCount
                && OwnedSlots.AsSpan().SequenceEqual(other.OwnedSlots.AsSpan())));

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(LowKeyInclusive, StringComparer.Ordinal);
        hash.Add(HighKeyExclusive, StringComparer.Ordinal);
        if (HasShardConstraint)
        {
            hash.Add(VirtualShardCount);
            hash.AddBytes(MemoryMarshal.AsBytes(OwnedSlots.AsSpan()));
        }

        return hash.ToHashCode();
    }

    private bool OwnsSlot(int slot) => (OwnedSlots[slot >> 6] & (1UL << (slot & 63))) != 0;

    private static int WordsFor(int slots) => (slots + 63) >> 6;
}
