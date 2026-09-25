using System.Buffers.Binary;
using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless routing helper that spreads a tree's saga decision registry across
/// durable <see cref="ITxRegistryGrain"/> activations (issue #3501). Each shard
/// has its own persisted row, admission budget, and decisions revision, so the
/// sustained saga rate a tree can retain scales with the number of shards new
/// sagas are minted across (<see cref="LatticeOptions.TxRegistryShardCount"/>).
/// <para>
/// <b>The shard travels in the transaction id, and routing depends on the id
/// alone.</b> A sharded transaction id is minted by
/// <see cref="MintTransactionId"/> as an RFC 9562 version-8 (custom) UUID whose
/// last byte carries the shard index. Every registry caller - the saga
/// coordinator, a leaf resolving a pending intent, a shard root, a split, a
/// replication receiver - derives the owning grain from that stamped index
/// through <see cref="ShardKey"/>, with no reference to any silo's configured
/// shard count. Two silos configured with different counts therefore route the
/// same txid to the same grain, and changing the count (up or down) never
/// reroutes an existing id. Any other id (a version-4 id minted before sharding
/// existed, or under a shard count of one) routes to the <b>legacy</b> registry
/// keyed by the bare tree id, which is the pre-sharding layout byte-for-byte.
/// </para>
/// <para>
/// Tree-wide operations (snapshots, the decisions revision, the cross-tree
/// in-flight observation, cursor pins) cover every key returned by
/// <see cref="EnumerateKeys"/> for the tree's durable shard high-water mark
/// (see <see cref="ITxRegistryHighWaterGrain"/> and
/// <see cref="TxRegistryFanOut"/>): each shard that can hold a decision, plus the
/// legacy key.
/// </para>
/// </summary>
internal static class TxRegistryRouting
{
    /// <summary>
    /// Separates a registry grain key from its shard suffix. Storage-safe: the
    /// registry is persistent and keyed storage backends reject <c>/</c>,
    /// <c>\</c>, <c>#</c> and <c>?</c> in a grain key.
    /// </summary>
    public const string ShardSeparator = "~s";

    /// <summary>The UUID version stamped into a sharded transaction id.</summary>
    public const int ShardedTransactionIdVersion = 8;

    private const int ShardByteIndex = 15;
    private const int VersionByteIndex = 7;

    /// <summary>
    /// Resolves the configured shard count from the global (unnamed) options,
    /// clamped to <c>[1, <see cref="LatticeOptions.MaxTxRegistryShardCount"/>]</c>.
    /// A <see langword="null"/> monitor yields one (the legacy layout). The
    /// count only decides which shards <b>new</b> transaction ids are minted
    /// across; it never takes part in routing an existing id or in choosing
    /// which keys a tree-wide read covers.
    /// </summary>
    /// <param name="options">The options monitor, or <see langword="null"/>.</param>
    /// <returns>The effective shard count.</returns>
    public static int ResolveShardCount(IOptionsMonitor<LatticeOptions>? options)
    {
        if (options is null)
        {
            return 1;
        }

        var configured = options.Get(string.Empty)?.TxRegistryShardCount ?? 1;
        return Math.Clamp(configured, 1, LatticeOptions.MaxTxRegistryShardCount);
    }

    /// <summary>
    /// Resolves the configured shard count from <paramref name="services"/> for
    /// a caller that has no injected options monitor. A missing provider or
    /// monitor yields one (the legacy layout).
    /// </summary>
    /// <param name="services">The activation service provider, or <see langword="null"/>.</param>
    /// <returns>The effective shard count.</returns>
    public static int ResolveShardCountFromServices(IServiceProvider? services) =>
        ResolveShardCount(services?.GetService(typeof(IOptionsMonitor<LatticeOptions>)) as IOptionsMonitor<LatticeOptions>);

    /// <summary>
    /// Mints a new saga transaction id for a tree configured with
    /// <paramref name="shardCount"/> registry shards. A count of one returns a
    /// plain random (version-4) id, which routes to the legacy registry for its
    /// whole life; a larger count returns a version-8 id carrying a uniformly
    /// chosen shard index in its last byte.
    /// </summary>
    /// <param name="shardCount">The configured shard count.</param>
    /// <returns>The minted transaction id.</returns>
    public static Guid MintTransactionId(int shardCount)
    {
        var id = Guid.NewGuid();
        if (shardCount <= 1)
        {
            return id;
        }

        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes);
        var count = (uint)Math.Min(shardCount, LatticeOptions.MaxTxRegistryShardCount);
        // Version nibble lives in the high half of byte 7 of the little-endian
        // layout; the RFC variant bits (byte 8) are already set by NewGuid.
        bytes[VersionByteIndex] = (byte)((bytes[VersionByteIndex] & 0x0F) | (ShardedTransactionIdVersion << 4));
        bytes[ShardByteIndex] = (byte)(BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(8, 4)) % count);
        return new Guid(bytes);
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="txid"/> was minted by
    /// <see cref="MintTransactionId"/> with a shard count above one.
    /// </summary>
    /// <param name="txid">The transaction id.</param>
    /// <returns>Whether the id carries a registry shard.</returns>
    public static bool IsSharded(Guid txid) => txid.Version == ShardedTransactionIdVersion;

    /// <summary>
    /// Returns the registry shard stamped into <paramref name="txid"/>, or
    /// <c>-1</c> when the id routes to the legacy registry (an unsharded id).
    /// The stamped index is returned as-is, never reduced by a configured shard
    /// count: routing is a pure function of the id, so every silo agrees on the
    /// owner whatever its configuration, and an id minted on a cluster with a
    /// larger count (a replicated saga, for instance) keeps its own shard.
    /// </summary>
    /// <param name="txid">The transaction id.</param>
    /// <returns>The shard index in <c>[0, 255]</c>, or <c>-1</c> for the legacy registry.</returns>
    public static int ShardOf(Guid txid)
    {
        if (!IsSharded(txid))
        {
            return -1;
        }

        Span<byte> bytes = stackalloc byte[16];
        txid.TryWriteBytes(bytes);
        return bytes[ShardByteIndex];
    }

    /// <summary>
    /// Returns the registry grain key owning <paramref name="txid"/> on
    /// <paramref name="treeId"/>: <c>{treeId}~s{shard}</c> for a sharded id, or
    /// the bare <paramref name="treeId"/> (the legacy registry) otherwise.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="txid">The transaction id.</param>
    /// <returns>The registry grain key.</returns>
    [GrainKeyBuilder]
    public static string ShardKey(string treeId, Guid txid)
    {
        var shard = ShardOf(txid);
        return shard < 0 ? treeId : ShardKeyAt(treeId, shard);
    }

    /// <summary>
    /// Returns the registry grain key for shard <paramref name="shard"/> of
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shard">The shard index.</param>
    /// <returns>The registry grain key.</returns>
    [GrainKeyBuilder]
    public static string ShardKeyAt(string treeId, int shard) =>
        string.Concat(treeId, ShardSeparator, shard.ToString(CultureInfo.InvariantCulture));

    /// <summary>
    /// Enumerates the registry grain keys a tree-wide read covers when shards
    /// <c>[0, <paramref name="shardHighWater"/>)</c> may hold decisions: each
    /// such shard in ordinal order, then the legacy bare-tree-id key. A
    /// high-water of zero or less (a tree no shard has written to) yields only
    /// the legacy key.
    /// </summary>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="shardHighWater">One more than the highest shard index that may hold a decision, or zero.</param>
    /// <returns>The registry grain keys.</returns>
    public static string[] EnumerateKeys(string treeId, int shardHighWater)
    {
        if (shardHighWater <= 0)
        {
            return [treeId];
        }

        var count = Math.Min(shardHighWater, LatticeOptions.MaxTxRegistryShardCount);
        var keys = new string[count + 1];
        for (var shard = 0; shard < count; shard++)
        {
            keys[shard] = ShardKeyAt(treeId, shard);
        }

        keys[count] = treeId;
        return keys;
    }

    /// <summary>
    /// Returns the registry grain owning <paramref name="txid"/> on
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="treeId">The physical tree id.</param>
    /// <param name="txid">The transaction id.</param>
    /// <returns>The owning registry grain reference.</returns>
    public static ITxRegistryGrain GetRegistry(IGrainFactory grainFactory, string treeId, Guid txid) =>
        grainFactory.GetGrain<ITxRegistryGrain>(ShardKey(treeId, txid));

    /// <summary>
    /// Strips a shard suffix from a registry grain key, yielding the tree id.
    /// Parsed from the end and only when the suffix is entirely digits, so a
    /// legacy key whose tree id merely contains the separator is returned whole.
    /// </summary>
    /// <param name="key">The registry grain key.</param>
    /// <returns>The tree id the key belongs to.</returns>
    public static string TreeIdFromKey(string key) =>
        TryParseShardKey(key, out var treeId, out _) ? treeId : key;

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="key"/> is a legacy
    /// (unsuffixed) registry key rather than a shard key.
    /// </summary>
    /// <param name="key">The registry grain key.</param>
    /// <returns>Whether the key addresses the legacy registry.</returns>
    public static bool IsLegacyKey(string key) => !TryParseShardKey(key, out _, out _);

    /// <summary>
    /// Parses a registry grain key into its tree id and shard index. Returns
    /// <see langword="false"/> (with <paramref name="treeId"/> set to the whole
    /// key and <paramref name="shard"/> to <c>-1</c>) for a legacy key.
    /// </summary>
    /// <param name="key">The registry grain key.</param>
    /// <param name="treeId">The tree id the key belongs to.</param>
    /// <param name="shard">The shard index, or <c>-1</c> for a legacy key.</param>
    /// <returns>Whether <paramref name="key"/> addresses a shard.</returns>
    public static bool TryParseShardKey(string key, out string treeId, out int shard)
    {
        var idx = key.LastIndexOf(ShardSeparator, StringComparison.Ordinal);
        if (idx > 0)
        {
            var suffix = key.AsSpan(idx + ShardSeparator.Length);
            if (!suffix.IsEmpty
                && suffix.Length <= 3
                && IsAllDigits(suffix)
                && int.TryParse(suffix, NumberStyles.None, CultureInfo.InvariantCulture, out shard)
                && shard < LatticeOptions.MaxTxRegistryShardCount)
            {
                treeId = key[..idx];
                return true;
            }
        }

        treeId = key;
        shard = -1;
        return false;
    }

    private static bool IsAllDigits(ReadOnlySpan<char> value)
    {
        foreach (var c in value)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        return true;
    }
}
