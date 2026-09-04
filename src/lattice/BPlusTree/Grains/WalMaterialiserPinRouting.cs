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
    /// Separates a pin grain key from its shard suffix. Storage-safe: a pin grain
    /// is persistent, and keyed storage backends reject <c>/</c>, <c>\</c>,
    /// <c>#</c> and <c>?</c> in a grain key because it is carried into the
    /// Partition/Row key columns and the request URL.
    /// </summary>
    public const string ShardSeparator = "~s";

    /// <summary>
    /// The separator used before the storage-safe one was adopted. Still read so
    /// a pin written by an earlier build keeps counting toward the WAL trim
    /// floor; never written.
    /// </summary>
    public const string LegacyShardSeparator = "#s";

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
    /// <remarks>
    /// Marked as a grain-key builder because the result is an Orleans grain
    /// primary key on a persistent grain, so the reflection-driven storage-safety
    /// guard audits it automatically.
    /// </remarks>
    [GrainKeyBuilder]
    public static string ShardKey(string treeName, string consumerId, int shardCount)
    {
        if (shardCount <= 1)
        {
            return treeName;
        }

        var shard = (int)(StableHash(consumerId) % (uint)shardCount);
        return ComposeShardKey(treeName, shard, ShardSeparator);
    }

    /// <summary>
    /// Enumerates every grain key the WAL GC must read to reconstruct the full
    /// durable pin floor for <paramref name="treeName"/>: each shard key under
    /// the current separator, each under the legacy separator, and the legacy
    /// unsuffixed key. When <paramref name="shardCount"/> is one this yields only
    /// the legacy key.
    /// </summary>
    /// <remarks>
    /// The dual read is what makes the separator change self-healing. A pin
    /// written by an earlier build still participates in the trim floor, so no
    /// WAL segment is stranded and no operator action is needed; new pins are
    /// written under the storage-safe key and the old ones fall away as their
    /// consumers re-pin. It is the same migration the pre-sharding legacy key
    /// already relies on, widened by one separator.
    /// </remarks>
    public static IReadOnlyList<string> EnumerateReadKeys(string treeName, int shardCount)
    {
        if (shardCount <= 1)
        {
            return new[] { treeName };
        }

        var keys = new string[(shardCount * 2) + 1];
        var at = 0;
        for (var shard = 0; shard < shardCount; shard++)
        {
            keys[at++] = ComposeShardKey(treeName, shard, ShardSeparator);
        }

        for (var shard = 0; shard < shardCount; shard++)
        {
            keys[at++] = ComposeShardKey(treeName, shard, LegacyShardSeparator);
        }

        // Legacy key last so a pre-sharding pin participates in the union.
        keys[at] = treeName;
        return keys;
    }

    /// <summary>
    /// Strips a shard suffix written under either separator, yielding the logical
    /// tree name. Parsed from the end and only when the suffix is entirely
    /// digits, so a tree whose own name contains the separator is not truncated
    /// at the wrong place.
    /// </summary>
    /// <param name="key">The pin grain key.</param>
    /// <returns>The tree name the key belongs to.</returns>
    public static string TreeNameFromKey(string? key)
    {
        if (string.IsNullOrEmpty(key))
        {
            return string.Empty;
        }

        return TryStrip(key, ShardSeparator, out var stripped)
            || TryStrip(key, LegacyShardSeparator, out stripped)
                ? stripped
                : key;
    }

    /// <summary>
    /// Parses the shard ordinal out of a pin grain key written under either
    /// separator, returning <c>0</c> for the legacy unsuffixed key (which is
    /// also the key used when the shard count is one). Used for per-shard
    /// attribution on the saturation signal.
    /// </summary>
    /// <param name="key">The pin grain key.</param>
    /// <returns>The shard ordinal the key addresses.</returns>
    public static int ShardIndexFromKey(string? key)
    {
        if (string.IsNullOrEmpty(key))
        {
            return 0;
        }

        return TryParseShard(key, ShardSeparator, out var shard)
            || TryParseShard(key, LegacyShardSeparator, out shard)
                ? shard
                : 0;
    }

    private static bool TryParseShard(string key, string separator, out int shard)
    {
        var idx = key.LastIndexOf(separator, StringComparison.Ordinal);
        if (idx >= 0)
        {
            var suffix = key.AsSpan(idx + separator.Length);
            if (IsAllDigits(suffix) && int.TryParse(suffix, System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture, out shard))
            {
                return true;
            }
        }

        shard = 0;
        return false;
    }

    /// <summary>
    /// Separates a durable pin <b>bucket</b> slot name from its ordinal. Bucket
    /// slots are grain-state names rather than grain keys, so they are not
    /// subject to the grain-key character restrictions that motivated
    /// <see cref="ShardSeparator"/>; the same spelling is reused for symmetry.
    /// </summary>
    public const string BucketSeparator = "~b";

    /// <summary>
    /// Resolves the configured durable pin bucket count from the global
    /// (unkeyed) options, clamped to at least one. Read the same way as
    /// <see cref="ResolveShardCount"/>.
    /// </summary>
    /// <param name="options">The options monitor, or <c>null</c>.</param>
    /// <returns>The effective bucket count.</returns>
    public static int ResolveBucketCount(IOptionsMonitor<LatticeOptions>? options)
    {
        if (options is null)
        {
            return 1;
        }

        return Math.Max(1, options.Get(string.Empty).WalMaterialiserPinBuckets);
    }

    /// <summary>
    /// Returns the durable grain-state slot name holding
    /// <paramref name="consumerId"/>'s pin for a shard configured with
    /// <paramref name="bucketCount"/> buckets.
    /// <para>
    /// A bucket count of one returns
    /// <see cref="WalMaterialiserPinState.StateName"/> unchanged, so a host that
    /// leaves the option at its default persists to byte-for-byte the same
    /// single slot as every build before bucketing existed. Only a host that
    /// explicitly raises the count writes suffixed slots.
    /// </para>
    /// </summary>
    /// <param name="consumerId">The leaf-materialiser consumer id.</param>
    /// <param name="bucketCount">The configured bucket count.</param>
    /// <returns>The durable state slot name to persist the pin under.</returns>
    public static string BucketStateName(string consumerId, int bucketCount)
    {
        if (bucketCount <= 1)
        {
            return WalMaterialiserPinState.StateName;
        }

        return BucketStateName(BucketOf(consumerId, bucketCount));
    }

    /// <summary>
    /// Resolves the bucket ordinal owning <paramref name="consumerId"/> under
    /// <paramref name="bucketCount"/>. Uses the same stable, process-independent
    /// hash as shard routing, so a consumer lands in the same bucket in every
    /// process and after every restart.
    /// </summary>
    /// <param name="consumerId">The leaf-materialiser consumer id.</param>
    /// <param name="bucketCount">The configured bucket count.</param>
    /// <returns>The bucket ordinal, or zero when bucketing is disabled.</returns>
    public static int BucketOf(string consumerId, int bucketCount)
        => bucketCount <= 1 ? 0 : (int)(StableHash(consumerId) % (uint)bucketCount);

    /// <summary>
    /// Returns the durable grain-state slot name for bucket
    /// <paramref name="bucket"/>.
    /// </summary>
    /// <param name="bucket">The bucket ordinal.</param>
    /// <returns>The durable state slot name.</returns>
    public static string BucketStateName(int bucket)
        => string.Concat(
            WalMaterialiserPinState.StateName,
            BucketSeparator,
            bucket.ToString(System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>
    /// Enumerates every durable state slot a pin shard activation must read to
    /// reconstruct its full pin map: each bucket slot plus the legacy
    /// unsuffixed slot.
    /// <para>
    /// Reading the legacy slot unconditionally is what makes raising the bucket
    /// count self-healing, on exactly the reasoning
    /// <see cref="EnumerateReadKeys"/> already applies to the shard dimension. A
    /// pin written before bucketing was enabled keeps counting toward the trim
    /// floor - it is merged into memory on activation and re-persisted into its
    /// bucket on the next advance - so no WAL segment is stranded and no
    /// operator migration step is required.
    /// </para>
    /// </summary>
    /// <param name="bucketCount">The configured bucket count.</param>
    /// <returns>The durable state slot names to read.</returns>
    public static IReadOnlyList<string> EnumerateBucketStateNames(int bucketCount)
    {
        if (bucketCount <= 1)
        {
            return new[] { WalMaterialiserPinState.StateName };
        }

        var names = new string[bucketCount + 1];
        for (var bucket = 0; bucket < bucketCount; bucket++)
        {
            names[bucket] = BucketStateName(bucket);
        }

        // Legacy slot last so a pre-bucketing pin participates in the union.
        names[bucketCount] = WalMaterialiserPinState.StateName;
        return names;
    }

    private static bool TryStrip(string key, string separator, out string treeName)
    {
        // Anchored at the last occurrence: the suffix is appended, so an earlier
        // occurrence belongs to the tree name itself.
        var idx = key.LastIndexOf(separator, StringComparison.Ordinal);
        if (idx >= 0 && IsAllDigits(key.AsSpan((idx + separator.Length))))
        {
            treeName = key[..idx];
            return true;
        }

        treeName = key;
        return false;
    }

    private static bool IsAllDigits(ReadOnlySpan<char> value)
    {
        if (value.IsEmpty)
        {
            return false;
        }

        foreach (var c in value)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        return true;
    }

    private static string ComposeShardKey(string treeName, int shard, string separator)
        => string.Concat(treeName, separator, shard.ToString(System.Globalization.CultureInfo.InvariantCulture));

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
