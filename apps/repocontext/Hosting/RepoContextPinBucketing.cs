using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Splits the WAL materialiser pin state across several persisted buckets so an
/// advancing retention floor rewrites a fraction of the pin blob rather than all
/// of it. The library default is a single bucket (bit-for-bit the pre-bucketing
/// write path, chosen so an existing deployment is unaffected by upgrading);
/// this host opts in because its own measurements make the single-bucket cost
/// concrete - a live box carried 216 pin rows totalling ~35 MB whose cumulative
/// version count had reached ~5.0 million, with the busiest shards rewriting
/// ~1.4 MB blobs tens of thousands of times each. That is a durability-cost
/// defect this host closes at wiring time, in the same spirit as
/// <see cref="RepoContextCompaction"/>.
/// </summary>
/// <remarks>
/// Widening is a safe, self-migrating change: the grain reads the legacy single
/// slot on activation (so every existing pin is loaded and authoritative), merges
/// every bucket monotonic-max, and never clears the legacy slot - which therefore
/// remains a rollback anchor. Reverting to one bucket, or to an image that
/// predates bucketing, reads only that legacy slot and so resolves an *older*
/// floor, retaining more WAL rather than over-trimming it. Every failure mode
/// degrades to over-retention, never to data loss.
/// </remarks>
public static class RepoContextPinBucketing
{
    /// <summary>Environment variable overriding the WAL materialiser pin bucket count.</summary>
    public const string PinBucketsKey = "LATTICE_WAL_PIN_BUCKETS";

    /// <summary>
    /// The bucket count this host applies when <see cref="PinBucketsKey"/> is unset.
    /// Eight matches the default pin shard count, so a shard's consumers spread
    /// evenly across buckets and a typical advance rewrites roughly an eighth of
    /// the blob.
    /// </summary>
    public const int DefaultPinBuckets = 8;

    /// <summary>
    /// The largest accepted bucket count. Every bucket is a distinct grain-storage
    /// slot read on activation, so an implausibly large value trades the write
    /// amplification this setting exists to remove for an equally bad read fan-in.
    /// </summary>
    public const int MaxPinBuckets = 256;

    /// <summary>
    /// Resolves the bucket count from <paramref name="configuration"/>, falling back
    /// to <see cref="DefaultPinBuckets"/> when <see cref="PinBucketsKey"/> is absent
    /// or blank.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The resolved bucket count, between 1 and <see cref="MaxPinBuckets"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not an integer in the accepted range. The host
    /// refuses to start rather than silently ignoring an operator's intent.
    /// </exception>
    public static int ResolveBucketCount(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[PinBucketsKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultPinBuckets;
        }

        if (!int.TryParse(raw.Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            || parsed < 1
            || parsed > MaxPinBuckets)
        {
            throw new InvalidOperationException(
                $"{PinBucketsKey} must be an integer between 1 and {MaxPinBuckets}; was '{raw}'.");
        }

        return parsed;
    }

    /// <summary>
    /// Applies the resolved bucket count as a <b>global</b> Lattice option on the
    /// silo. Deliberately global rather than per-tree: the pin router resolves the
    /// bucket count from the unnamed options instance, because a pin grain answers
    /// for a shard of consumers that can span trees.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="configuration"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextPinBucketing(
        this ISiloBuilder silo,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var buckets = ResolveBucketCount(configuration);
        silo.ConfigureLattice(options => options.WalMaterialiserPinBuckets = buckets);
        return silo;
    }
}
