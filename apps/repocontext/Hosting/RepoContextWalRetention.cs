using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Arms the advisory WAL byte-pressure policy
/// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) on every repository-context
/// tree, so a tree whose WAL has grown past a ceiling has its trim frontier lowered
/// on each garbage-collection pass instead of retaining forever.
/// <para>
/// The library leaves the policy off by default and is right to: the ceiling is a
/// capacity quota, a correct value is a fraction of the volume the WAL lives on, and
/// a library cannot know that volume. This host can. It ships as a container with a
/// declared data volume and a documented resource envelope, so it is the layer that
/// can name a number - the same reason it already names a compaction dead-byte
/// budget, a pin bucket count, and a replay-concurrency ceiling rather than leaving
/// those at library defaults.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>What the absence cost, measured.</b> On a live container every tree reported
/// <c>orleans_lattice_wal_gc_backlog_bytes_unavailable_total{reason="policy_disabled"}</c>
/// and <c>orleans_lattice_wal_compaction_reclaimed_bytes_total</c> sat at exactly
/// <b>zero</b> for every tree but one - including <c>repo-context-vector-index</c>,
/// which had itself appended roughly 358 MB across its shards. The WAL was not
/// leaking (it measured flat at 3048 MB across three samples), but it was also never
/// giving anything back: on-disk occupancy was monotonically non-decreasing even
/// where the log was logically compactable, because no pass ever computed a byte
/// ceiling to trim toward.
/// </para>
/// <para>
/// <b>It cannot lose data and cannot block a write.</b> The ceiling is advisory. It
/// lowers the effective trim frontier toward
/// <see cref="LatticeOptions.WalBytePressureReclaimTarget"/> of the ceiling, but only
/// <i>within</i> the already-safe frontier - the minimum consumer cursor intersected
/// with the causal-stable frontier. When bytes cannot be safely reclaimed because a
/// consumer is lagging, the policy raises an advisory over-threshold signal and
/// leaves every byte intact. Trimming a WAL is not deleting a record either: the
/// records have already been materialised into leaves.
/// </para>
/// <para>
/// <b>The cost it does carry</b> is one <c>GetPhysicalByteSizeAsync</c> probe per WAL
/// partition on every garbage-collection pass. That is the cost the library declines
/// to impose on every consumer, and it is bounded here: the container held 153 WAL
/// files across eleven trees, and the pass cadence is already adaptive, relaxing
/// geometrically on a tree that trims nothing.
/// </para>
/// </remarks>
public static class RepoContextWalRetention
{
    /// <summary>Environment variable overriding the per-tree WAL retained-byte ceiling.</summary>
    public const string MaxRetainedBytesKey = "LATTICE_WAL_MAX_RETAINED_BYTES";

    /// <summary>
    /// The per-tree ceiling this host applies when <see cref="MaxRetainedBytesKey"/>
    /// is unset: 1 GiB.
    /// <para>
    /// Derived from measured occupancy rather than picked round. Every repository-context
    /// tree on a converged container sat well under 200 MB - vector-metadata 156 MB,
    /// vector-membership 45 MB, vector-coverage 33 MB - with two exceptions, both
    /// derived vector accelerators: <c>repo-context-vector-payload</c> at 1.1 GB and
    /// <c>repo-context-vector-index</c> at 1.7 GB. A 1 GiB ceiling therefore arms the
    /// policy on exactly the two trees whose growth motivated it and leaves the other
    /// nine untouched, so the probe cost is paid where there is something to reclaim
    /// and nowhere else.
    /// </para>
    /// <para>
    /// It is a ceiling, not a reservation: a tree below it costs one probe per
    /// partition per pass and nothing else. Raise it on a deployment indexing a
    /// substantially larger corpus, where the steady-state index legitimately exceeds
    /// this and a permanently-armed policy would trim on every pass without ever
    /// disarming.
    /// </para>
    /// </summary>
    public const long DefaultMaxRetainedBytes = 1L * 1024 * 1024 * 1024;

    /// <summary>
    /// The smallest accepted ceiling: 64 MiB. Below roughly this, a single WAL
    /// partition's live records can exceed the whole tree's budget, so the policy
    /// would arm permanently, never reach its low-water mark, and pay the probe on
    /// every pass forever while reclaiming nothing it was not already free to reclaim.
    /// </summary>
    public const long MinMaxRetainedBytes = 64L * 1024 * 1024;

    /// <summary>
    /// The sentinel that turns the policy off, restoring the library default. Accepted
    /// so an operator can opt out without editing the image - the host refuses an
    /// unparseable value rather than ignoring it, so "off" needs a spelling.
    /// </summary>
    public const string DisabledValue = "0";

    /// <summary>
    /// Resolves the per-tree ceiling from <paramref name="configuration"/>, falling
    /// back to <see cref="DefaultMaxRetainedBytes"/> when
    /// <see cref="MaxRetainedBytesKey"/> is absent or blank.
    /// </summary>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>
    /// The resolved ceiling in bytes, or <see langword="null"/> when the operator set
    /// <see cref="DisabledValue"/> to turn the policy off.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The variable is present but is not an integer in the accepted range. The host
    /// refuses to start rather than silently ignoring an operator's intent.
    /// </exception>
    public static long? ResolveMaxRetainedBytes(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var raw = configuration[MaxRetainedBytesKey];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultMaxRetainedBytes;
        }

        var trimmed = raw.Trim();
        if (string.Equals(trimmed, DisabledValue, StringComparison.Ordinal))
        {
            return null;
        }

        if (!long.TryParse(
                trimmed,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out var parsed)
            || parsed < MinMaxRetainedBytes)
        {
            throw new InvalidOperationException(
                $"{MaxRetainedBytesKey} must be an integer of at least {MinMaxRetainedBytes} bytes, "
                + $"or '{DisabledValue}' to disable the byte-pressure policy; was '{raw}'.");
        }

        return parsed;
    }

    /// <summary>
    /// Applies the resolved ceiling to every <see cref="RepoContextHostTrees.All"/>
    /// entry on the silo.
    /// <para>
    /// Deliberately per-tree rather than global, because the ceiling <i>is</i> per-tree
    /// and the WAL garbage collector resolves it from each tree's named options. It is
    /// applied to <see cref="RepoContextHostTrees.All"/>, not to
    /// <see cref="RepoContextHostTrees.ChurnTrees"/>: those two lists answer different
    /// questions. Churn is about tombstones, which is why the write-once
    /// vector-payload tree is excluded from it - it has no in-place deletes to reap.
    /// WAL retention is about bytes on disk, and a write-once tree accumulates those
    /// just as readily; on the measured container vector-payload was the
    /// second-largest WAL on the box at 1.1 GB. Excluding it here would have missed
    /// a third of the growth.
    /// </para>
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="configuration"/> is null.</exception>
    public static ISiloBuilder ConfigureRepoContextWalRetention(
        this ISiloBuilder silo,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var ceiling = ResolveMaxRetainedBytes(configuration);
        if (ceiling is null)
        {
            return silo;
        }

        foreach (var tree in RepoContextHostTrees.All)
        {
            silo.ConfigureLattice(tree, options => options.WalMaxRetainedBytes = ceiling);
        }

        return silo;
    }
}
