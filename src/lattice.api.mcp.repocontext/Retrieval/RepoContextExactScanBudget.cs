using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How large a corpus the brute-force <see cref="ExactKnnSemanticIndex"/> gather
/// may visit before the scan is no longer worth starting, derived entirely from
/// the scan-page budget the vector-metadata tree is configured with.
/// <para>
/// <b>Why a budget exists at all.</b> The gather range-scans
/// <see cref="RepoContextTrees.VectorMetadata"/> one page at a time. Each page
/// fill is bounded by <see cref="LatticeOptions.MaxScanPageStallDuration"/>, the
/// hard ceiling past which the shard root abandons the fill with
/// <see cref="ScanPageStalledException"/> rather than keep holding its
/// non-reentrant shard. While the approximate plane is still building, that
/// ceiling is reached routinely: the gather competes for the very tree the build
/// is streaming, so the ladder approximate -> exact -> keyword degenerates to
/// keyword by way of a full ceiling's worth of latency, and the doomed scan slows
/// the build it is waiting on. Not starting a scan that cannot finish costs
/// nothing and removes both effects.
/// </para>
/// <para>
/// <b>The threshold is derived, never a row count.</b> Two configured options on
/// the tree being scanned fix it, so retuning either moves the threshold with it
/// and no magic constant can go stale:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <see cref="LatticeOptions.MaxScanPageDuration"/> - the cooperative budget one
/// page fill is expected to complete within. It is the store's own statement of
/// what a page fill nominally costs.
/// </description></item>
/// <item><description>
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> - the hard ceiling on a
/// single page fill. It is the store's own statement of how much wall-clock scan
/// work is affordable before a scan is declared stalled.
/// </description></item>
/// </list>
/// <para>
/// A gather over <c>C</c> vectors issues
/// <c>ceil(C / <see cref="RepoContextPortability.DefaultPageSize"/>)</c>
/// sequential page fills, so at the nominal per-page cost it runs for
/// <c>pages * MaxScanPageDuration</c>. It is affordable only while that
/// projection stays inside the hard ceiling, which gives
/// <c>affordablePages = MaxScanPageStallDuration / MaxScanPageDuration</c> and
/// <c>affordableVectors = affordablePages * DefaultPageSize</c>. On the shipped
/// defaults (25s effective ceiling, 5s page budget, 256 rows per page) that is
/// 1,280 vectors - comfortably above
/// <see cref="RepoContextAnnOptions.MinimumTrainingCount"/>, below which the plane
/// trains no partitioning and answers exhaustively for itself rather than falling
/// back to the exact gather at all.
/// </para>
/// <para>
/// <b>It fails open.</b> Every case where the budget cannot be computed - the
/// ceiling disabled, the cooperative bound disabled - reports
/// <see cref="Unbounded"/>, so the exact gather keeps running exactly as before.
/// The budget only ever removes a scan that the configuration itself says cannot
/// complete.
/// </para>
/// </summary>
internal sealed class RepoContextExactScanBudget
{
    /// <summary>
    /// Reported when no bound applies, so the exact gather runs whatever the
    /// corpus size. This is the fail-open answer, not a very large threshold.
    /// </summary>
    internal const int Unbounded = int.MaxValue;

    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly TimeSpan _responseTimeout;

    /// <summary>Creates the budget.</summary>
    /// <param name="options">The monitor the vector-metadata tree's effective scan-page options are read from. Must not be <see langword="null"/>.</param>
    /// <param name="siloMessagingOptions">
    /// The silo's messaging options, used only to reproduce the derivation
    /// <see cref="LatticeOptions.MaxScanPageStallDuration"/> performs when it is
    /// left unset. Absent - in a client host, or a test that constructs this
    /// directly - the Orleans default is used, which is what such a silo runs
    /// with anyway.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    public RepoContextExactScanBudget(
        IOptionsMonitor<LatticeOptions> options,
        IOptions<SiloMessagingOptions>? siloMessagingOptions = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _responseTimeout =
            siloMessagingOptions?.Value.ResponseTimeout ?? new SiloMessagingOptions().ResponseTimeout;
    }

    /// <summary>
    /// The largest corpus an exact gather may visit under the vector-metadata
    /// tree's current configuration, or <see cref="Unbounded"/> when no bound
    /// applies. Read per query rather than cached, because
    /// <see cref="IOptionsMonitor{TOptions}"/> is live and a reconfigured budget
    /// must take effect without a restart.
    /// </summary>
    public int AffordableVectorCount
    {
        get
        {
            var options = _options.Get(RepoContextTrees.VectorMetadata);
            return AffordableVectors(
                EffectiveStallCeiling(options),
                options.MaxScanPageDuration,
                RepoContextPortability.DefaultPageSize);
        }
    }

    /// <summary>
    /// Projects the two configured page-fill bounds onto the number of vectors a
    /// sequential gather may visit within the hard ceiling.
    /// </summary>
    /// <param name="stallCeiling">
    /// The effective hard ceiling on one page fill.
    /// <see cref="Timeout.InfiniteTimeSpan"/> or a non-positive value disables the
    /// bound.
    /// </param>
    /// <param name="nominalPageFill">
    /// The cooperative budget one page fill is expected to complete within. A
    /// non-positive value means the store publishes no nominal page cost, so
    /// there is nothing to project from and the bound is disabled.
    /// </param>
    /// <param name="pageSize">Rows the gather requests per page fill. Must be positive.</param>
    /// <returns>The affordable vector count, or <see cref="Unbounded"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    internal static int AffordableVectors(TimeSpan stallCeiling, TimeSpan nominalPageFill, int pageSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        if (stallCeiling == Timeout.InfiniteTimeSpan || stallCeiling <= TimeSpan.Zero)
        {
            return Unbounded;
        }

        if (nominalPageFill == Timeout.InfiniteTimeSpan || nominalPageFill <= TimeSpan.Zero)
        {
            return Unbounded;
        }

        // At least one page: the validator already requires the ceiling to exceed
        // the cooperative budget, but a floor of one page keeps a misconfigured
        // pair from producing a zero threshold that would skip every gather.
        var pages = Math.Max(1L, stallCeiling.Ticks / nominalPageFill.Ticks);
        var vectors = pages * pageSize;
        return vectors >= Unbounded ? Unbounded : (int)vectors;
    }

    /// <summary>
    /// Reproduces the effective
    /// <see cref="LatticeOptions.MaxScanPageStallDuration"/> the shard root arms a
    /// page fill with: the explicit value when one is configured, otherwise the
    /// silo's response timeout less
    /// <see cref="LatticeOptions.DefaultMaxScanPageStallHeadroom"/>, floored at the
    /// cooperative budget. The resolver that owns this derivation is internal to
    /// the core library, so it is mirrored here rather than reached into; the
    /// values it reads are plain passthroughs of the silo-wide options, so the two
    /// cannot disagree without the options themselves disagreeing.
    /// </summary>
    /// <param name="options">The tree's effective options.</param>
    /// <returns>The effective ceiling, which may be <see cref="Timeout.InfiniteTimeSpan"/>.</returns>
    internal TimeSpan EffectiveStallCeiling(LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.MaxScanPageStallDuration is { } configured)
        {
            return configured;
        }

        if (_responseTimeout == Timeout.InfiniteTimeSpan)
        {
            return Timeout.InfiniteTimeSpan;
        }

        var derived = _responseTimeout - LatticeOptions.DefaultMaxScanPageStallHeadroom;
        var floor = options.MaxScanPageDuration;
        return floor != Timeout.InfiniteTimeSpan && derived < floor ? floor : derived;
    }
}
