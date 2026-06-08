using System.Collections.Concurrent;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// Per-silo benchmark observer that lands one greppable
/// <c>[silo:saturation]</c> line on stdout per transition of the
/// per-tree WAL saturation signal (F-085). The line names the
/// transition direction (<c>previous -&gt; new</c>) and the underlying
/// source attribution (partition for admission-depth-driven
/// transitions, shard for dispatch-timeout-driven transitions) so the
/// existing benchmark post-mortem pipeline can correlate the
/// producer's <c>slipMaxMs</c> spike with the silo's recorded
/// transition window without parsing the OpenTelemetry meter scrape.
/// <para>
/// The observer is the F-086 demonstration-of-fitness companion to
/// the polling adoption in <c>TcpIngestService.HandleConnectionAsync</c>:
/// the reader uses the polling shape on the hot path (zero cost per
/// TCP line); the observer surfaces the transitions as log events so
/// the cohort closeout in <c>throughput.md</c> can quote the exact
/// instant the producer back-pressure path engaged.
/// </para>
/// <para>
/// FX-029: the observer also records a per-tree wall-clock timestamp
/// of the most-recent <c>Saturated</c> transition via
/// <see cref="LastSaturatedUtc"/>. The bench's drain loop consults
/// this at the producer-stop boundary to decide whether to dispatch
/// the residual ingest-channel batch (when the tree has not recently
/// been saturated) or abandon it (when a saturation episode is in
/// progress or just ended). Using a recency timestamp rather than a
/// monotonic "ever-saturated" flag tolerates the F-085 classifier's
/// known <c>Healthy &lt;-&gt; Saturated</c> flap (tracked as FX-030):
/// a tree that flapped Saturated 200 ms ago is treated as
/// still-saturated for the purposes of the drain decision even if
/// the sampler's current tick reads Healthy.
/// </para>
/// </summary>
internal sealed class BenchSaturationLogger : IWalSaturationObserver
{
    // Per-tree wall-clock UTC of the most recently observed
    // Healthy -> Saturated (or Throttled -> Saturated) transition.
    // Reads are lock-free via ConcurrentDictionary; writes happen
    // only on transitions (rare). Trees that never reached Saturated
    // are absent from the map and the drain-decision helper returns
    // false (no recent saturation).
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastSaturatedUtc
        = new(StringComparer.Ordinal);

    /// <summary>
    /// Returns the wall-clock UTC at which the named tree most recently
    /// transitioned into <see cref="WalSaturationState.Saturated"/>, or
    /// <c>null</c> if the tree has never been observed saturated.
    /// Consulted by <c>TcpIngestService.DrainAsync</c> at the
    /// producer-stop boundary to decide whether to dispatch the
    /// residual batch or abandon it (FX-029).
    /// </summary>
    public DateTimeOffset? LastSaturatedUtc(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _lastSaturatedUtc.TryGetValue(treeId, out var ts) ? ts : null;
    }

    /// <inheritdoc />
    public ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken)
    {
        // FX-029: record the per-tree last-Saturated wall-clock so the
        // drain loop can detect recent saturation episodes even when
        // the current sampler tick reads Healthy (flap window).
        if (change.NewState == WalSaturationState.Saturated)
        {
            _lastSaturatedUtc[change.TreeId] = change.ObservedAt;
        }

        // Build a short attribution suffix so the line carries
        // partition / shard context when the sampler attributed the
        // transition to a single source. Aggregate-driven transitions
        // (e.g. dispatch-timeout rate summed across several partitions)
        // leave both slots null and the suffix is omitted.
        var attribution = (change.AttributedPartition, change.AttributedShard) switch
        {
            (int p, int s) => $" partition={p} shard={s}",
            (int p, null) => $" partition={p}",
            (null, int s) => $" shard={s}",
            _ => string.Empty,
        };
        Console.WriteLine(
            $"[silo:saturation] tree={change.TreeId} {Format(change.PreviousState)} -> {Format(change.NewState)}{attribution} observedAtUtc={change.ObservedAt:O}");
        return ValueTask.CompletedTask;
    }

    private static string Format(WalSaturationState state) => state switch
    {
        WalSaturationState.Healthy => "Healthy",
        WalSaturationState.Throttled => "Throttled",
        WalSaturationState.Saturated => "Saturated",
        _ => state.ToString(),
    };
}
