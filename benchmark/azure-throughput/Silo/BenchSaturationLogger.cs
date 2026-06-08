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
/// </summary>
internal sealed class BenchSaturationLogger : IWalSaturationObserver
{
    /// <inheritdoc />
    public ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken)
    {
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
