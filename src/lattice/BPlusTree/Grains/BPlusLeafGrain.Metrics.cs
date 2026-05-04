using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instrumentation for
/// <see cref="BPlusLeafGrain"/>. Wraps <c>IPersistentState.WriteStateAsync</c>
/// in a latency-capturing helper, exposes typed tag builders for the
/// scan, compaction, and tombstone-churn counters defined in
/// <see cref="LatticeMetrics"/>, and provides the dual-durability
/// commit-path helpers (lazy <see cref="ICommitLogWriter"/> resolution,
/// shadow-persist wrapper, and per-step latency recorder).
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Cached <see cref="ICommitLogWriter"/> resolved from
    /// <see cref="IGrainContext.ActivationServices"/> on first use.
    /// <see langword="null"/> when the host has not registered the
    /// commit-log adapter (i.e. no replication package), in which case
    /// the dual-durability commit path falls through to the legacy
    /// state-row persist exclusively.
    /// </summary>
    private ICommitLogWriter? _commitLogWriter;

    /// <summary>
    /// <see langword="true"/> once the lazy resolution of
    /// <see cref="_commitLogWriter"/> has run. Caching the outcome
    /// (including the <see langword="null"/> result) avoids paying the
    /// service-provider lookup on every commit.
    /// </summary>
    private bool _commitLogWriterResolved;

    /// <summary>
    /// Cached typed logger resolved on first use. Used by the
    /// dual-durability commit path to log a warning when the
    /// shadow-persist step throws (the caller still observes success
    /// because the WAL is the durable boundary).
    /// </summary>
    private ILogger<BPlusLeafGrain>? _logger;

    /// <summary>
    /// Persists the leaf's state and records the elapsed time on the
    /// <see cref="LatticeMetrics.LeafWriteDuration"/> histogram. The tree-id
    /// tag is sourced from persisted state and may be empty when the tree
    /// has not yet been registered with this leaf (pre-<c>SetTreeIdAsync</c>).
    /// </summary>
    private async Task PersistAsync()
    {
        var startTicks = Stopwatch.GetTimestamp();
        try
        {
            await state.WriteStateAsync();
        }
        finally
        {
            var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
            LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        }
    }

    /// <summary>Builds the single-tree tag used by every leaf-level instrument.</summary>
    private KeyValuePair<string, object?> LeafTreeTag() =>
        new(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty);

    /// <summary>
    /// Lazily resolves the commit-log writer from the activation's
    /// service provider. Returns <see langword="null"/> when no adapter
    /// has been registered (the legacy-only commit path).
    /// </summary>
    private ICommitLogWriter? ResolveCommitLogWriter()
    {
        if (_commitLogWriterResolved)
            return _commitLogWriter;

        _commitLogWriterResolved = true;
        _commitLogWriter = context.ActivationServices?.GetService<ICommitLogWriter>();
        return _commitLogWriter;
    }

    /// <summary>
    /// Lazily resolves an <see cref="ILogger{BPlusLeafGrain}"/> from the
    /// activation's service provider, caching the result.
    /// </summary>
    private ILogger<BPlusLeafGrain>? ResolveLogger()
    {
        if (_logger is not null)
            return _logger;

        _logger = context.ActivationServices?
            .GetService<ILoggerFactory>()?
            .CreateLogger<BPlusLeafGrain>();
        return _logger;
    }

    /// <summary>
    /// Records the elapsed time since <paramref name="startTicks"/> on
    /// <see cref="LatticeMetrics.LeafCommitDuration"/> tagged with
    /// <see cref="LatticeMetrics.TagStep"/> = <paramref name="step"/>.
    /// </summary>
    private void RecordCommitStep(string step, long startTicks)
    {
        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        LatticeMetrics.LeafCommitDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty),
            new KeyValuePair<string, object?>(LatticeMetrics.TagStep, step));
    }
}