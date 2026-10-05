using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Flushes the durable WAL materialiser-pin advances that
/// <see cref="LeafCursorReporter"/> coalesced inside its debounce window but never
/// persisted, when the silo stops (issue #3509).
/// </summary>
/// <remarks>
/// The flush runs on the stop side of <see cref="ServiceLifecycleStage.ApplicationServices"/>.
/// Stop runs in reverse stage order, so this executes after
/// <see cref="ServiceLifecycleStage.GrainDeactivation"/> has let every leaf make its
/// final frontier note, and before <see cref="ServiceLifecycleStage.RuntimeStorageServices"/>
/// tears storage down. The flush is bounded by <see cref="DefaultDeadline"/> and every
/// failure is swallowed: the cost of a lost advance is extra retained WAL, never a
/// safety problem, so it must not block or fail shutdown. The steady-state reporting
/// path is unchanged and stays fire-and-forget.
/// </remarks>
internal sealed class LeafCursorReporterShutdownFlushParticipant(
    ILeafCursorReporter reporter,
    ILogger<LeafCursorReporterShutdownFlushParticipant>? logger = null) : ILifecycleParticipant<ISiloLifecycle>
{
    /// <summary>The upper bound on how long the stop-time flush may delay shutdown.</summary>
    internal static readonly TimeSpan DefaultDeadline = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    public void Participate(ISiloLifecycle lifecycle)
    {
        ArgumentNullException.ThrowIfNull(lifecycle);
        lifecycle.Subscribe(
            nameof(LeafCursorReporterShutdownFlushParticipant),
            ServiceLifecycleStage.ApplicationServices,
            static _ => Task.CompletedTask,
            OnStopAsync);
    }

    /// <summary>Runs the bounded flush when the reporter is the built-in implementation.</summary>
    internal Task OnStopAsync(CancellationToken cancellationToken)
    {
        if (reporter is not LeafCursorReporter builtIn)
        {
            logger?.LogDebug(
                "Skipping the shutdown durable-pin flush: the registered reporter {ReporterType} is not the built-in reporter.",
                reporter.GetType().Name);
            return Task.CompletedTask;
        }

        return builtIn.FlushPendingDurablePinsAsync(DefaultDeadline, cancellationToken);
    }
}
