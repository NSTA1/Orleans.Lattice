using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo hosted-service wrapper that wires the
/// <see cref="WalCommitLogWriter"/>'s drain entry into the host's
/// shutdown lifecycle. On <see cref="StopAsync"/> (fired by the host
/// when SIGTERM arrives or the silo otherwise begins shutdown) the
/// service invokes <see cref="WalCommitLogWriter.DrainAsync"/> so
/// every parked admission-semaphore caller is released within bounded
/// wall-clock time of the shutdown signal, rather than blocking on a
/// downstream provider call whose SDK retry loop ignores cancellation
/// once an HTTP request has been handed to the underlying socket.
/// <para>
/// Multi-silo: registered per-silo (one <see cref="IHostedService"/>
/// per host builder, which is one per silo process). Each silo's
/// service drains only the local <see cref="WalCommitLogWriter"/>
/// singleton; peer silos in the cluster have their own service
/// triggered by their own <see cref="StopAsync"/>. There is no
/// cross-silo coordination: a draining silo's downstream
/// <see cref="IWalShardGrain"/> activations continue serving traffic
/// from any peer silo whose writer has not yet drained.
/// </para>
/// <para>
/// Idempotency: <see cref="WalCommitLogWriter.DrainAsync"/> is
/// itself idempotent; a host that calls <see cref="StopAsync"/>
/// twice (or that explicitly invokes <see cref="WalCommitLogWriter.DrainAsync"/>
/// before <see cref="StopAsync"/> fires) sees the second invocation
/// as a no-op rather than a corrupted state.
/// </para>
/// </summary>
internal sealed class WalCommitLogWriterDrainer(
    ICommitLogWriter writer,
    ILogger<WalCommitLogWriterDrainer> logger) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // No work on start: the writer is constructed lazily by DI on
        // first use, and the drain seam is purely passive until
        // StopAsync fires. Returning a completed task keeps the host
        // startup cost at zero.
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        // Only the canonical WalCommitLogWriter has a drain seam. Other
        // ICommitLogWriter implementations (in-process fakes, test
        // doubles, future replacements) opt out of the writer-side drain
        // contract by not deriving from WalCommitLogWriter; the
        // cast-and-skip shape keeps this hosted service safe to register
        // unconditionally in AddLattice without requiring every
        // ICommitLogWriter consumer to implement the drain.
        if (writer is WalCommitLogWriter walWriter)
        {
            logger.LogDebug("Draining WalCommitLogWriter on host stop.");
            return walWriter.DrainAsync(cancellationToken);
        }
        return Task.CompletedTask;
    }
}
