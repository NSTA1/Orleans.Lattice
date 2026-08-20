using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context self-healer for the rebuildable derived vector-plane
/// trees. When a vector tree falls terminally off its write-ahead log - its durable
/// projection checkpoint was trimmed with no covering snapshot, surfaced on leaf
/// activation as <see cref="LeafProjectionStaleException"/> - the affected tree can
/// never activate again and every ingest write and gap-scan probe against it spins
/// in a permanent failing state. This re-deriver breaks that terminal state
/// <b>without masking the fault</b>: it always logs the originating exception with
/// its full stack trace and increments a dedicated telemetry counter <b>before</b>
/// any remediation runs, then triggers a bounded, single-flight, idempotent reset of
/// that one tree so the always-on gap scanner and ingest re-embed every uncovered
/// source from the store-of-record structural, symbol, and memory trees plus the
/// working files.
/// <para>
/// <b>Fail-closed.</b> Re-derivation applies only to the two rebuildable vector
/// projections (<see cref="RepoContextTrees.VectorMetadata"/> and
/// <see cref="RepoContextTrees.VectorMembership"/>), classified through the single
/// authoritative <see cref="RepoContextTrees.IsRebuildableVectorTree(string?)"/>
/// allow-list. Every other tree - a store-of-record structural, symbol, or memory
/// tree (real data loss) or the write-once content-addressed
/// <see cref="RepoContextTrees.VectorPayload"/> tree (no in-place deletes, cannot be
/// re-derived by a drop-and-re-embed) - is refused: the fault is still surfaced
/// (logged and metered) but never auto-reset. The tree the re-derivation targets is
/// always the local layout constant the caller already holds at the write/probe
/// seam, never a value parsed from wire- or exception-supplied text.
/// </para>
/// <para>
/// <b>Reset primitive.</b> The reset issues <see cref="ILattice.DeleteTreeAsync"/>
/// then <see cref="ILattice.PurgeTreeAsync"/> on the faulting tree.
/// <c>DeleteTreeAsync</c> is the one public primitive that reaches a terminally-stale
/// tree purely through shard-root state (it marks every shard root deleted and never
/// activates the throwing leaf), so it makes progress where the leaf-activating
/// primitives (<c>RebuildLeafProjectionAsync</c>, <c>RecoverTreeAsync</c>) only
/// re-throw. <c>PurgeTreeAsync</c> then requests immediate reclaim so a subsequent
/// ingest re-derives a clean tree; if the immediate purge itself trips the terminal
/// leaf while walking the chain, the delete's registered reminder-driven purge
/// completes the reclaim out of band, so the reset is best-effort and never itself
/// throws out of the remediation.
/// </para>
/// </summary>
internal sealed class RepoContextVectorPlaneReDeriver : IDisposable
{
    /// <summary>The counter name incremented once per observed or remediated vector-plane fall-off.</summary>
    internal const string ReDeriveInstrumentName = "repocontext.vectorplane.rederive";

    /// <summary>The low-cardinality tag key carrying the affected tree name.</summary>
    internal const string TreeTagKey = "tree";

    /// <summary>The low-cardinality tag key carrying the remediation outcome.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>Outcome tag value: an allowlisted fall-off was observed and a reset was triggered.</summary>
    internal const string OutcomeObserved = "observed";

    /// <summary>Outcome tag value: a non-rebuildable tree's fall-off was surfaced but refused (fail-closed).</summary>
    internal const string OutcomeRefused = "refused";

    /// <summary>Outcome tag value: a tree's reset completed.</summary>
    internal const string OutcomeCompleted = "completed";

    /// <summary>Outcome tag value: a tree's reset failed and the fault stands for the next pass to retry.</summary>
    internal const string OutcomeFailed = "failed";

    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<RepoContextVectorPlaneReDeriver> _logger;
    private readonly Meter _meter;
    private readonly Counter<long> _counter;

    // Single-flight per tree: while a reset is in flight its task lives here, so a
    // concurrent observer awaits the same reset rather than starting a second one.
    // An entry is removed once its reset settles, so a fresh fall-off after a
    // completed reset starts a new one (idempotent, not permanently suppressed).
    private readonly ConcurrentDictionary<string, Task> _inFlight =
        new(StringComparer.Ordinal);

    /// <summary>Creates the vector-plane re-deriver.</summary>
    /// <param name="grainFactory">The grain factory used to reach the faulting tree for reset. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the originating fault and remediation are recorded on. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextVectorPlaneReDeriver(
        IGrainFactory grainFactory,
        ILogger<RepoContextVectorPlaneReDeriver> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logger);
        _grainFactory = grainFactory;
        _logger = logger;

        // Publish under the same meter name as the rest of the repocontext surface so
        // a single scraper subscription covers it.
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _counter = _meter.CreateCounter<long>(
            ReDeriveInstrumentName,
            unit: "{event}",
            description: "Rebuildable vector-plane tree fall-off observations and re-derivations, tagged by tree and outcome.");
    }

    /// <summary>
    /// Runs <paramref name="operation"/> against the named vector tree and, if it
    /// surfaces a terminal <see cref="LeafProjectionStaleException"/>, records the
    /// fault and triggers a bounded single-flight re-derivation of
    /// <paramref name="treeName"/> before re-throwing the originating fault. The
    /// fault is never masked: it always propagates so the current pass fails and the
    /// always-on next pass re-embeds once the reset has dropped the terminal tree.
    /// </summary>
    /// <param name="treeName">The vector tree the operation targets - a local layout constant. Must not be <see langword="null"/>.</param>
    /// <param name="operation">The tree operation to run. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the awaited remediation, never the reset itself.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeName"/> or <paramref name="operation"/> is null.</exception>
    public async Task GuardAsync(string treeName, Func<Task> operation, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(operation);

        try
        {
            await operation().ConfigureAwait(false);
        }
        catch (LeafProjectionStaleException stale)
        {
            await ObserveAndReDeriveAsync(treeName, stale, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// The value-returning overload of
    /// <see cref="GuardAsync(string, Func{Task}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">The operation's result type.</typeparam>
    /// <param name="treeName">The vector tree the operation targets - a local layout constant. Must not be <see langword="null"/>.</param>
    /// <param name="operation">The tree operation to run. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the awaited remediation, never the reset itself.</param>
    /// <returns>The operation's result when it does not fall off the log.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeName"/> or <paramref name="operation"/> is null.</exception>
    public async Task<T> GuardAsync<T>(string treeName, Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(operation);

        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (LeafProjectionStaleException stale)
        {
            await ObserveAndReDeriveAsync(treeName, stale, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Records an observed terminal fall-off of <paramref name="treeName"/> and, when
    /// the tree is a rebuildable vector projection, triggers its bounded single-flight
    /// reset. The originating <paramref name="stale"/> is logged with its full stack
    /// trace and metered before any remediation. Returns the in-flight reset task so a
    /// caller (or a test) can await convergence; a refused (non-rebuildable) tree
    /// returns a completed task without resetting anything.
    /// </summary>
    /// <param name="treeName">The tree that surfaced the fault - a local layout constant. Must not be <see langword="null"/>.</param>
    /// <param name="stale">The originating fall-off exception. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Unused by the reset itself (a reset always runs to completion); accepted for call-site symmetry.</param>
    /// <returns>The single-flight reset task for the tree, or a completed task when the tree is refused.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeName"/> or <paramref name="stale"/> is null.</exception>
    internal Task ObserveAndReDeriveAsync(
        string treeName, LeafProjectionStaleException stale, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(stale);
        _ = cancellationToken;

        // Fail-closed classification against local constants only. A store-of-record
        // tree, the write-once payload tree, or any unknown name is refused: the fault
        // is still surfaced (logged with its stack trace and metered) but never
        // auto-reset, because resetting a primary tree would be real data loss.
        if (!RepoContextTrees.IsRebuildableVectorTree(treeName))
        {
            _logger.LogWarning(
                stale,
                "Repo-context vector plane: tree {Tree} surfaced a terminal stale-projection fault but is " +
                "not a rebuildable derived vector tree; refusing auto re-derivation (fail-closed) and " +
                "propagating the fault.",
                treeName);
            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeRefused));
            return Task.CompletedTask;
        }

        // No masking: the originating fault is logged with its full stack trace and
        // metered BEFORE any remediation runs.
        _logger.LogWarning(
            stale,
            "Repo-context vector plane: rebuildable tree {Tree} fell terminally off its write-ahead log " +
            "(durable projection checkpoint trimmed with no covering snapshot). Auto re-deriving the tree " +
            "from the store-of-record sources; the always-on gap scanner re-embeds every uncovered source.",
            treeName);
        _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeObserved));

        // Single-flight per tree: a re-derivation already in flight is a no-op; a
        // concurrent observer awaits the same reset task.
        return _inFlight.GetOrAdd(treeName, name => ResetAsync(name));
    }

    private async Task ResetAsync(string treeName)
    {
        // Yield so the GetOrAdd factory hands the task back to the registering caller
        // before the reset body runs, keeping the single-flight registration race-free.
        await Task.Yield();

        try
        {
            var tree = _grainFactory.GetGrain<ILattice>(treeName);

            // DeleteTreeAsync marks every shard root deleted via shard-root state alone
            // (no leaf activation), so it is the one public primitive that makes
            // progress on a tree whose leaf is terminally un-activatable. It is
            // idempotent and registers a reminder-driven purge. Run the reset under
            // CancellationToken.None so a cancelled observing request never leaves the
            // tree half-reset.
            await tree.DeleteTreeAsync(CancellationToken.None).ConfigureAwait(false);

            // Request immediate reclaim so the next ingest re-derives a clean tree. If
            // the immediate purge trips the terminal leaf while walking the chain, the
            // delete's registered reminder purge completes the reclaim out of band - the
            // delete has already unblocked the terminal state, so this is best-effort.
            try
            {
                await tree.PurgeTreeAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (LeafProjectionStaleException purgeStale)
            {
                _logger.LogWarning(
                    purgeStale,
                    "Repo-context vector plane: immediate purge of tree {Tree} tripped the terminal leaf; " +
                    "the soft-delete's reminder-driven purge will complete the reclaim out of band.",
                    treeName);
            }

            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeCompleted));
            _logger.LogInformation(
                "Repo-context vector plane: tree {Tree} re-derivation reset completed; the always-on gap " +
                "scanner re-embeds every uncovered source from the store-of-record trees and files.",
                treeName);
        }
        catch (Exception ex)
        {
            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeFailed));
            _logger.LogError(
                ex,
                "Repo-context vector plane: tree {Tree} re-derivation reset failed; the fault stands and " +
                "the next always-on pass re-observes and retries.",
                treeName);
        }
        finally
        {
            // Clear the in-flight (degraded) signal so a future fall-off triggers a
            // fresh reset. Single-flight is about concurrent duplicates, not permanent
            // suppression.
            _inFlight.TryRemove(treeName, out _);
        }
    }

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
