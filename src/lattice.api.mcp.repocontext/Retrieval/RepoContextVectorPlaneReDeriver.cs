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
/// <para>
/// <b>System origin.</b> <c>DeleteTreeAsync</c> is a privileged tree-lifecycle
/// operation, so the access gate evaluates it against the ambient subject. This reset
/// is not caller-initiated: it is infrastructure-authored maintenance with no user
/// identity behind it, so evaluating it against whatever subject happened to be
/// ambient when the fall-off surfaced is the wrong question, and widening the gate's
/// default to admit it would be a security regression rather than a fix. The reset
/// therefore runs inside a <see cref="LatticeSystemOrigin"/> scope, the public seam
/// the core library documents for a co-hosted in-silo infrastructure extension. The
/// bypass is bounded by the fail-closed allow-list that precedes it (a store-of-record
/// tree can never reach the scope), by the tree name being a local layout constant
/// rather than any wire- or exception-derived value, by containing nothing but the
/// delete/purge of those rebuildable projections, and by being lexically scoped and
/// disposed so it never leaks into a caller's turn.
/// </para>
/// <para>
/// <b>Bounded retry.</b> A reset that fails is not re-attempted on the observation
/// cadence. Each tree carries a per-tree backoff that doubles up to a cap and is
/// cleared by a completed reset, and an observation inside that window is metered
/// <c>suppressed</c> and logged at debug rather than retried. An access-gate denial
/// gets its own, much longer, schedule and its own <c>denied</c> outcome: unlike a
/// transient failure it is a deterministic decision that cannot clear on retry, so
/// retrying it at observation cadence only produces identical refusals that bury the
/// underlying data-loss signal.
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

    /// <summary>Outcome tag value: a tree's reset failed for a non-authorization reason and the fault stands for the next pass to retry.</summary>
    internal const string OutcomeFailed = "failed";

    /// <summary>
    /// Outcome tag value: a tree's reset was refused by the access gate. Partitioned
    /// away from <see cref="OutcomeFailed"/> because the two have opposite remedies -
    /// a denial is deterministic and cannot clear on retry (it needs the host's
    /// authorization posture changing, or the maintenance path establishing a
    /// system origin), whereas a generic failure is usually transient.
    /// </summary>
    internal const string OutcomeDenied = "denied";

    /// <summary>
    /// Outcome tag value: a fall-off was observed on an allowlisted tree but no reset
    /// was attempted because the tree is inside its post-failure backoff window. This
    /// is what keeps a permanently-failing reset from retrying on the observation
    /// cadence, and it keeps <see cref="OutcomeObserved"/> meaning exactly "a reset
    /// was triggered" rather than "a fall-off was seen".
    /// </summary>
    internal const string OutcomeSuppressed = "suppressed";

    /// <summary>The first backoff delay after a reset that failed for a transient (non-authorization) reason.</summary>
    internal static readonly TimeSpan TransientBackoffBase = TimeSpan.FromSeconds(30);

    /// <summary>The ceiling the transient backoff doubles up to.</summary>
    internal static readonly TimeSpan TransientBackoffCap = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The first backoff delay after a reset the access gate denied. Deliberately far
    /// longer than <see cref="TransientBackoffCap"/> - not merely than
    /// <see cref="TransientBackoffBase"/>, so the two schedules never overlap: a
    /// denial is a deterministic decision about a subject and an operation, so
    /// retrying it on the transient cadence produces identical refusals that drown out
    /// the data-loss signal underneath them without ever converging.
    /// </summary>
    internal static readonly TimeSpan DeniedBackoffBase = TimeSpan.FromMinutes(15);

    /// <summary>The ceiling the denied backoff doubles up to.</summary>
    internal static readonly TimeSpan DeniedBackoffCap = TimeSpan.FromHours(1);

    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<RepoContextVectorPlaneReDeriver> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly Meter _meter;
    private readonly Counter<long> _counter;

    // Single-flight per tree: while a reset is in flight its task lives here, so a
    // concurrent observer awaits the same reset rather than starting a second one.
    // An entry is removed once its reset settles, so a fresh fall-off after a
    // completed reset starts a new one (idempotent, not permanently suppressed).
    private readonly ConcurrentDictionary<string, Task> _inFlight =
        new(StringComparer.Ordinal);

    // Per-tree backoff after a failed reset. An entry is cleared on a completed reset,
    // so the backoff only ever throttles a tree that is actually failing.
    private readonly ConcurrentDictionary<string, BackoffState> _backoff =
        new(StringComparer.Ordinal);

    /// <summary>Creates the vector-plane re-deriver.</summary>
    /// <param name="grainFactory">The grain factory used to reach the faulting tree for reset. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the originating fault and remediation are recorded on. Must not be <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock the post-failure backoff window is measured on, or <see langword="null"/> for <see cref="TimeProvider.System"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextVectorPlaneReDeriver(
        IGrainFactory grainFactory,
        ILogger<RepoContextVectorPlaneReDeriver> logger,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logger);
        _grainFactory = grainFactory;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;

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
            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeRefused), LatticeTenantLabel.ForTree(treeName));
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

        // Bounded retry. A reset that has already failed is not re-attempted on the
        // observation cadence: an authorization denial in particular is deterministic
        // and cannot clear on retry, so hammering it produces identical refusals that
        // bury the fall-off signal underneath them. The observation itself is still
        // logged and metered above, so a suppressed pass is visible rather than silent.
        if (_backoff.TryGetValue(treeName, out var backoff)
            && _timeProvider.GetUtcNow() < backoff.NextAttemptUtc)
        {
            _logger.LogDebug(
                "Repo-context vector plane: tree {Tree} fall-off observed inside the post-failure backoff " +
                "window after {Failures} consecutive failed reset(s); the next reset is not attempted " +
                "before {NextAttempt:O}.",
                treeName,
                backoff.ConsecutiveFailures,
                backoff.NextAttemptUtc);
            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeSuppressed), LatticeTenantLabel.ForTree(treeName));
            return Task.CompletedTask;
        }

        _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeObserved), LatticeTenantLabel.ForTree(treeName));

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
            // This reset is infrastructure-authored maintenance, not a caller-initiated
            // deletion, so it runs under a system-origin scope: the access gate is not
            // the right place to decide it, and the gate's own default must not be
            // widened to admit it (that would trade this defect for a real security
            // regression). LatticeSystemOrigin is the public seam the core library
            // documents for exactly this - a co-hosted, in-silo infrastructure
            // extension performing a scoped operation with no user identity behind it.
            //
            // WHAT BOUNDS THE BYPASS, since DeleteTreeAsync is destructive:
            //  * the scope is entered only AFTER the fail-closed allow-list above has
            //    accepted the tree, so a store-of-record tree can never reach it;
            //  * the tree name is the local layout constant the write/probe seam
            //    already held - never parsed from the exception text, a wire value, or
            //    caller input - so the bypass cannot be steered onto another tree;
            //  * the only operations inside the scope are the delete/purge of those
            //    two rebuildable derived projections and the coverage digest that
            //    mirrors them, all of which the ingest path re-derives;
            //  * the scope is lexical and disposed on every path, restoring the prior
            //    ambient value, so it never leaks into a caller's turn.
            using var systemOrigin = LatticeSystemOrigin.Enter();

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

            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeCompleted), LatticeTenantLabel.ForTree(treeName));

            // A completed reset clears the backoff, so the throttle only ever applies
            // to a tree that is actually failing.
            _backoff.TryRemove(treeName, out _);

            _logger.LogInformation(
                "Repo-context vector plane: tree {Tree} re-derivation reset completed; the always-on gap " +
                "scanner re-embeds every uncovered source from the store-of-record trees and files.",
                treeName);

            // Resetting membership invalidates the coverage digest that mirrors it
            // (issue #2486), and this is the ONE direction the digest's safety argument
            // does not already cover. Every other path leaves the digest a SUBSET of
            // membership, which under-reports and costs a redundant idempotent embed. An
            // emptied membership tree under a surviving digest is the inverse: the digest
            // becomes a strict SUPERSET, so it reports coverage that no longer exists and
            // masks a repository-wide gap silently and permanently. Cascading the reset
            // is what keeps the subset invariant true across a self-heal.
            if (string.Equals(treeName, RepoContextTrees.VectorMembership, StringComparison.Ordinal))
            {
                await ResetCoverageDigestAsync().ConfigureAwait(false);
            }
        }
        catch (LatticeAuthorizationDeniedException denied)
        {
            // Partitioned away from the generic failure arm because the remedy is
            // different: a denial is a deterministic decision about (subject, tree,
            // operation) and re-attempting it changes nothing, so it gets its own
            // outcome value and its own, much longer, backoff schedule. Reaching this
            // arm at all means the system-origin scope above did not take effect for
            // this operation, which is an authorization-posture fault in the host and
            // is worth an operator's attention rather than a silent retry loop.
            var next = RecordFailure(treeName, DeniedBackoffBase, DeniedBackoffCap);
            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeDenied), LatticeTenantLabel.ForTree(treeName));
            _logger.LogError(
                denied,
                "Repo-context vector plane: tree {Tree} re-derivation reset was denied by the access gate " +
                "(subject {Subject}, operation {Operation}); this is deterministic and will not clear on " +
                "retry, so further resets are suppressed until {NextAttempt:O}. The tree stays terminally " +
                "stale until the host's authorization posture admits the maintenance path.",
                treeName,
                denied.SubjectId,
                denied.Operation,
                next);
        }
        catch (Exception ex)
        {
            var next = RecordFailure(treeName, TransientBackoffBase, TransientBackoffCap);
            _counter.Add(1, new(TreeTagKey, treeName), new(OutcomeTagKey, OutcomeFailed), LatticeTenantLabel.ForTree(treeName));
            _logger.LogError(
                ex,
                "Repo-context vector plane: tree {Tree} re-derivation reset failed; the fault stands and " +
                "a pass after {NextAttempt:O} re-observes and retries.",
                treeName,
                next);
        }
        finally
        {
            // Clear the in-flight (degraded) signal so a future fall-off triggers a
            // fresh reset. Single-flight is about concurrent duplicates, not permanent
            // suppression.
            _inFlight.TryRemove(treeName, out _);
        }
    }

    /// <summary>
    /// Drops the vector-coverage digest tree after a membership reset, so a digest
    /// derived from the emptied membership cannot survive it and over-report coverage
    /// (issue #2486). Best-effort and self-contained: the digest is a rebuildable
    /// accelerator, so a failure here is logged and swallowed rather than failing the
    /// membership reset that has already succeeded. A digest that is not dropped is
    /// still repaired by the periodic exhaustive audit, so this narrows the window
    /// rather than being the only defence.
    /// </summary>
    private async Task ResetCoverageDigestAsync()
    {
        try
        {
            var digest = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorCoverage);
            await digest.DeleteTreeAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await digest.PurgeTreeAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (LeafProjectionStaleException)
            {
                // The soft-delete's reminder-driven purge completes the reclaim.
            }

            _logger.LogInformation(
                "Repo-context vector plane: dropped tree {Tree} alongside the membership reset so the " +
                "coverage digest cannot outlive the membership it mirrors.",
                RepoContextTrees.VectorCoverage);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Repo-context vector plane: could not drop tree {Tree} after the membership reset. The " +
                "digest may over-report coverage until the periodic exhaustive audit re-derives it.",
                RepoContextTrees.VectorCoverage);
        }
    }

    /// <summary>
    /// Records a failed reset against the tree's backoff state and returns the instant
    /// before which no further reset is attempted. The delay starts at
    /// <paramref name="baseDelay"/> and doubles per consecutive failure up to
    /// <paramref name="cap"/>, so a tree whose reset keeps failing is re-attempted on a
    /// decaying cadence rather than on every observation.
    /// </summary>
    private DateTimeOffset RecordFailure(string treeName, TimeSpan baseDelay, TimeSpan cap)
    {
        var now = _timeProvider.GetUtcNow();
        var state = _backoff.AddOrUpdate(
            treeName,
            _ => new BackoffState(1, now + baseDelay),
            (_, existing) =>
            {
                var failures = existing.ConsecutiveFailures + 1;

                // Double per consecutive failure, clamped to the cap. The shift is
                // bounded before it is taken so a long-lived failing tree cannot
                // overflow the multiplier.
                var shift = Math.Min(failures - 1, 16);
                var ticks = Math.Min(baseDelay.Ticks * (1L << shift), cap.Ticks);
                return new BackoffState(failures, now + TimeSpan.FromTicks(ticks));
            });

        return state.NextAttemptUtc;
    }

    /// <summary>The per-tree post-failure backoff window.</summary>
    /// <param name="ConsecutiveFailures">How many resets have failed in a row without an intervening success.</param>
    /// <param name="NextAttemptUtc">The instant before which no further reset is attempted for the tree.</param>
    private sealed record BackoffState(int ConsecutiveFailures, DateTimeOffset NextAttemptUtc);

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
