namespace Orleans.Lattice;

/// <summary>
/// Polling and await-able surface over the per-silo WAL saturation
/// signal. Callers reach this singleton from DI (it is registered by
/// <c>AddLattice</c>) and use it to check, query, or wait on the
/// current saturation regime of a tree without subscribing to the
/// push-style <see cref="IWalSaturationObserver"/> hook.
/// <para>
/// The signal is computed by an internal silo-scoped sampler that
/// waits <see cref="LatticeOptions.WalSaturationSampleInterval"/>
/// (default 200 ms) between sampling passes and reads the writer-side admission gate's per-
/// partition depth together with the sampler's other inputs (see
/// <see cref="WalSaturationCause"/>).
/// Runtime scheduling, sampling work and awaited observers can delay later
/// passes. The configured interval is not a wall-time transition deadline;
/// sustained-input windows and the recovery hold also affect classification.
/// </para>
/// <para>
/// <b>Idle cost.</b> <see cref="GetCurrentState"/> costs one
/// <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}"/>
/// lookup returning a <see cref="WalSaturationState"/> enum, and
/// <see cref="GetAggregateState"/> walks the states of the trees this silo
/// has observed, stopping at the first <see cref="WalSaturationState.Saturated"/>
/// one; neither fans out to grains. The sampler runs on its own timer. The
/// write path is not entirely free of it: every timestamped WAL append
/// advances an in-memory per-tree WAL-head reading that the drain-lag input
/// reads (one lock-free dictionary update). Except for terminal-only relief,
/// it also consults its partition's
/// verdict before admission - a dictionary lookup each for the
/// <see cref="WalSaturationState.Saturated"/> gate and the
/// <see cref="WalSaturationState.Throttled"/> pace while those are enabled.
/// An append to a Throttled partition is delayed by
/// <see cref="LatticeOptions.WalThrottledAdmissionPace"/>, and one to a
/// Saturated partition waits for it to recover, up to
/// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>, before it
/// is refused.
/// A point append or entire caller batch containing only transaction terminal
/// records bypasses those two signal checks, not admission capacity, storage
/// acknowledgement, deadlines, cancellation or shutdown.
/// </para>
/// </summary>
public interface IWalSaturationSignal
{
    /// <summary>
    /// Returns the most recent saturation state observed for
    /// <paramref name="treeId"/>. Returns <see cref="WalSaturationState.Healthy"/>
    /// when the sampler has not yet observed any signal for the tree
    /// (a freshly-started silo, or a tree whose WAL has not been
    /// exercised yet). The result reflects the state captured at the
    /// last sampling pass, not the instantaneous input. The configured
    /// <see cref="LatticeOptions.WalSaturationSampleInterval"/> is an inter-pass
    /// delay, not a worst-case observation latency.
    /// </summary>
    /// <param name="treeId">The WAL tree id to query.</param>
    /// <returns>The most recent observed saturation state for the tree.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="treeId"/> is <c>null</c>.</exception>
    WalSaturationState GetCurrentState(string treeId);

    /// <summary>
    /// Returns a tree-aggregated view across every tree the sampler
    /// has observed so far on this silo, taking the worst-case state
    /// across all trees. Suitable for a multi-tree silo whose
    /// back-pressure consumer wants a single global signal (for example
    /// a TCP listener that fronts every tree at once). Returns
    /// <see cref="WalSaturationState.Healthy"/> when no tree has been
    /// observed yet.
    /// </summary>
    /// <returns>The maximum (worst-case) state across every observed tree.</returns>
    WalSaturationState GetAggregateState();

    /// <summary>
    /// Asynchronously waits until <paramref name="treeId"/> returns to
    /// <see cref="WalSaturationState.Healthy"/>. Returns immediately
    /// when the tree is already <see cref="WalSaturationState.Healthy"/>
    /// (cheap synchronous fast-path). Otherwise eligible sampling passes release
    /// waiters oldest-first, subject to
    /// <see cref="LatticeOptions.WalSaturationRecoveryReleaseBatch"/>.
    /// A positive release batch can leave this waiter for a later Healthy pass.
    /// The recovery window can delay Healthy classification, and awaited
    /// observers and runtime scheduling can delay later passes. No unconditional
    /// wall-time completion bound follows from
    /// <see cref="LatticeOptions.WalSaturationSampleInterval"/>; use cancellation
    /// to bound the caller's wait.
    /// </summary>
    /// <param name="treeId">The WAL tree id to wait on.</param>
    /// <param name="cancellationToken">Cancels the wait. A cancelled
    /// wait throws <see cref="OperationCanceledException"/>.</param>
    /// <returns>A task that completes when the tree is observed
    /// <see cref="WalSaturationState.Healthy"/>.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="treeId"/> is <c>null</c>.</exception>
    /// <exception cref="OperationCanceledException">Thrown if <paramref name="cancellationToken"/> is cancelled before recovery.</exception>
    Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default);
}
