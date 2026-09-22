namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// (#3348) Internal partition-scoped extension of the saturation
/// signal, consumed only by the writer's own pre-admission gate.
/// <para>
/// The public <see cref="IWalSaturationSignal"/> verdict is a worst
/// case across every WAL partition of a tree, so a single partition at
/// its admission cap reports the whole tree saturated. That is the
/// correct shape for a tree-wide consumer - replication flow control,
/// autoscaling pressure, dashboards - all of which want to know that
/// <i>some</i> part of the tree is in trouble. It is the wrong shape
/// for <see cref="WalCommitLogWriter"/>'s gate, which refuses one
/// specific append to one specific partition: gating an idle partition
/// on a busy sibling converts a single hot partition into a tree-wide
/// write stall. With per-partition busy probability <c>p</c> over
/// <c>B</c> partitions the tree reads saturated with probability
/// <c>1-(1-p)^B</c>, which crosses its knee as offered load rises and
/// is the direct cause of the multi-silo <c>set-many</c> collapse.
/// </para>
/// <para>
/// Causes that are genuinely tree-wide - dispatch-timeout trips,
/// provider failures, sustained flush latency, materialiser drain lag,
/// pin latency - are re-applied to every partition by the sampler, so
/// narrowing the gate loses none of them. Only the admission-depth and
/// parked-caller inputs become partition-local.
/// </para>
/// <para>
/// <b>Why this is deliberately internal and separate rather than a
/// default interface method on <see cref="IWalSaturationSignal"/>.</b>
/// A dynamic proxy - a mocking framework, or a DI interception
/// decorator - implements every member of an interface it stands in
/// for, including default interface methods, and a generated stub
/// returns <c>default</c>. Because
/// <see cref="WalSaturationState.Healthy"/> is <c>0</c>, that stub
/// would report every partition healthy and silently remove WAL
/// back-pressure altogether: a fail-<i>open</i> admission gate. Keeping
/// the partition-scoped surface on a separate internal interface means
/// any implementation that does not provide it - proxied, foreign, or
/// simply older - fails the type test and falls back to the tree-wide
/// gate, which is strictly more restrictive and byte-identical to the
/// pre-#3348 behaviour. The fallback is therefore fail-closed by
/// construction, and the public API is unchanged.
/// </para>
/// </summary>
internal interface IWalPartitionSaturationSignal
{
    /// <summary>
    /// Returns the most recent saturation state observed for a single
    /// WAL partition of <paramref name="treeId"/>. Returns
    /// <see cref="WalSaturationState.Healthy"/> when the sampler has
    /// not yet observed the partition.
    /// </summary>
    /// <param name="treeId">The logical tree id to query.</param>
    /// <param name="partition">The WAL writer partition to query.</param>
    /// <returns>The most recent observed saturation state for the partition.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="treeId"/> is <c>null</c>.</exception>
    WalSaturationState GetCurrentState(string treeId, int partition);

    /// <summary>
    /// Asynchronously waits until a single WAL partition of
    /// <paramref name="treeId"/> returns to
    /// <see cref="WalSaturationState.Healthy"/>. Returns immediately
    /// when the partition is already healthy; otherwise the awaiter
    /// completes on the next sample tick that observes it healthy.
    /// </summary>
    /// <param name="treeId">The logical tree id to wait on.</param>
    /// <param name="partition">The WAL writer partition to wait on.</param>
    /// <param name="cancellationToken">Cancels the wait. A cancelled
    /// wait throws <see cref="OperationCanceledException"/>.</param>
    /// <returns>A task that completes when the partition is observed
    /// <see cref="WalSaturationState.Healthy"/>.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="treeId"/> is <c>null</c>.</exception>
    /// <exception cref="OperationCanceledException">Thrown if <paramref name="cancellationToken"/> is cancelled before recovery.</exception>
    Task WaitForHealthyAsync(string treeId, int partition, CancellationToken cancellationToken = default);
}
