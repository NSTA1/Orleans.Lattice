namespace Orleans.Lattice;

/// <summary>
/// Which admission seam refused an operation with a
/// <see cref="LatticeSaturatedException"/>. Several independent seams raise the
/// same exception type because they share one operational meaning - "this
/// tree's storage layer is back-pressured; the operation was refused" - and a
/// second type carrying the same caller contract would only fragment the catch
/// sites that already honour it.
/// <para>
/// The shared type is right for the <em>contract</em> and insufficient for
/// <em>policy</em>, which is why this discriminator exists. The seams differ on
/// the one question an automatic retry has to answer - whether the refused
/// caller did any work before it was refused - and they differ in opposite
/// directions:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="ReplayPermitAdmission"/> refuses
///   <b>before</b> the caller waits for anything or touches storage, so a
///   retry costs one more admission test against a queue that may since have
///   drained.</description></item>
///   <item><description>Every other member refuses <b>after</b> a wait budget
///   has already elapsed against a tree that has just reported it is full, so a
///   retry re-offers the same work into the regime that refused it and the
///   feedback is positive.</description></item>
/// </list>
/// <para>
/// Retrying the wrong member is therefore not merely wasteful, it amplifies:
/// issue #3348 was precisely a generic handler treating a
/// <see cref="WalAdmission"/> refusal as retryable and re-fanning an entire
/// batch across every shard of a saturated tree. A caller choosing a retry
/// policy must branch on this value rather than on the exception type alone.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeSaturationSource)]
public enum LatticeSaturationSource
{
    /// <summary>
    /// The refusing seam was not recorded. Carried by the constructors that
    /// take no source - including the framework-contract parameterless
    /// overload - and by any exception deserialised from a host that predates
    /// source attribution. Treat as <b>not</b> automatically retryable: it is
    /// the conservative reading, because three of the four known seams are
    /// amplifying to retry and an unattributed refusal could be any of them.
    /// </summary>
    Unspecified = 0,

    /// <summary>
    /// The writer-side admission refusal from
    /// <c>WalCommitLogWriter.PartitionTracker.AcquireAsync</c>, raised when the
    /// per-tree <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>
    /// elapses with the per-tree saturation signal still reporting
    /// <see cref="WalSaturationState.Saturated"/>.
    /// <para>
    /// <b>Not automatically retryable.</b> The wait budget has already been
    /// spent, so a retry is a second full attempt at work the tree just
    /// declined. On a batch write it re-offers every entry across every shard;
    /// see issue #3348.
    /// </para>
    /// </summary>
    WalAdmission = 1,

    /// <summary>
    /// The atomic-write saga coordinator's caller-facing refusal, raised when
    /// <c>AtomicWriteGrain.QuiesceOnSaturatedAsync</c> exhausts its quiesce
    /// budget and the saga declines to dispatch into a still-saturated tree.
    /// <para>
    /// <b>Not automatically retryable.</b> The saga refuses precisely to avoid
    /// re-issuing the same RowKeys into a back-pressured storage account, which
    /// amplifies the 409-Conflict burst. An automatic retry would reinstate the
    /// amplification the refusal exists to prevent.
    /// </para>
    /// </summary>
    AtomicWriteSaga = 2,

    /// <summary>
    /// The snapshot-cursor read-admission refusal from
    /// <c>LatticeGrain.OpenSnapshotCursorAsync</c>, raised when
    /// <see cref="LatticeOptions.ShedSnapshotOpensWhenSaturated"/> is enabled
    /// and the tree is saturated at the open.
    /// <para>
    /// <b>Not automatically retryable.</b> This is deliberate load shedding of
    /// a heavy per-shard baseline capture; retrying it automatically defeats
    /// the shed and re-fans the scan storm onto shard roots already collapsing
    /// under write back-pressure.
    /// </para>
    /// </summary>
    SnapshotCursorOpen = 3,

    /// <summary>
    /// The WAL replay permit admission refusal from
    /// <c>BPlusLeafGrain.AcquireReplayPermitAsync</c>, raised when the admitted
    /// waiter count already meets the bound derived from
    /// <see cref="LatticeOptions.WalReplayPermitQueueDepthPerPermit"/> (issue
    /// #3284) <b>and</b> the queue is failing to drain within
    /// <see cref="LatticeOptions.WalReplayPermitMaxQueueWait"/> (issue #3290).
    /// <para>
    /// <b>Automatically retryable, and it is the only member that is.</b> The
    /// refusal is raised before the activation waits for anything, so the
    /// refused caller has done no work and holds no permit: a retry costs one
    /// further admission test rather than a repeat of the operation. It is also
    /// the member that most needs one, because the refusal aborts a grain
    /// <em>activation</em>, and an activation that fails has no queue to park
    /// on and no interposed policy of its own - without a caller-side retry the
    /// bound does not shed the request, it fails it (issue #3294).
    /// </para>
    /// <para>
    /// A retry must be bounded and jittered. Refusals at this seam are
    /// correlated by construction - the callers were refused by one gate at one
    /// moment - so an unjittered retry re-converges them into the thundering
    /// herd that issue #3284 removed.
    /// </para>
    /// </summary>
    ReplayPermitAdmission = 4,
}
