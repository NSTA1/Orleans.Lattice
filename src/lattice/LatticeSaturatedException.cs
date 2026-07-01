namespace Orleans.Lattice;

/// <summary>
/// Thrown by the WAL writer admission gate, and by the atomic-write
/// saga coordinator, when an operation cannot complete because the
/// per-tree saturation signal reported
/// <see cref="WalSaturationState.Saturated"/> for longer than the
/// caller's configured wait budget. Distinct from
/// <see cref="LatticeShuttingDownException"/>: saturation is a
/// recoverable steady-state regime (offered load is exceeding the
/// storage layer's sustained drain rate), not a one-way silo
/// shutdown.
/// <para>
/// <b>Caller contract.</b> A caller observing this exception should
/// retry the operation after backing off (typical recovery is
/// 1-10 seconds, until the underlying storage account or per-partition
/// WAL admission gate drains). Long-lived consumers should also
/// reduce offered load on the affected tree until the per-tree
/// <see cref="IWalSaturationSignal"/> reports
/// <see cref="WalSaturationState.Healthy"/> again. Unlike
/// <see cref="LatticeShuttingDownException"/>, retries against the
/// same silo activation can succeed once the regime clears.
/// </para>
/// <para>
/// <b>Sources.</b> Surfaces from three distinct saturation failure
/// shapes that share the same operational meaning ("this tree's
/// storage layer is back-pressured; the operation was refused"):
/// </para>
/// <list type="bullet">
///   <item><description>The writer-side admission refusal from
///   <c>WalCommitLogWriter.PartitionTracker.AcquireAsync</c>, raised
///   when the per-tree <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>
///   elapses with the per-tree saturation signal still reporting
///   <see cref="WalSaturationState.Saturated"/>.</description></item>
///   <item><description>The saga coordinator's caller-facing throw
///   path, raised when <c>AtomicWriteGrain.QuiesceOnSaturatedAsync</c>
///   exhausts its quiesce budget (bounded by
///   <see cref="LatticeOptions.WalAppendDispatchTimeout"/>) and the
///   saga refuses to dispatch into a still-saturated tree rather
///   than re-issuing the same RowKeys into a back-pressured storage
///   account, which would amplify the 409-Conflict burst.</description></item>
///   <item><description>The snapshot-cursor read-admission refusal
///   from <c>LatticeGrain.OpenSnapshotCursorAsync</c>, raised when
///   <see cref="LatticeOptions.ShedSnapshotOpensWhenSaturated"/> is
///   enabled and the per-tree saturation signal reports
///   <see cref="WalSaturationState.Saturated"/> at the open, so the
///   heavy per-shard baseline capture is shed before it fans out onto
///   shard roots already collapsing under write back-pressure (the
///   Explorer-driven scan storm documented in issue #1053).</description></item>
/// </list>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so
/// existing catch handlers that match on
/// <see cref="System.InvalidOperationException"/> continue to absorb
/// it; the typed slot lets callers that care about the saturation
/// regime explicitly distinguish it from generic
/// <see cref="System.InvalidOperationException"/> failures (which
/// are not back-pressure and should be handled per the caller's
/// normal policy).
/// </para>
/// <para>
/// Carries the originating <see cref="TreeId"/> so caller-side
/// diagnostics can attribute the back-pressure to the specific tree
/// without parsing the exception message.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeSaturated)]
public sealed class LatticeSaturatedException : InvalidOperationException
{
    /// <summary>
    /// Logical tree id whose saturation regime caused the refusal.
    /// Empty on the parameterless constructor; populated on the
    /// production overloads so caller-side diagnostics can attribute
    /// the back-pressure without parsing the exception message.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an
    /// empty <see cref="TreeId"/>. Provided to satisfy the framework's
    /// exception construction contract; production throw sites use
    /// the overloads that carry diagnostic context.
    /// </summary>
    public LatticeSaturatedException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic
    /// message and an empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which operation was refused and why.</param>
    public LatticeSaturatedException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic
    /// message and wrapped inner exception, and an empty
    /// <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Diagnostic context describing which operation was refused and why.</param>
    /// <param name="innerException">The underlying cause (admission deadline, saga quiesce timeout, or another saturated-regime indicator).</param>
    public LatticeSaturatedException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic
    /// message and originating tree id. The primary production throw
    /// shape; preferred over the message-only overload so caller-side
    /// diagnostics can attribute the back-pressure to a specific
    /// tree without parsing the exception message.
    /// </summary>
    /// <param name="message">Diagnostic context describing which operation was refused and why.</param>
    /// <param name="treeId">Logical tree id whose saturation regime caused the refusal.</param>
    public LatticeSaturatedException(string message, string treeId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic
    /// message, originating tree id, and wrapped inner exception.
    /// Production throw sites that observed the saturation regime
    /// through an underlying exception (e.g. the writer's drain
    /// release converting to a saturation refusal) use this
    /// overload to preserve the inner cause for log diagnostics.
    /// </summary>
    /// <param name="message">Diagnostic context describing which operation was refused and why.</param>
    /// <param name="treeId">Logical tree id whose saturation regime caused the refusal.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeSaturatedException(string message, string treeId, Exception innerException)
        : base(message, innerException)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
    }
}
