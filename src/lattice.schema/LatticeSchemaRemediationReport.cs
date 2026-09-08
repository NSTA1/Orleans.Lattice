namespace Orleans.Lattice.Schema;

/// <summary>
/// A durable, serializable snapshot of a tree's background schema-remediation
/// status: the current or terminal <see cref="Phase"/>, whether a build is in
/// flight, how many entries were scanned, and - on an abort - the first offending
/// key, the reason, and a bounded preview of the offending value. It is the
/// observable output of <see cref="ILatticeSchemaRemediationAdmin"/> and the
/// serialized sibling of the pure in-process
/// <see cref="LatticeSchemaRemediationOutcome"/>: where the outcome describes a
/// single dry-run, the report describes the whole coordinator's last known state
/// and survives a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaRemediationReport)]
[Immutable]
public readonly record struct LatticeSchemaRemediationReport
{
    /// <summary>The current (if in flight) or terminal phase of the remediation.</summary>
    [Id(0)] public LatticeSchemaRemediationPhase Phase { get; init; }

    /// <summary>Whether a remediation build is currently in flight for the tree.</summary>
    [Id(1)] public bool InProgress { get; init; }

    /// <summary>
    /// The number of entries scanned. On a completed remediation this is the whole
    /// tree; on an abort it is the count inspected up to and including the offender.
    /// </summary>
    [Id(2)] public int ScannedCount { get; init; }

    /// <summary>The first offending key on an abort, otherwise <c>null</c>.</summary>
    [Id(3)] public string? OffendingKey { get; init; }

    /// <summary>
    /// The reason the offending value failed on an abort (a policy-violation reason
    /// or a transform-failure message), otherwise <c>null</c>.
    /// </summary>
    [Id(4)] public string? Reason { get; init; }

    /// <summary>
    /// A bounded preview of the offending value's bytes on an abort (post-transform
    /// when the transform succeeded, otherwise the original), otherwise <c>null</c>.
    /// </summary>
    [Id(5)] public byte[]? OffendingValuePreview { get; init; }

    /// <summary>
    /// The destination physical tree id the shadow build targets, or <c>null</c>
    /// when the remediation is idle or aborted during the pre-build dry-run gate.
    /// </summary>
    [Id(6)] public string? DestinationTreeId { get; init; }

    /// <summary>The unique operation id of the in-flight or last remediation, or <c>null</c> when idle.</summary>
    [Id(7)] public string? OperationId { get; init; }

    /// <summary>Whether the remediation completed successfully and cut the tree over.</summary>
    public bool Succeeded => Phase == LatticeSchemaRemediationPhase.Completed;

    /// <summary>Whether the remediation aborted on an offending value with no cutover.</summary>
    public bool DidAbort => Phase == LatticeSchemaRemediationPhase.Aborted;

    /// <summary>The idle report for a tree that has never been remediated.</summary>
    public static LatticeSchemaRemediationReport Idle { get; } =
        new() { Phase = LatticeSchemaRemediationPhase.Idle };

    /// <summary>Creates an in-flight report for the given phase.</summary>
    /// <param name="phase">The phase currently executing.</param>
    /// <param name="scannedCount">Entries scanned so far (best-effort).</param>
    /// <param name="destinationTreeId">The destination physical tree id.</param>
    /// <param name="operationId">The operation id.</param>
    public static LatticeSchemaRemediationReport InFlight(
        LatticeSchemaRemediationPhase phase, int scannedCount, string? destinationTreeId, string? operationId) =>
        new()
        {
            Phase = phase,
            InProgress = true,
            ScannedCount = scannedCount,
            DestinationTreeId = destinationTreeId,
            OperationId = operationId,
        };

    /// <summary>Creates a successful terminal report.</summary>
    /// <param name="scannedCount">The number of entries remediated.</param>
    /// <param name="destinationTreeId">The destination physical tree the logical tree was cut over to.</param>
    /// <param name="operationId">The operation id.</param>
    public static LatticeSchemaRemediationReport Completed(
        int scannedCount, string destinationTreeId, string operationId) =>
        new()
        {
            Phase = LatticeSchemaRemediationPhase.Completed,
            InProgress = false,
            ScannedCount = scannedCount,
            DestinationTreeId = destinationTreeId,
            OperationId = operationId,
        };

    /// <summary>Creates an aborted terminal report describing the first offending entry.</summary>
    /// <param name="scannedCount">Entries scanned up to and including the offender.</param>
    /// <param name="offendingKey">The offending key.</param>
    /// <param name="reason">Why the value could not be remediated.</param>
    /// <param name="offendingValuePreview">A bounded preview of the offending value.</param>
    /// <param name="operationId">The operation id.</param>
    public static LatticeSchemaRemediationReport Aborted(
        int scannedCount, string offendingKey, string reason, byte[] offendingValuePreview, string operationId) =>
        new()
        {
            Phase = LatticeSchemaRemediationPhase.Aborted,
            InProgress = false,
            ScannedCount = scannedCount,
            OffendingKey = offendingKey,
            Reason = reason,
            OffendingValuePreview = offendingValuePreview,
            OperationId = operationId,
        };

    /// <summary>
    /// Compares two reports by value, with <see cref="OffendingValuePreview"/>
    /// compared by content. The compiler-generated record-struct equality compares
    /// the <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical reports - and, in
    /// particular, a report and its post-serialization self - would otherwise never
    /// compare equal.
    /// </summary>
    /// <param name="other">The report to compare against.</param>
    public bool Equals(LatticeSchemaRemediationReport other) =>
        Phase == other.Phase
        && InProgress == other.InProgress
        && ScannedCount == other.ScannedCount
        && string.Equals(OffendingKey, other.OffendingKey, StringComparison.Ordinal)
        && string.Equals(Reason, other.Reason, StringComparison.Ordinal)
        && BytesEqual(OffendingValuePreview, other.OffendingValuePreview)
        && string.Equals(DestinationTreeId, other.DestinationTreeId, StringComparison.Ordinal)
        && string.Equals(OperationId, other.OperationId, StringComparison.Ordinal);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Phase);
        hash.Add(InProgress);
        hash.Add(ScannedCount);
        hash.Add(OffendingKey, StringComparer.Ordinal);
        hash.Add(Reason, StringComparer.Ordinal);
        if (OffendingValuePreview is { } preview)
        {
            hash.AddBytes(preview);
        }

        hash.Add(DestinationTreeId, StringComparer.Ordinal);
        hash.Add(OperationId, StringComparer.Ordinal);
        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
