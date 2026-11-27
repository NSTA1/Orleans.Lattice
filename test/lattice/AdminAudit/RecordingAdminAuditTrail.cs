namespace Orleans.Lattice.Tests.AdminAudit;

/// <summary>
/// An <see cref="ILatticeAdminAuditTrail"/> that records every call in order, for
/// asserting the admin audit protocol <see cref="LatticeAdminAudit"/> drives.
/// </summary>
internal sealed class RecordingAdminAuditTrail : ILatticeAdminAuditTrail
{
    /// <summary>Every call, in order: <c>denied:{reason}</c>, <c>begin</c>, <c>complete:{outcome}:{reason}</c>, or a caller-appended marker.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>When set, <see cref="BeginAsync"/> fails closed as the durable trail does.</summary>
    public bool FailBegin { get; set; }

    /// <inheritdoc />
    public bool IsEnabled { get; set; } = true;

    /// <inheritdoc />
    public ValueTask RecordDeniedAsync(LatticeAdminAuditRequest request, string reasonCode, CancellationToken cancellationToken)
    {
        Calls.Add("denied:" + reasonCode);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<LatticeAdminAuditRecord> BeginAsync(LatticeAdminAuditRequest request, CancellationToken cancellationToken)
    {
        if (FailBegin)
        {
            throw new LatticeAdminAuditUnavailableException();
        }

        Calls.Add("begin");
        return ValueTask.FromResult(new LatticeAdminAuditRecord
        {
            OperationId = "op",
            ActorSubjectId = "actor",
            TargetScope = request.TargetScope,
            Action = request.Action,
        });
    }

    /// <inheritdoc />
    public ValueTask CompleteAsync(LatticeAdminAuditRecord intent, LatticeAdminAuditCompletion completion)
    {
        Calls.Add($"complete:{completion.Outcome}:{completion.ReasonCode}");
        return ValueTask.CompletedTask;
    }
}
