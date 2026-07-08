namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Worked sample <see cref="ISagaParticipant"/> that demonstrates the public
/// contract with a realistic prepare/commit/abort resource. It models an
/// application resource holding a single committed value: prepare <b>stages</b> a
/// pending value (without applying it), commit <b>applies</b> the staged value,
/// and abort <b>discards</b> the staged value (total compensation back to the
/// pre-prepare view). Every method is idempotent, matching the SPI guardrails a
/// host participant must honour.
/// <para>
/// Access is not synchronised because the durable participant model drives each
/// participant turn-by-turn on a single logical thread; a real participant that
/// is reached from other code paths would guard its state accordingly.
/// </para>
/// </summary>
public sealed class ExampleSagaParticipant : ISagaParticipant
{
    private bool _hasPending;
    private string? _pendingValue;
    private string? _committedValue;

    /// <summary>
    /// The vote this participant returns from <see cref="PrepareAsync"/>. Set to
    /// <see cref="SagaVote.Abort"/> to model a participant that declines (a
    /// precondition failure); it then stages nothing.
    /// </summary>
    public SagaVote PrepareVote { get; set; } = SagaVote.Commit;

    /// <summary>The value staged on prepare and applied on commit.</summary>
    public string StagedValue { get; set; } = "example-value";

    /// <summary>Number of times <see cref="PrepareAsync"/> was invoked.</summary>
    public int PrepareCount { get; private set; }

    /// <summary>Number of times <see cref="CommitAsync"/> was invoked.</summary>
    public int CommitCount { get; private set; }

    /// <summary>Number of times <see cref="AbortAsync"/> was invoked.</summary>
    public int AbortCount { get; private set; }

    /// <summary>The value that has been durably committed, or <see langword="null"/>.</summary>
    public string? CommittedValue => _committedValue;

    /// <summary>Whether a staged (prepared-but-not-yet-committed) value is held.</summary>
    public bool HasPendingValue => _hasPending;

    /// <inheritdoc />
    public Task<SagaParticipantPrepareResult> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        PrepareCount++;

        if (PrepareVote == SagaVote.Abort)
        {
            // Declined: self-compensate (stage nothing) and vote abort.
            _hasPending = false;
            _pendingValue = null;
            return Task.FromResult(new SagaParticipantPrepareResult(SagaVote.Abort, "example participant declined to prepare"));
        }

        // Idempotent: re-preparing simply keeps a single staged value.
        _hasPending = true;
        _pendingValue = StagedValue;
        return Task.FromResult(new SagaParticipantPrepareResult(SagaVote.Commit));
    }

    /// <inheritdoc />
    public Task CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        CommitCount++;

        // Idempotent: apply the staged value once; a duplicate commit (nothing
        // pending) is a no-op that leaves the committed value untouched.
        if (_hasPending)
        {
            _committedValue = _pendingValue;
            _hasPending = false;
            _pendingValue = null;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        AbortCount++;

        // Total compensation: discard the staged value, restoring the
        // pre-prepare view. Idempotent: aborting with nothing pending is a no-op.
        _hasPending = false;
        _pendingValue = null;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<SagaPhase> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        var phase = _hasPending
            ? SagaPhase.Prepared
            : _committedValue is not null ? SagaPhase.Committed : SagaPhase.None;
        return Task.FromResult(phase);
    }
}
