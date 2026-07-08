namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Recording test-double <see cref="ISagaParticipant"/>. Votes a configurable
/// result on prepare and records every prepare / commit / abort / status call
/// so tests can assert the durable participant model drives it correctly. Used
/// to exercise the participant model in isolation from any production
/// participant.
/// </summary>
internal sealed class RecordingSagaParticipant(SagaVote prepareVote = SagaVote.Commit, string? detail = null)
    : ISagaParticipant
{
    /// <summary>Number of times <see cref="PrepareAsync"/> was invoked.</summary>
    public int PrepareCount { get; private set; }

    /// <summary>Number of times <see cref="CommitAsync"/> was invoked.</summary>
    public int CommitCount { get; private set; }

    /// <summary>Number of times <see cref="AbortAsync"/> was invoked.</summary>
    public int AbortCount { get; private set; }

    /// <summary>The vote this double returns from <see cref="PrepareAsync"/>.</summary>
    public SagaVote PrepareVote { get; set; } = prepareVote;

    /// <inheritdoc />
    public Task<SagaParticipantPrepareResult> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        PrepareCount++;
        return Task.FromResult(new SagaParticipantPrepareResult(PrepareVote, detail));
    }

    /// <inheritdoc />
    public Task CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        CommitCount++;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        AbortCount++;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<SagaPhase> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default) =>
        Task.FromResult(SagaPhase.None);
}
