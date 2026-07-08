using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Diagnostic decorator that wraps a host-supplied <typeparamref name="TParticipant"/>
/// with an operator-chosen name and logs each saga phase transition (prepare
/// vote, commit, abort) at <see cref="LogLevel.Debug"/> under that name. Enlisted
/// by the named overload of
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeSagaParticipant{TParticipant}(ISiloBuilder, string?)"/>.
/// <para>
/// The wrapper is transparent to the participant model: it forwards every SPI
/// call unchanged and preserves the inner participant's vote and idempotency, so
/// the name is used for diagnostics only and never affects the saga wire contract
/// or the drive model. One closed generic implementation type per
/// <typeparamref name="TParticipant"/> keeps the enumerable registration
/// idempotent per participant type.
/// </para>
/// </summary>
/// <typeparam name="TParticipant">The wrapped participant implementation type.</typeparam>
internal sealed class NamedSagaParticipant<TParticipant>(
    string name,
    TParticipant inner,
    ILogger<NamedSagaParticipant<TParticipant>> logger) : ISagaParticipant
    where TParticipant : class, ISagaParticipant
{
    private readonly string _name = name;
    private readonly TParticipant _inner = inner;
    private readonly ILogger<NamedSagaParticipant<TParticipant>> _logger = logger;

    /// <summary>The operator-chosen diagnostic name for this participant.</summary>
    public string Name => _name;

    /// <summary>The wrapped participant instance.</summary>
    public TParticipant Inner => _inner;

    /// <inheritdoc />
    public async Task<SagaParticipantPrepareResult> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _inner.PrepareAsync(request, cancellationToken).ConfigureAwait(false);
        _logger.LogDebug(
            "Saga participant '{ParticipantName}' voted {Vote} on prepare for saga {SagaId}.",
            _name, result.Vote, request.SagaId);
        return result;
    }

    /// <inheritdoc />
    public async Task CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug(
            "Saga participant '{ParticipantName}' committing saga {SagaId}.", _name, request.SagaId);
        await _inner.CommitAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug(
            "Saga participant '{ParticipantName}' aborting (compensating) saga {SagaId}.", _name, request.SagaId);
        await _inner.AbortAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<SagaPhase> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default) =>
        _inner.GetStatusAsync(request, cancellationToken);
}
