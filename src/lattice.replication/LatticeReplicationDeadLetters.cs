using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationDeadLetters"/> implementation.
/// Routes inspection / discard calls straight to the per-tree
/// <see cref="IReplicationDeadLetterGrain"/> activation, and replays
/// through the canonical concrete <see cref="ReplicationApplier"/> so
/// the in-memory failure tracker on
/// <see cref="DeadLetterTrackingReplicationApplier"/> is not engaged
/// for replay attempts (otherwise a deterministically-failing parked
/// entry would re-enqueue itself on every replay).
/// </summary>
internal sealed class LatticeReplicationDeadLetters(
    IGrainFactory grainFactory,
    ReplicationApplier inner,
    IOptionsMonitor<LatticeReplicationOptions>? options = null,
    ILogger<LatticeReplicationDeadLetters>? logger = null) : ILatticeReplicationDeadLetters
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options =
        options ?? new StaticOptionsMonitor(new LatticeReplicationOptions { ClusterId = "test" });
    private readonly ILogger<LatticeReplicationDeadLetters> _logger =
        logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<LatticeReplicationDeadLetters>.Instance;

    /// <inheritdoc />
    public Task<IReadOnlyList<DeadLetterEntry>> ListAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Grain(treeId).ListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Grain(treeId).CountAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> DiscardAsync(string treeId, long entryId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Grain(treeId).DiscardAsync(entryId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ApplyResult?> ReplayAsync(string treeId, long entryId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var grain = Grain(treeId);
        var parked = await grain.TryGetAsync(entryId, cancellationToken).ConfigureAwait(false);
        if (parked is null)
        {
            return null;
        }

        // Replay routes through the canonical applier, bypassing the
        // failure-tracking decorator. A successful return removes the
        // entry from the queue with reason=replayed; a thrown exception
        // leaves the entry parked for the operator to decide.
        var result = await inner.ApplyAsync(parked.Value.Entry, cancellationToken).ConfigureAwait(false);

        // A deferred result is not terminal: the durable receive fence of an
        // in-flight restore saga held the entry back without applying it, and
        // unlike a streamed batch nothing will re-ship a parked entry once the
        // fence lifts. Removing it here would silently drop the write, so it
        // stays parked for a later replay (issue #3757).
        if (result.Deferred)
        {
            return result;
        }

        // Successful apply (or filtered re-delivery) is terminal for
        // inspection - remove the entry and tag the metric with
        // reason=replayed so dashboards can distinguish operator replay
        // from explicit discard.
        await grain.RemoveReplayedAsync(entryId, cancellationToken).ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc />
    public async Task<bool> PoisonSagaAsync(
        string treeId,
        string originClusterId,
        Guid transactionId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException("Transaction id must not be empty.", nameof(transactionId));
        }

        cancellationToken.ThrowIfCancellationRequested();

        var recorded = await TxRegistryRouting.GetRegistry(grainFactory, treeId, transactionId)
            .GetRecordedStatusAsync(transactionId)
            .ConfigureAwait(false);
        if (recorded != TxStatus.InFlight)
        {
            DeadLetterTrackingReplicationApplier.RecordReceiverSagaPoisoned(
                treeId,
                originClusterId,
                LatticeReplicationMetrics.OutcomeReceiverSagaPoisonRefusedDecided);
            _logger.LogWarning(
                "Operator poison of receiver saga {TransactionId} from origin {Origin} on tree '{TreeId}' was refused because receiver registry status is {Status}.",
                transactionId,
                originClusterId,
                treeId,
                recorded);
            return false;
        }

        var poison = grainFactory.GetGrain<IReceiverSagaPoisonGrain>(treeId);
        var poisoned = await poison
            .PoisonAsync(originClusterId, transactionId, "Operator requested receiver-side saga poison.")
            .ConfigureAwait(false);
        if (!poisoned)
        {
            DeadLetterTrackingReplicationApplier.RecordReceiverSagaPoisoned(
                treeId,
                originClusterId,
                LatticeReplicationMetrics.OutcomeReceiverSagaPoisonRefusedFull);
            _logger.LogWarning(
                "Operator poison of receiver saga {TransactionId} from origin {Origin} on tree '{TreeId}' was refused because the receiver poison set is full.",
                transactionId,
                originClusterId,
                treeId);
            return false;
        }

        DeadLetterTrackingReplicationApplier.RecordReceiverSagaPoisoned(
            treeId,
            originClusterId,
            LatticeReplicationMetrics.OutcomeReceiverSagaPoisonedOperator);
        _logger.LogWarning(
            "Operator poisoned receiver saga {TransactionId} from origin {Origin} on tree '{TreeId}'; the origin link will park matching saga records until re-seed settles it.",
            transactionId,
            originClusterId,
            treeId);

        _ = ReceiverSagaPoisonReseed.TryStartOrMarkOwedAsync(
            grainFactory,
            _options,
            treeId,
            originClusterId,
            _logger,
            CancellationToken.None);
        return true;
    }

    private IReplicationDeadLetterGrain Grain(string treeId) =>
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(treeId);

    private sealed class StaticOptionsMonitor(LatticeReplicationOptions value) : IOptionsMonitor<LatticeReplicationOptions>
    {
        public LatticeReplicationOptions CurrentValue => value;

        public LatticeReplicationOptions Get(string? name) => value;

        public IDisposable? OnChange(Action<LatticeReplicationOptions, string?> listener) => null;
    }
}
