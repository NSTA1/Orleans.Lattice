using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree saga decision registry. See <see cref="ITxRegistryGrain"/>
/// for the contract and the role this grain plays in delivering strict
/// per-tree atomic-write visibility.
/// <para>
/// Implementation notes:
/// </para>
/// <list type="bullet">
/// <item><description>The registry is the single tree-wide linearization
/// point. <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> persist
/// the decision before returning; the saga grain then begins the
/// terminal fan-out. Concurrent leaf reads observing a pending entry
/// dial back to <c>GetStatusAsync</c> and use the registry's
/// already-persisted decision to resolve the read.</description></item>
/// <item><description>The grain is single-threaded by Orleans turn semantics
/// and persisted via <see cref="LatticeOptions.StorageProviderName"/>,
/// so decision recording is a single atomic state-write per call.</description></item>
/// <item><description>Idempotency: repeated calls with the same outcome are
/// no-ops. Conflicting calls (commit-then-abort or abort-then-commit)
/// throw <see cref="InvalidOperationException"/> — they indicate a saga
/// implementation bug, not a recoverable transient.</description></item>
/// <item><description><c>ForgetAsync</c> is the only operation that shrinks
/// the persisted footprint. The saga grain calls it after every
/// touched leaf has applied its terminal so the registry's working set
/// stays bounded by in-flight + recently-completed sagas.</description></item>
/// </list>
/// </summary>
internal sealed class TxRegistryGrain(
    IGrainContext context,
    [PersistentState("tx-registry", LatticeOptions.StorageProviderName)]
    IPersistentState<TxRegistryState> state) : ITxRegistryGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task MarkCommittedAsync(Guid txid)
    {
        if (state.State.Decisions.TryGetValue(txid, out var existing))
        {
            if (existing == TxStatus.Committed) return;
            if (existing == TxStatus.Aborted)
            {
                throw new InvalidOperationException(
                    $"Cannot mark saga {txid:N} as committed: it was previously recorded as aborted.");
            }
        }

        state.State.Decisions[txid] = TxStatus.Committed;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task MarkAbortedAsync(Guid txid)
    {
        if (state.State.Decisions.TryGetValue(txid, out var existing))
        {
            if (existing == TxStatus.Aborted) return;
            if (existing == TxStatus.Committed)
            {
                throw new InvalidOperationException(
                    $"Cannot mark saga {txid:N} as aborted: it was previously recorded as committed.");
            }
        }

        state.State.Decisions[txid] = TxStatus.Aborted;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<TxStatus> GetStatusAsync(Guid txid)
    {
        return Task.FromResult(
            state.State.Decisions.TryGetValue(txid, out var status)
                ? status
                : TxStatus.InFlight);
    }

    /// <inheritdoc />
    public Task<Dictionary<Guid, TxStatus>> GetStatusManyAsync(IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);
        var result = new Dictionary<Guid, TxStatus>(txids.Count);
        foreach (var txid in txids)
        {
            result[txid] = state.State.Decisions.TryGetValue(txid, out var status)
                ? status
                : TxStatus.InFlight;
        }
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public Task<Dictionary<Guid, TxStatus>> SnapshotAsync()
    {
        // Return a defensive copy so callers cannot mutate the
        // registry's persisted state through the returned reference.
        // The dictionary's working set is bounded by in-flight +
        // recently-completed sagas (forgotten on saga completion via
        // ForgetAsync), so allocating a fresh copy on every snapshot
        // is cheap relative to the cost of cross-shard scan fan-out.
        return Task.FromResult(new Dictionary<Guid, TxStatus>(state.State.Decisions));
    }

    /// <inheritdoc />
    public async Task ForgetAsync(Guid txid)
    {
        if (state.State.Decisions.Remove(txid))
        {
            await state.WriteStateAsync();
        }
    }
}
