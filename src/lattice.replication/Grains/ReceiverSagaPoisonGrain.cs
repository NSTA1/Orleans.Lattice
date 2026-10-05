using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default durable receiver-side saga poison set.</summary>
internal sealed class ReceiverSagaPoisonGrain(
    [PersistentState("receiver-saga-poison", LatticeOptions.StorageProviderName)]
    IPersistentState<ReceiverSagaPoisonState> state) : Grain, IReceiverSagaPoisonGrain
{
    private const int Capacity = 4096;

    /// <inheritdoc />
    public async Task<bool> PoisonAsync(string originClusterId, Guid transactionId, string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentException.ThrowIfNullOrEmpty(reason);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException("Transaction id must not be empty.", nameof(transactionId));
        }

        if (state.State.Entries.Any(e =>
            string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal)
            && e.TransactionId == transactionId))
        {
            return true;
        }

        if (state.State.Entries.Count >= Capacity)
        {
            return false;
        }

        state.State.Entries.Add(new ReceiverSagaPoisonRecord(originClusterId, transactionId, reason));
        AddReseedOwed(originClusterId);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Entries.RemoveAt(state.State.Entries.Count - 1);
            RemoveReseedOwedIfNoEntries(originClusterId);
            throw;
        }

        return true;
    }

    /// <inheritdoc />
    public Task<IReadOnlyCollection<Guid>> FilterPoisonedAsync(
        string originClusterId,
        IReadOnlyCollection<Guid> transactionIds)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentNullException.ThrowIfNull(transactionIds);
        if (transactionIds.Count == 0 || state.State.Entries.Count == 0)
        {
            return Task.FromResult<IReadOnlyCollection<Guid>>(Array.Empty<Guid>());
        }

        var requested = new HashSet<Guid>(transactionIds);
        var result = state.State.Entries
            .Where(e => string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal)
                && requested.Contains(e.TransactionId))
            .Select(e => e.TransactionId)
            .Distinct()
            .ToArray();
        return Task.FromResult<IReadOnlyCollection<Guid>>(result);
    }

    /// <inheritdoc />
    public Task<IReadOnlyCollection<Guid>> GetPoisonedAsync(string originClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (state.State.Entries.Count == 0)
        {
            return Task.FromResult<IReadOnlyCollection<Guid>>(Array.Empty<Guid>());
        }

        var result = state.State.Entries
            .Where(e => string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal))
            .Select(e => e.TransactionId)
            .Distinct()
            .ToArray();
        return Task.FromResult<IReadOnlyCollection<Guid>>(result);
    }

    /// <inheritdoc />
    public async Task RetireAsync(string originClusterId, IReadOnlyCollection<Guid> transactionIds)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentNullException.ThrowIfNull(transactionIds);
        if (state.State.Entries.Count == 0)
        {
            return;
        }

        if (transactionIds.Count == 0)
        {
            return;
        }

        var retired = new HashSet<Guid>(transactionIds);
        var previous = state.State.Entries;
        var retained = previous
            .Where(e => !string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal)
                || !retired.Contains(e.TransactionId))
            .ToList();
        if (retained.Count == previous.Count)
        {
            return;
        }

        var previousOwed = state.State.ReseedOwedOrigins;
        state.State.Entries = retained;
        RemoveReseedOwedIfNoEntries(originClusterId);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Entries = previous;
            state.State.ReseedOwedOrigins = previousOwed;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyCollection<string>> GetReseedOwedOriginsAsync()
    {
        if (state.State.ReseedOwedOrigins.Count == 0)
        {
            return Task.FromResult<IReadOnlyCollection<string>>(Array.Empty<string>());
        }

        return Task.FromResult<IReadOnlyCollection<string>>(
            state.State.ReseedOwedOrigins
                .Distinct(StringComparer.Ordinal)
                .ToArray());
    }

    /// <inheritdoc />
    public async Task SetReseedOwedAsync(string originClusterId, bool owed)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        var previous = state.State.ReseedOwedOrigins;
        var changed = owed
            ? AddReseedOwed(originClusterId)
            : RemoveReseedOwed(originClusterId);
        if (!changed)
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.ReseedOwedOrigins = previous;
            throw;
        }
    }

    private bool AddReseedOwed(string originClusterId)
    {
        if (state.State.ReseedOwedOrigins.Contains(originClusterId, StringComparer.Ordinal))
        {
            return false;
        }

        state.State.ReseedOwedOrigins.Add(originClusterId);
        return true;
    }

    private bool RemoveReseedOwed(string originClusterId)
    {
        var previous = state.State.ReseedOwedOrigins;
        var retained = previous
            .Where(o => !string.Equals(o, originClusterId, StringComparison.Ordinal))
            .ToList();
        if (retained.Count == previous.Count)
        {
            return false;
        }

        state.State.ReseedOwedOrigins = retained;
        return true;
    }

    private void RemoveReseedOwedIfNoEntries(string originClusterId)
    {
        if (state.State.Entries.Any(e => string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal)))
        {
            return;
        }

        RemoveReseedOwed(originClusterId);
    }
}
