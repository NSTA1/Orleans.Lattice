using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>Default <see cref="IWalPurgeHoldGrain"/>.</summary>
internal sealed class WalPurgeHoldGrain(
    [PersistentState("wal-purge-hold", LatticeOptions.StorageProviderName)]
    IPersistentState<WalPurgeHoldState> state) : Grain, IWalPurgeHoldGrain
{
    /// <summary>Time source for <see cref="WalPurgeHold.Since"/>; settable for tests.</summary>
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <inheritdoc />
    public async Task AddAsync(string consumerId, long[] trimmedThrough)
    {
        ArgumentException.ThrowIfNullOrEmpty(consumerId);
        ArgumentNullException.ThrowIfNull(trimmedThrough);

        var holds = state.State.Holds;
        holds.TryGetValue(consumerId, out var previous);
        var width = Math.Max(trimmedThrough.Length, previous?.TrimmedThrough.Length ?? 0);
        var merged = new long[width];
        for (var p = 0; p < width; p++)
        {
            var incoming = p < trimmedThrough.Length ? trimmedThrough[p] : -1L;
            var existing = previous is not null && p < previous.TrimmedThrough.Length ? previous.TrimmedThrough[p] : -1L;
            merged[p] = Math.Max(incoming, existing);
        }

        if (previous is not null && merged.AsSpan().SequenceEqual(previous.TrimmedThrough))
        {
            return;
        }

        holds[consumerId] = new WalPurgeHold
        {
            TrimmedThrough = merged,
            Since = previous?.Since ?? TimeProvider.GetUtcNow(),
        };
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            if (previous is null)
            {
                holds.Remove(consumerId);
            }
            else
            {
                holds[consumerId] = previous;
            }

            throw;
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(string consumerId)
    {
        ArgumentException.ThrowIfNullOrEmpty(consumerId);
        if (!state.State.Holds.Remove(consumerId, out var previous))
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Holds[consumerId] = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<bool> ReleaseIfCoveredAsync(string consumerId, long[]? positions)
    {
        ArgumentException.ThrowIfNullOrEmpty(consumerId);
        if (!state.State.Holds.TryGetValue(consumerId, out var hold))
        {
            return false;
        }

        if (positions is not null)
        {
            var trimmed = hold.TrimmedThrough;
            for (var p = 0; p < trimmed.Length; p++)
            {
                if (trimmed[p] >= 0 && (p >= positions.Length || positions[p] <= trimmed[p]))
                {
                    return false;
                }
            }
        }

        await RemoveAsync(consumerId);
        return true;
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, WalPurgeHold>> GetAsync() =>
        Task.FromResult<IReadOnlyDictionary<string, WalPurgeHold>>(
            new Dictionary<string, WalPurgeHold>(state.State.Holds, StringComparer.Ordinal));
}
