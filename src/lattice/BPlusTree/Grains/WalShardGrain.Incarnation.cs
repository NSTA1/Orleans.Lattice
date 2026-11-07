namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class WalShardGrain
{
    /// <summary>
    /// Serialises every write of the floor state. The clock floor is persisted
    /// from an interleaving shipping read, so without this an incarnation
    /// advance could write concurrently with it, and the loser's stale ETag
    /// would roll its in-memory value back below what storage holds (issue
    /// #5896).
    /// </summary>
    private readonly SemaphoreSlim _floorStateWriteGate = new(1, 1);
    /// <inheritdoc />
    public ValueTask<long> GetIncarnationAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(floorState.State?.Incarnation ?? 0);
    }

    /// <inheritdoc />
    public async Task<long> AdvanceIncarnationAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await _floorStateWriteGate.WaitAsync(cancellationToken).ConfigureAwait(true);
        try
        {
            var previous = floorState.State.Incarnation;
            floorState.State.Incarnation = previous + 1;
            try
            {
                await floorState.WriteStateAsync().ConfigureAwait(true);
            }
            catch
            {
                // Persist before publish: a failed write must not leave a value
                // in memory that the durable state does not hold.
                floorState.State.Incarnation = previous;
                throw;
            }

            return previous + 1;
        }
        finally
        {
            _floorStateWriteGate.Release();
        }
    }
}
