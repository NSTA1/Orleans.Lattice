namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class WalShardGrain
{
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
}
