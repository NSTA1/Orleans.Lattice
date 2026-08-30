namespace Orleans.Lattice.Views;

/// <summary>
/// <see cref="IAggregationViewStore"/> adapter over a view <see cref="ILattice"/>
/// tree, used by the maintainer to apply aggregation contributions to the live
/// view.
/// </summary>
internal sealed class LatticeViewStore(ILattice viewTree) : IAggregationViewStore
{
    /// <inheritdoc />
    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        viewTree.GetAsync(key, cancellationToken);

    /// <inheritdoc />
    public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default) =>
        viewTree.GetManyAsync(keys, cancellationToken);

    /// <inheritdoc />
    public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default) =>
        viewTree.SetAsync(key, value, cancellationToken);

    /// <inheritdoc />
    public async Task DeleteAsync(string key, CancellationToken cancellationToken = default) =>
        await viewTree.DeleteAsync(key, cancellationToken);

    /// <inheritdoc />
    public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default) =>
        viewTree.SetManyAtomicAsync(entries, operationId, cancellationToken);
}
