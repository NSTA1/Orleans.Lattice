namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// The minimal key-value surface the <see cref="AggregationApplier"/> needs over
/// the <c>view-{name}</c> tree. Abstracted so the applier's reduce / retraction
/// logic is unit-testable against an in-memory store without a grain or cluster;
/// the maintainer supplies an adapter over the view <see cref="ILattice"/>.
/// </summary>
internal interface IAggregationViewStore
{
    /// <summary>Gets the value for <paramref name="key"/>, or <see langword="null"/> when absent.</summary>
    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/>.</summary>
    Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Removes <paramref name="key"/> (an idempotent no-op when already absent).</summary>
    Task DeleteAsync(string key, CancellationToken cancellationToken = default);
}
