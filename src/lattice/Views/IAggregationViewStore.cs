namespace Orleans.Lattice.Views;

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

    /// <summary>
    /// Writes <paramref name="entries"/> all-or-nothing across the listed keys,
    /// idempotently keyed by <paramref name="operationId"/>: re-submitting the
    /// same operation id re-attaches to the completed flip and returns without
    /// re-applying. The flip can only <c>Set</c> (there is no atomic delete), so
    /// a row that must vanish atomically with its siblings is written as the
    /// <see cref="AggregationRowCodec.EmptyRow"/> sentinel and cleaned up
    /// out-of-band afterwards. Used to move a contribution's membership row and
    /// the affected accumulator slot(s) as a single unit, so a mid-drain crash
    /// plus a full-batch WAL replay cannot double-count.
    /// </summary>
    Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default);
}
