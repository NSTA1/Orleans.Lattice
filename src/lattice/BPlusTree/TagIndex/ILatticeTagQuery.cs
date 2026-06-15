namespace Orleans.Lattice;

/// <summary>
/// A lazy, streaming tag query over a single subject tree. Enumerating yields
/// the matching keys in the order the underlying posting lists are walked;
/// <see cref="CountAsync"/> drains the same stream to a count without
/// materialising the keys.
/// </summary>
public interface ILatticeTagQuery : IAsyncEnumerable<string>
{
    /// <summary>Returns the number of keys the query matches.</summary>
    /// <param name="cancellationToken">Cancels the drain.</param>
    Task<int> CountAsync(CancellationToken cancellationToken = default);
}
