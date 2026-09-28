namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// A source of address completions for the address line: an area's answer to
/// "what here matches what the user typed?".
/// </summary>
/// <remarks>
/// <see langword="internal"/>, and reachable only through
/// <see cref="IExplorerArea.Completions"/>: there is no other way to register one.
/// The address line asks every visible area's source in parallel, each under its
/// own timeout, and a source that throws or times out never blocks the rest.
/// </remarks>
internal interface IAddressCompletionSource
{
    /// <summary>
    /// Completes <paramref name="query"/>. Return at most
    /// <see cref="AddressQuery.Limit"/> results, best first; any beyond it are
    /// dropped. Honour <paramref name="cancellationToken"/>: it is cancelled when
    /// the user types again or the source's time is up.
    /// </summary>
    /// <param name="query">What the user typed, and where they are.</param>
    /// <param name="cancellationToken">Cancelled when the result is no longer wanted.</param>
    ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken);
}
