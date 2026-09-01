using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The store-of-record seam the approximate index derives itself from: the
/// repository's vectors in one embedding space, streamed in ascending identifier
/// order so a background build is resumable, plus the reverse lookup that turns
/// the identifiers a search returned back into the canonical source keys the
/// search service hydrates from.
/// <para>
/// Both halves read the vector trees and nothing else, so the whole approximate
/// plane above this seam is exercisable without a silo by substituting an
/// in-memory implementation.
/// </para>
/// </summary>
internal interface IRepoContextVectorSource : IVectorSource
{
    /// <summary>
    /// Resolves the canonical source keys of <paramref name="vectorIds"/> from
    /// the store of record in one bounded batch, in the order the identifiers
    /// were supplied.
    /// <para>
    /// An identifier the store no longer holds is simply absent from the result,
    /// which is what stops a retired vector the index has not yet dropped from
    /// being hydrated: the store of record settles the disagreement, always.
    /// </para>
    /// </summary>
    /// <param name="vectorIds">The vector identifiers to resolve. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    /// <returns>The source key of each identifier the store still holds.</returns>
    Task<IReadOnlyDictionary<string, string>> ResolveSourceKeysAsync(
        IReadOnlyList<string> vectorIds, CancellationToken cancellationToken);
}
