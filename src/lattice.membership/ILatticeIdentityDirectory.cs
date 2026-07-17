namespace Orleans.Lattice.Membership;

/// <summary>
/// A provider-agnostic seam over the external identity source (Entra, a static
/// roster, an LDAP directory, ...) used to browse / search principals and to
/// confirm a supplied id actually exists before an operator grants it access.
/// Sits alongside the identity-layer seams <see cref="ILatticeMembershipDirectory"/>,
/// <see cref="ILatticeSubjectMapper"/>, and the Entra group resolver: those
/// resolve an already-authenticated caller, whereas this validates candidate ids
/// an operator is about to add. A default no-op provider
/// (<see cref="NullIdentityDirectory"/>) is always registered, so any real
/// provider overrides it with a last-wins registration.
/// </summary>
public interface ILatticeIdentityDirectory
{
    /// <summary>
    /// A stable identifier for the configured provider (for example
    /// <c>"null"</c>, <c>"entra"</c>, or <c>"static"</c>). Surfaced so callers can
    /// distinguish which identity source is backing validation.
    /// </summary>
    string ProviderId { get; }

    /// <summary>
    /// A human-readable, operator-facing description of what a valid principal id
    /// is for this deployment and where it comes from - surfaced inline in the
    /// New user / New group forms so the operator sees exactly what to enter.
    /// Every provider should override this with wording specific to its identity
    /// source.
    /// </summary>
    string Explanation { get; }

    /// <summary>
    /// Searches or browses the external identity source for principals matching
    /// <paramref name="query"/>, returning a single page plus an optional
    /// continuation token for 'load more'.
    /// </summary>
    /// <param name="query">The typeahead / browse query.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>A page of matched principals; empty when nothing matches.</returns>
    Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves a single principal by its exact id, confirming it exists in the
    /// external identity source.
    /// </summary>
    /// <param name="principalId">The exact principal id to resolve. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <returns>
    /// The resolved <see cref="DirectoryPrincipal"/>, or <c>null</c> when no
    /// principal with that id exists.
    /// </returns>
    Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default);
}
