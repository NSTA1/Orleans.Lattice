namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The seam over the Microsoft Graph transitive-group query. Abstracted so tests
/// fake the Graph response without any live Graph call. The production
/// implementation calls the user's <c>getMemberGroups</c> endpoint and returns the
/// full set of transitive group (and directory role) ids.
/// </summary>
internal interface IEntraGraphMemberGroupsClient
{
    /// <summary>
    /// Resolves the transitive group ids for the caller identified by
    /// <paramref name="subjectId"/>.
    /// </summary>
    /// <param name="subjectId">The caller's Entra object id.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>The resolved group ids; empty when the caller belongs to none.</returns>
    Task<IReadOnlyCollection<string>> GetTransitiveGroupIdsAsync(string subjectId, CancellationToken cancellationToken);
}
