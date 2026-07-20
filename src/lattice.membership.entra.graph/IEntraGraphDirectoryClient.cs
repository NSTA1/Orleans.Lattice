namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The seam over the Microsoft Graph <c>/users</c> and <c>/groups</c> read
/// queries used by <see cref="EntraGraphIdentityDirectory"/>. Abstracted so unit
/// tests fake the Graph responses with no live Graph call. The production
/// implementation (<see cref="GraphEntraDirectoryClient"/>) issues server-side
/// filtered / searched and paged Graph queries and translates a Graph denial
/// (missing <c>User.Read.All</c> / <c>Group.Read.All</c> scopes, or any other
/// error) into <see cref="EntraDirectoryUnavailableException"/>.
/// </summary>
internal interface IEntraGraphDirectoryClient
{
    /// <summary>
    /// Searches or browses Entra users. An empty <paramref name="term"/> browses
    /// the first page of all users.
    /// </summary>
    /// <param name="term">The search term matched against display name / UPN / mail.</param>
    /// <param name="pageSize">The already-clamped maximum number of records to return.</param>
    /// <param name="continuationToken">The raw Graph next-link from a prior page, or <c>null</c> for the first page.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>A page of user records; <see cref="EntraDirectoryPage.Empty"/> when none match.</returns>
    /// <exception cref="EntraDirectoryUnavailableException">Graph denied the query (for example missing scopes).</exception>
    Task<EntraDirectoryPage> SearchUsersAsync(string term, int pageSize, string? continuationToken, CancellationToken cancellationToken);

    /// <summary>
    /// Searches or browses Entra groups. An empty <paramref name="term"/> browses
    /// the first page of all groups.
    /// </summary>
    /// <param name="term">The search term matched against display name / mail.</param>
    /// <param name="pageSize">The already-clamped maximum number of records to return.</param>
    /// <param name="continuationToken">The raw Graph next-link from a prior page, or <c>null</c> for the first page.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>A page of group records; <see cref="EntraDirectoryPage.Empty"/> when none match.</returns>
    /// <exception cref="EntraDirectoryUnavailableException">Graph denied the query (for example missing scopes).</exception>
    Task<EntraDirectoryPage> SearchGroupsAsync(string term, int pageSize, string? continuationToken, CancellationToken cancellationToken);

    /// <summary>
    /// Resolves a single user by exact id (object id or UPN), confirming it
    /// exists.
    /// </summary>
    /// <param name="userId">The exact user id (object id or UPN) to resolve.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>The resolved record, or <c>null</c> when no such user exists.</returns>
    /// <exception cref="EntraDirectoryUnavailableException">Graph denied the query (for example missing scopes).</exception>
    Task<EntraDirectoryRecord?> ResolveUserAsync(string userId, CancellationToken cancellationToken);

    /// <summary>
    /// Resolves a single group by exact object id, confirming it exists.
    /// </summary>
    /// <param name="groupId">The exact group object id to resolve.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>The resolved record, or <c>null</c> when no such group exists.</returns>
    /// <exception cref="EntraDirectoryUnavailableException">Graph denied the query (for example missing scopes).</exception>
    Task<EntraDirectoryRecord?> ResolveGroupAsync(string groupId, CancellationToken cancellationToken);
}
