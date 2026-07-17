using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Graph.Models.ODataErrors;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The production <see cref="IEntraGraphDirectoryClient"/>. It issues Microsoft
/// Graph <c>/users</c> and <c>/groups</c> read queries - server-side searched (or
/// browsed) and paged - and resolves a single principal by exact id. Requires the
/// app-only token to hold the <c>User.Read.All</c> and <c>Group.Read.All</c> read
/// scopes; a Graph denial (missing scopes, or any other error) is translated into
/// <see cref="EntraDirectoryUnavailableException"/> so the calling
/// <see cref="EntraGraphIdentityDirectory"/> degrades cleanly instead of faulting.
/// </summary>
internal sealed class GraphEntraDirectoryClient : IEntraGraphDirectoryClient
{
    private const string ConsistencyLevelHeader = "ConsistencyLevel";
    private const string EventualConsistency = "eventual";
    private const int NotFoundStatusCode = 404;

    private static readonly string[] UserSelect = { "id", "displayName", "userPrincipalName", "mail" };
    private static readonly string[] GroupSelect = { "id", "displayName", "mail" };
    private static readonly string[] OrderByDisplayName = { "displayName" };

    private const string InvalidContinuationToken = "The directory continuation token was not a valid Microsoft Graph pagination cursor.";

    private readonly GraphServiceClient _graphClient;
    private readonly Uri? _graphBaseUrl;

    /// <summary>
    /// Initializes a new <see cref="GraphEntraDirectoryClient"/>.
    /// </summary>
    /// <param name="graphClient">The Graph client, authenticated with the app-only token. Must not be <c>null</c>.</param>
    public GraphEntraDirectoryClient(GraphServiceClient graphClient)
    {
        ArgumentNullException.ThrowIfNull(graphClient);
        _graphClient = graphClient;

        // Capture the configured Graph endpoint host once so continuation tokens can be
        // validated against it. Honours national-cloud endpoints (graph.microsoft.us, etc.)
        // rather than hard-coding graph.microsoft.com. An unparseable base URL fails closed.
        _graphBaseUrl = GraphContinuationToken.ParseGraphBaseUrl(graphClient.RequestAdapter.BaseUrl);
    }

    /// <inheritdoc />
    public async Task<EntraDirectoryPage> SearchUsersAsync(string term, int pageSize, string? continuationToken, CancellationToken cancellationToken)
    {
        try
        {
            UserCollectionResponse? response;
            if (!string.IsNullOrEmpty(continuationToken))
            {
                if (!GraphContinuationToken.IsValid(continuationToken, _graphBaseUrl))
                {
                    throw new EntraDirectoryUnavailableException(InvalidContinuationToken);
                }

                response = await _graphClient.Users
                    .WithUrl(continuationToken)
                    .GetAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                var search = BuildSearch(term, includeUpn: true);
                response = await _graphClient.Users.GetAsync(rc =>
                {
                    rc.QueryParameters.Top = pageSize;
                    rc.QueryParameters.Select = UserSelect;
                    if (search is null)
                    {
                        rc.QueryParameters.Orderby = OrderByDisplayName;
                    }
                    else
                    {
                        rc.QueryParameters.Search = search;
                        rc.QueryParameters.Count = true;
                        rc.Headers.Add(ConsistencyLevelHeader, EventualConsistency);
                    }
                }, cancellationToken).ConfigureAwait(false);
            }

            return MapUsers(response);
        }
        catch (ODataError ex)
        {
            throw new EntraDirectoryUnavailableException("Microsoft Graph denied the users search.", ex);
        }
    }

    /// <inheritdoc />
    public async Task<EntraDirectoryPage> SearchGroupsAsync(string term, int pageSize, string? continuationToken, CancellationToken cancellationToken)
    {
        try
        {
            GroupCollectionResponse? response;
            if (!string.IsNullOrEmpty(continuationToken))
            {
                if (!GraphContinuationToken.IsValid(continuationToken, _graphBaseUrl))
                {
                    throw new EntraDirectoryUnavailableException(InvalidContinuationToken);
                }

                response = await _graphClient.Groups
                    .WithUrl(continuationToken)
                    .GetAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                var search = BuildSearch(term, includeUpn: false);
                response = await _graphClient.Groups.GetAsync(rc =>
                {
                    rc.QueryParameters.Top = pageSize;
                    rc.QueryParameters.Select = GroupSelect;
                    if (search is null)
                    {
                        rc.QueryParameters.Orderby = OrderByDisplayName;
                    }
                    else
                    {
                        rc.QueryParameters.Search = search;
                        rc.QueryParameters.Count = true;
                        rc.Headers.Add(ConsistencyLevelHeader, EventualConsistency);
                    }
                }, cancellationToken).ConfigureAwait(false);
            }

            return MapGroups(response);
        }
        catch (ODataError ex)
        {
            throw new EntraDirectoryUnavailableException("Microsoft Graph denied the groups search.", ex);
        }
    }

    /// <inheritdoc />
    public async Task<EntraDirectoryRecord?> ResolveUserAsync(string userId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(userId);

        try
        {
            var user = await _graphClient.Users[userId]
                .GetAsync(rc => rc.QueryParameters.Select = UserSelect, cancellationToken)
                .ConfigureAwait(false);

            return user is null ? null : ToUserRecord(user);
        }
        catch (ODataError ex) when (ex.ResponseStatusCode == NotFoundStatusCode)
        {
            return null;
        }
        catch (ODataError ex)
        {
            throw new EntraDirectoryUnavailableException("Microsoft Graph denied the user resolve.", ex);
        }
    }

    /// <inheritdoc />
    public async Task<EntraDirectoryRecord?> ResolveGroupAsync(string groupId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);

        try
        {
            var group = await _graphClient.Groups[groupId]
                .GetAsync(rc => rc.QueryParameters.Select = GroupSelect, cancellationToken)
                .ConfigureAwait(false);

            return group is null ? null : ToGroupRecord(group);
        }
        catch (ODataError ex) when (ex.ResponseStatusCode == NotFoundStatusCode)
        {
            return null;
        }
        catch (ODataError ex)
        {
            throw new EntraDirectoryUnavailableException("Microsoft Graph denied the group resolve.", ex);
        }
    }

    private static string? BuildSearch(string term, bool includeUpn)
    {
        if (string.IsNullOrWhiteSpace(term))
        {
            return null;
        }

        // Strip embedded quotes so the term cannot break out of the quoted search clause.
        var sanitized = term.Replace("\"", string.Empty, StringComparison.Ordinal);
        return includeUpn
            ? $"\"displayName:{sanitized}\" OR \"userPrincipalName:{sanitized}\" OR \"mail:{sanitized}\""
            : $"\"displayName:{sanitized}\" OR \"mail:{sanitized}\"";
    }

    private static EntraDirectoryPage MapUsers(UserCollectionResponse? response)
    {
        var users = response?.Value;
        if (users is null || users.Count == 0)
        {
            return new EntraDirectoryPage(Array.Empty<EntraDirectoryRecord>(), response?.OdataNextLink);
        }

        var records = new EntraDirectoryRecord[users.Count];
        for (var i = 0; i < users.Count; i++)
        {
            records[i] = ToUserRecord(users[i]);
        }

        return new EntraDirectoryPage(records, response!.OdataNextLink);
    }

    private static EntraDirectoryPage MapGroups(GroupCollectionResponse? response)
    {
        var groups = response?.Value;
        if (groups is null || groups.Count == 0)
        {
            return new EntraDirectoryPage(Array.Empty<EntraDirectoryRecord>(), response?.OdataNextLink);
        }

        var records = new EntraDirectoryRecord[groups.Count];
        for (var i = 0; i < groups.Count; i++)
        {
            records[i] = ToGroupRecord(groups[i]);
        }

        return new EntraDirectoryPage(records, response!.OdataNextLink);
    }

    private static EntraDirectoryRecord ToUserRecord(User user)
    {
        var objectId = user.Id ?? string.Empty;
        var displayName = user.DisplayName ?? user.UserPrincipalName ?? objectId;
        return new EntraDirectoryRecord(objectId, displayName, user.UserPrincipalName, DirectoryPrincipalKind.User);
    }

    private static EntraDirectoryRecord ToGroupRecord(Group group)
    {
        var objectId = group.Id ?? string.Empty;
        var displayName = group.DisplayName ?? objectId;
        return new EntraDirectoryRecord(objectId, displayName, UserPrincipalName: null, DirectoryPrincipalKind.Group);
    }
}
