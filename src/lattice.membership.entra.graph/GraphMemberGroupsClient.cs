using Microsoft.Graph;
using Microsoft.Graph.Users.Item.GetMemberGroups;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The production <see cref="IEntraGraphMemberGroupsClient"/>. It calls the
/// caller's Microsoft Graph <c>getMemberGroups</c> endpoint, which returns the
/// full set of transitive group and directory-role ids the caller belongs to,
/// bypassing the token's group-count cap.
/// </summary>
internal sealed class GraphMemberGroupsClient : IEntraGraphMemberGroupsClient
{
    private readonly GraphServiceClient _graphClient;
    private readonly bool _securityEnabledOnly;

    /// <summary>
    /// Initializes a new <see cref="GraphMemberGroupsClient"/>.
    /// </summary>
    /// <param name="graphClient">The Graph client, authenticated with the app-only token. Must not be <c>null</c>.</param>
    /// <param name="securityEnabledOnly">Whether to return only security-enabled groups.</param>
    public GraphMemberGroupsClient(GraphServiceClient graphClient, bool securityEnabledOnly)
    {
        ArgumentNullException.ThrowIfNull(graphClient);
        _graphClient = graphClient;
        _securityEnabledOnly = securityEnabledOnly;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GetTransitiveGroupIdsAsync(string subjectId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);

        var body = new GetMemberGroupsPostRequestBody { SecurityEnabledOnly = _securityEnabledOnly };
        var response = await _graphClient.Users[subjectId].GetMemberGroups
            .PostAsGetMemberGroupsPostResponseAsync(body, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return response?.Value?.ToArray() ?? Array.Empty<string>();
    }
}
