namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The Microsoft Graph-backed <see cref="IEntraGroupResolver"/>. It delegates to
/// the Graph transitive-group query behind <see cref="IEntraGraphMemberGroupsClient"/>,
/// which authenticates with the app-only token managed (cached and refreshed) by
/// <see cref="EntraGraphTokenProvider"/>. Consulted only on the Entra
/// groups-overage path, so an application that never overflows its groups claim
/// makes no Graph call.
/// </summary>
internal sealed class GraphEntraGroupResolver : IEntraGroupResolver
{
    private readonly IEntraGraphMemberGroupsClient _client;

    /// <summary>
    /// Initializes a new <see cref="GraphEntraGroupResolver"/>.
    /// </summary>
    /// <param name="client">The Graph transitive-group query seam. Must not be <c>null</c>.</param>
    public GraphEntraGroupResolver(IEntraGraphMemberGroupsClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyCollection<string>> ResolveGroupsAsync(
        EntraGroupResolutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        return await _client.GetTransitiveGroupIdsAsync(context.SubjectId, cancellationToken).ConfigureAwait(false);
    }
}
