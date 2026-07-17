namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The Microsoft Graph-backed <see cref="ILatticeIdentityDirectory"/>: searches
/// and resolves Entra users and groups so an operator can validate a candidate
/// principal id before granting it access. Delegates every Graph read to the
/// <see cref="IEntraGraphDirectoryClient"/> seam (which authenticates with the
/// app-only token managed by <see cref="EntraGraphTokenProvider"/>), shapes each
/// <see cref="DirectoryPrincipal.Id"/> per the configured
/// <see cref="EntraDirectorySubjectIdSource"/> so validation matches the active
/// authenticator's subject claim, and clamps page sizes to the
/// <see cref="LatticeIdentityDirectoryOptions"/> bounds.
/// <para>
/// Requires the app-only Graph token to hold the <c>User.Read.All</c> and
/// <c>Group.Read.All</c> read scopes. When those scopes are absent (or Graph
/// otherwise denies a query) the provider degrades cleanly - search returns an
/// empty page and resolve returns <c>null</c> - rather than surfacing an
/// unhandled fault.
/// </para>
/// </summary>
public sealed class EntraGraphIdentityDirectory : ILatticeIdentityDirectory
{
    /// <summary>The stable <see cref="ProviderId"/> of the Entra Graph provider.</summary>
    public const string EntraProviderId = "entra";

    private const string UsersPhasePrefix = "U|";
    private const string GroupsPhasePrefix = "G|";

    private readonly IEntraGraphDirectoryClient _client;
    private readonly LatticeIdentityDirectoryOptions _directoryOptions;
    private readonly EntraDirectorySubjectIdSource _subjectIdSource;

    /// <summary>
    /// Initializes a new <see cref="EntraGraphIdentityDirectory"/>.
    /// </summary>
    /// <param name="client">The Microsoft Graph query seam. Must not be <c>null</c>.</param>
    /// <param name="directoryOptions">The provider-neutral page-size bounds. Must not be <c>null</c>.</param>
    /// <param name="subjectIdSource">Which Entra identifier to record as a principal id.</param>
    /// <exception cref="ArgumentNullException"><paramref name="client"/> or <paramref name="directoryOptions"/> is <c>null</c>.</exception>
    internal EntraGraphIdentityDirectory(
        IEntraGraphDirectoryClient client,
        LatticeIdentityDirectoryOptions directoryOptions,
        EntraDirectorySubjectIdSource subjectIdSource = EntraDirectorySubjectIdSource.ObjectId)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(directoryOptions);
        _client = client;
        _directoryOptions = directoryOptions;
        _subjectIdSource = subjectIdSource;
    }

    /// <inheritdoc />
    public string ProviderId => EntraProviderId;

    /// <inheritdoc />
    public string DescribeEntry(DirectoryPrincipalKind? kind)
    {
        var entry = kind switch
        {
            DirectoryPrincipalKind.User => "A valid entry is a user from the connected Entra directory.",
            DirectoryPrincipalKind.Group => "A valid entry is a group from the connected Entra directory.",
            _ => "A valid entry is a user or group from the connected Entra directory.",
        };

        // A group has no user principal name, so a group-only form must not invite
        // searching by one.
        var search = kind == DirectoryPrincipalKind.Group
            ? "Search by name and pick a match."
            : "Search by name or user principal name and pick a match.";

        return $"{entry} {search} {DescribeRecordedIdentifier(kind)}";
    }

    private string DescribeRecordedIdentifier(DirectoryPrincipalKind? kind)
    {
        // The object-id source records the oid for every principal kind, so the
        // id-semantics sentence is kind-independent.
        if (_subjectIdSource != EntraDirectorySubjectIdSource.UserPrincipalName)
        {
            return "The recorded identifier is the Entra object id (oid) - the same value the " +
                   "token's subject claim carries.";
        }

        // Under the user-principal-name source only a user records its UPN; a group
        // always records its object id.
        return kind switch
        {
            DirectoryPrincipalKind.User =>
                "The recorded identifier is its user principal name (the configured subject claim).",
            DirectoryPrincipalKind.Group =>
                "The recorded identifier is the Entra object id.",
            _ =>
                "For a user the recorded identifier is its user principal name (the configured " +
                "subject claim); for a group it is the Entra object id.",
        };
    }

    /// <inheritdoc />
    public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)
    {
        var pageSize = ClampPageSize(query.PageSize);
        var term = query.Term ?? string.Empty;

        return query.Kind switch
        {
            DirectoryPrincipalKind.User => SearchSingleKindAsync(term, pageSize, query.ContinuationToken, DirectoryPrincipalKind.User, cancellationToken),
            DirectoryPrincipalKind.Group => SearchSingleKindAsync(term, pageSize, query.ContinuationToken, DirectoryPrincipalKind.Group, cancellationToken),
            _ => SearchCombinedAsync(term, pageSize, query.ContinuationToken, cancellationToken),
        };
    }

    /// <inheritdoc />
    public async Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principalId);

        try
        {
            var user = await _client.ResolveUserAsync(principalId, cancellationToken).ConfigureAwait(false);
            if (user is not null)
            {
                return MapRecord(user);
            }

            var group = await _client.ResolveGroupAsync(principalId, cancellationToken).ConfigureAwait(false);
            return group is not null ? MapRecord(group) : null;
        }
        catch (EntraDirectoryUnavailableException)
        {
            return null;
        }
    }

    private async Task<DirectorySearchPage> SearchSingleKindAsync(
        string term,
        int pageSize,
        string? continuationToken,
        DirectoryPrincipalKind kind,
        CancellationToken cancellationToken)
    {
        try
        {
            var page = kind == DirectoryPrincipalKind.User
                ? await _client.SearchUsersAsync(term, pageSize, continuationToken, cancellationToken).ConfigureAwait(false)
                : await _client.SearchGroupsAsync(term, pageSize, continuationToken, cancellationToken).ConfigureAwait(false);

            if (page.Records.Count == 0 && page.ContinuationToken is null)
            {
                return DirectorySearchPage.Empty;
            }

            return new DirectorySearchPage(MapRecords(page.Records), page.ContinuationToken);
        }
        catch (EntraDirectoryUnavailableException)
        {
            return DirectorySearchPage.Empty;
        }
    }

    private async Task<DirectorySearchPage> SearchCombinedAsync(
        string term,
        int pageSize,
        string? continuationToken,
        CancellationToken cancellationToken)
    {
        try
        {
            var (groupsPhase, innerToken) = DecodeComposite(continuationToken);

            if (!groupsPhase)
            {
                var users = await _client.SearchUsersAsync(term, pageSize, innerToken, cancellationToken).ConfigureAwait(false);
                var nextToken = users.ContinuationToken is { } t
                    ? UsersPhasePrefix + t
                    : GroupsPhasePrefix;
                return new DirectorySearchPage(MapRecords(users.Records), nextToken);
            }

            var groups = await _client.SearchGroupsAsync(term, pageSize, innerToken, cancellationToken).ConfigureAwait(false);
            var groupsNext = groups.ContinuationToken is { } gt ? GroupsPhasePrefix + gt : null;
            return new DirectorySearchPage(MapRecords(groups.Records), groupsNext);
        }
        catch (EntraDirectoryUnavailableException)
        {
            return DirectorySearchPage.Empty;
        }
    }

    private int ClampPageSize(int requested)
    {
        var size = requested <= 0 ? _directoryOptions.DefaultPageSize : requested;
        return size > _directoryOptions.MaxPageSize ? _directoryOptions.MaxPageSize : size;
    }

    private IReadOnlyList<DirectoryPrincipal> MapRecords(IReadOnlyList<EntraDirectoryRecord> records)
    {
        if (records.Count == 0)
        {
            return Array.Empty<DirectoryPrincipal>();
        }

        var mapped = new DirectoryPrincipal[records.Count];
        for (var i = 0; i < records.Count; i++)
        {
            mapped[i] = MapRecord(records[i]);
        }

        return mapped;
    }

    private DirectoryPrincipal MapRecord(EntraDirectoryRecord record) =>
        new(ShapeId(record), record.DisplayName, record.Kind);

    private string ShapeId(EntraDirectoryRecord record) =>
        record.Kind == DirectoryPrincipalKind.User
            && _subjectIdSource == EntraDirectorySubjectIdSource.UserPrincipalName
            && !string.IsNullOrEmpty(record.UserPrincipalName)
                ? record.UserPrincipalName
                : record.ObjectId;

    private static (bool GroupsPhase, string? InnerToken) DecodeComposite(string? continuationToken)
    {
        if (string.IsNullOrEmpty(continuationToken))
        {
            return (false, null);
        }

        if (continuationToken.StartsWith(GroupsPhasePrefix, StringComparison.Ordinal))
        {
            var inner = continuationToken.Substring(GroupsPhasePrefix.Length);
            return (true, inner.Length == 0 ? null : inner);
        }

        if (continuationToken.StartsWith(UsersPhasePrefix, StringComparison.Ordinal))
        {
            var inner = continuationToken.Substring(UsersPhasePrefix.Length);
            return (false, inner.Length == 0 ? null : inner);
        }

        // An unprefixed token predates a phase boundary; treat it as a users-phase token.
        return (false, continuationToken);
    }
}
