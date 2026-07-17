using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// An <see cref="ILatticeIdentityDirectory"/> backed by an explicitly-configured,
/// in-memory roster (<see cref="StaticIdentityDirectoryOptions"/>). Intended for
/// deployments with no queryable external directory - primarily the reference
/// Basic / environment-variable authorizer, whose valid usernames are provisioned
/// as <c>LATTICE_STATE_USER_&lt;name&gt;</c> credentials at deploy time - so the
/// deployed principal set stays browsable. <see cref="ResolveAsync"/> is an
/// exact-id lookup; <see cref="SearchAsync"/> filters the roster by a
/// case-insensitive term and an optional kind, honouring the page-size and
/// continuation conventions of <see cref="LatticeIdentityDirectoryOptions"/>.
/// Overrides the default <see cref="NullIdentityDirectory"/> with a last-wins
/// registration via
/// <see cref="LatticeMembershipServiceCollectionExtensions.AddStaticIdentityDirectory(Orleans.Hosting.ISiloBuilder, Action{StaticIdentityDirectoryOptions})"/>.
/// </summary>
public sealed class StaticIdentityDirectory : ILatticeIdentityDirectory
{
    /// <summary>The stable <see cref="ProviderId"/> of the static-roster provider.</summary>
    public const string StaticProviderId = "static";

    private static readonly Task<DirectorySearchPage> EmptyPageResult = Task.FromResult(DirectorySearchPage.Empty);
    private static readonly Task<DirectoryPrincipal?> NullPrincipalResult = Task.FromResult<DirectoryPrincipal?>(null);

    // A snapshot of the roster taken at construction: the ordered array drives
    // deterministic search paging, the dictionary drives O(1) exact-id resolve.
    private readonly DirectoryPrincipal[] _ordered;
    private readonly Dictionary<string, DirectoryPrincipal> _byId;
    private readonly IOptions<LatticeIdentityDirectoryOptions> _directoryOptions;

    /// <summary>
    /// Initialises the provider from the configured roster and the shared
    /// identity-directory paging options.
    /// </summary>
    /// <param name="rosterOptions">The static roster to surface.</param>
    /// <param name="directoryOptions">The page-size options applied to searches.</param>
    /// <exception cref="ArgumentNullException"><paramref name="rosterOptions"/> or
    /// <paramref name="directoryOptions"/> is <c>null</c>.</exception>
    public StaticIdentityDirectory(
        IOptions<StaticIdentityDirectoryOptions> rosterOptions,
        IOptions<LatticeIdentityDirectoryOptions> directoryOptions)
    {
        ArgumentNullException.ThrowIfNull(rosterOptions);
        ArgumentNullException.ThrowIfNull(directoryOptions);

        _directoryOptions = directoryOptions;

        // Snapshot the roster, de-duplicating by exact id (last declaration wins)
        // while preserving first-seen order for stable paging.
        var byId = new Dictionary<string, DirectoryPrincipal>(StringComparer.Ordinal);
        var order = new List<string>();
        foreach (var principal in rosterOptions.Value.Principals)
        {
            if (!byId.ContainsKey(principal.Id))
            {
                order.Add(principal.Id);
            }

            byId[principal.Id] = principal;
        }

        _byId = byId;
        _ordered = new DirectoryPrincipal[order.Count];
        for (var i = 0; i < order.Count; i++)
        {
            _ordered[i] = byId[order[i]];
        }
    }

    /// <inheritdoc />
    public string ProviderId => StaticProviderId;

    /// <inheritdoc />
    public string DescribeEntry(DirectoryPrincipalKind? kind) =>
        "Enter an exact id provisioned at deployment time - a static-roster principal or a " +
        "LATTICE_STATE_USER_<name> Basic credential - not an arbitrary string. Ids that are " +
        "not in the deployed roster are rejected.";

    /// <inheritdoc />
    public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var options = _directoryOptions.Value;
        var pageSize = query.PageSize <= 0
            ? options.DefaultPageSize
            : Math.Min(query.PageSize, options.MaxPageSize);

        var start = ParseContinuationToken(query.ContinuationToken);
        var term = query.Term;
        var kind = query.Kind;

        List<DirectoryPrincipal>? matches = null;
        var matchIndex = 0;
        var nextStart = -1;

        for (var i = 0; i < _ordered.Length; i++)
        {
            var principal = _ordered[i];
            if (!Matches(principal, term, kind))
            {
                continue;
            }

            // Skip matches consumed by prior pages.
            if (matchIndex++ < start)
            {
                continue;
            }

            if (matches is { Count: var count } && count == pageSize)
            {
                // One match beyond the page: there is a next page starting here.
                nextStart = start + pageSize;
                break;
            }

            (matches ??= new List<DirectoryPrincipal>(pageSize)).Add(principal);
        }

        if (matches is null)
        {
            return EmptyPageResult;
        }

        var token = nextStart >= 0 ? nextStart.ToString(CultureInfo.InvariantCulture) : null;
        return Task.FromResult(new DirectorySearchPage(matches, token));
    }

    /// <inheritdoc />
    public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principalId);
        cancellationToken.ThrowIfCancellationRequested();

        return _byId.TryGetValue(principalId, out var principal)
            ? Task.FromResult<DirectoryPrincipal?>(principal)
            : NullPrincipalResult;
    }

    private static bool Matches(DirectoryPrincipal principal, string term, DirectoryPrincipalKind? kind)
    {
        if (kind is { } required && principal.Kind != required)
        {
            return false;
        }

        if (string.IsNullOrEmpty(term))
        {
            return true;
        }

        return principal.Id.Contains(term, StringComparison.OrdinalIgnoreCase)
            || principal.DisplayName.Contains(term, StringComparison.OrdinalIgnoreCase);
    }

    private static int ParseContinuationToken(string? token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return 0;
        }

        // An opaque, provider-owned offset. A malformed token restarts from the
        // first page rather than throwing, so a stale cursor degrades gracefully.
        return int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out var start) && start > 0
            ? start
            : 0;
    }
}
