namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// A fake <see cref="IEntraGraphDirectoryClient"/> backed by in-memory user and
/// group records. It paginates deterministically (a numeric offset carried in the
/// continuation token), records the arguments of the most recent search, and can
/// be switched to simulate a Graph denial by throwing
/// <see cref="EntraDirectoryUnavailableException"/>. No live Graph call.
/// </summary>
internal sealed class FakeGraphDirectoryClient : IEntraGraphDirectoryClient
{
    private readonly List<EntraDirectoryRecord> _users = new();
    private readonly List<EntraDirectoryRecord> _groups = new();

    /// <summary>When <c>true</c>, every method throws <see cref="EntraDirectoryUnavailableException"/>.</summary>
    public bool Unavailable { get; set; }

    /// <summary>The <c>pageSize</c> passed to the most recent search call.</summary>
    public int? LastPageSize { get; private set; }

    /// <summary>The <c>term</c> passed to the most recent search call.</summary>
    public string? LastTerm { get; private set; }

    public FakeGraphDirectoryClient AddUser(string objectId, string displayName, string? userPrincipalName)
    {
        _users.Add(new EntraDirectoryRecord(objectId, displayName, userPrincipalName, DirectoryPrincipalKind.User));
        return this;
    }

    public FakeGraphDirectoryClient AddGroup(string objectId, string displayName)
    {
        _groups.Add(new EntraDirectoryRecord(objectId, displayName, UserPrincipalName: null, DirectoryPrincipalKind.Group));
        return this;
    }

    /// <inheritdoc />
    public Task<EntraDirectoryPage> SearchUsersAsync(string term, int pageSize, string? continuationToken, CancellationToken cancellationToken)
    {
        LastTerm = term;
        LastPageSize = pageSize;
        return Task.FromResult(Page(_users, pageSize, continuationToken));
    }

    /// <inheritdoc />
    public Task<EntraDirectoryPage> SearchGroupsAsync(string term, int pageSize, string? continuationToken, CancellationToken cancellationToken)
    {
        LastTerm = term;
        LastPageSize = pageSize;
        return Task.FromResult(Page(_groups, pageSize, continuationToken));
    }

    /// <inheritdoc />
    public Task<EntraDirectoryRecord?> ResolveUserAsync(string userId, CancellationToken cancellationToken)
    {
        ThrowIfUnavailable();
        var match = _users.FirstOrDefault(u =>
            string.Equals(u.ObjectId, userId, StringComparison.Ordinal)
            || string.Equals(u.UserPrincipalName, userId, StringComparison.Ordinal));
        return Task.FromResult(match);
    }

    /// <inheritdoc />
    public Task<EntraDirectoryRecord?> ResolveGroupAsync(string groupId, CancellationToken cancellationToken)
    {
        ThrowIfUnavailable();
        var match = _groups.FirstOrDefault(g => string.Equals(g.ObjectId, groupId, StringComparison.Ordinal));
        return Task.FromResult(match);
    }

    private EntraDirectoryPage Page(List<EntraDirectoryRecord> source, int pageSize, string? continuationToken)
    {
        ThrowIfUnavailable();

        var offset = ParseOffset(continuationToken);
        var remaining = source.Count - offset;
        if (remaining <= 0)
        {
            return EntraDirectoryPage.Empty;
        }

        var take = Math.Min(pageSize, remaining);
        var records = source.GetRange(offset, take);
        var nextOffset = offset + take;
        var nextToken = nextOffset < source.Count ? nextOffset.ToString() : null;
        return new EntraDirectoryPage(records, nextToken);
    }

    private void ThrowIfUnavailable()
    {
        if (Unavailable)
        {
            throw new EntraDirectoryUnavailableException("Fake Graph directory is unavailable.");
        }
    }

    private static int ParseOffset(string? continuationToken) =>
        int.TryParse(continuationToken, out var offset) ? offset : 0;
}
