namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// A hand-written <see cref="ILatticeMembershipDirectory"/> fake that returns a
/// fixed transitive-group set for <see cref="GroupsOfAsync"/> and counts how
/// often it was queried, so tests can prove the resolution cache spares storage
/// on a warm hit. All mutating members throw: the context under test only reads.
/// </summary>
internal sealed class CountingDirectory(IReadOnlyCollection<string> groups) : ILatticeMembershipDirectory
{
    public int GroupsOfCalls { get; private set; }

    public int ExpandCalls { get; private set; }

    public Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default)
    {
        GroupsOfCalls++;
        return Task.FromResult(groups);
    }

    public Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default)
    {
        ExpandCalls++;
        // Identity expansion: this fake has no edges, so a seed set expands to
        // itself. Enough for the context's merge/expand plumbing tests.
        return Task.FromResult(seedGroups);
    }

    public Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public IAsyncEnumerable<MembershipGroup> ListGroupsAsync(CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();
}
