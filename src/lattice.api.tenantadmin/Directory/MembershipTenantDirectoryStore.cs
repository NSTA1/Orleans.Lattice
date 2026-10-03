using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantDirectoryStore"/>: the registered
/// <see cref="ILatticeMembershipDirectory"/> for group records and edges, and
/// membership's internal <see cref="ITenantScopedMembershipStore"/> for the
/// tenant-scoped counts, the tenant group page and the group removal cascade. Every
/// call runs under system origin; authorization is the facade's job.
/// </summary>
/// <param name="directory">The membership directory. Must not be <c>null</c>.</param>
/// <param name="scoped">The tenant-scoped membership store. Must not be <c>null</c>.</param>
internal sealed class MembershipTenantDirectoryStore(
    ILatticeMembershipDirectory directory,
    ITenantScopedMembershipStore scoped) : ITenantDirectoryStore
{
    private readonly ILatticeMembershipDirectory _directory = directory ?? throw new ArgumentNullException(nameof(directory));
    private readonly ITenantScopedMembershipStore _scoped = scoped ?? throw new ArgumentNullException(nameof(scoped));

    /// <inheritdoc />
    public async Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _directory.GetGroupAsync(groupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory.UpsertGroupAsync(group, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> CountTenantGroupsAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _scoped.CountTenantGroupsAsync(tenant, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> CountTenantEdgesAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _scoped.CountTenantEdgesAsync(tenant, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<DirectoryGroupSlice> ListTenantGroupsAsync(
        TenantId tenant, string? afterGroupId, int pageSize, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var page = await _scoped
                .ListTenantGroupsAsync(tenant, afterGroupId, pageSize, cancellationToken)
                .ConfigureAwait(false);
            return new DirectoryGroupSlice(page.Groups, page.ContinuationAfter);
        }
    }

    /// <inheritdoc />
    public async Task<int> RemoveGroupCascadeAsync(string groupId, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var removed = await _scoped.RemoveGroupCascadeAsync(groupId, cancellationToken).ConfigureAwait(false);
            return removed.Count;
        }
    }

    /// <inheritdoc />
    public async Task AddMemberAsync(
        string groupId, string memberId, MembershipMemberKind memberKind, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory.AddMemberAsync(groupId, memberId, memberKind, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory.RemoveMemberAsync(groupId, memberId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _directory.MembersOfAsync(groupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _directory.GroupsOfAsync(memberId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyCollection<string>> ExpandGroupsAsync(
        IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _directory.ExpandGroupsAsync(seedGroups, cancellationToken).ConfigureAwait(false);
        }
    }
}
