using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeTenantDirectoryAdmin"/> over gRPC: a per-circuit
/// adapter over <see cref="LatticeTenantAdminApiGrpcClient"/>'s tenant group, group
/// member and member-set RPCs. Every call carries the circuit's sign-in and asserts
/// its active tenant through <see cref="ShellTransportChannel"/>, and faults map
/// through <see cref="ShellTenantAccessFaults"/>, so a page sees the facade's typed
/// refusals.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellTenantDirectoryAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTenantAdminApiGrpcClient>(channel, LatticeTenantAdminApiGrpcClient.Create), ILatticeTenantDirectoryAdmin
{
    /// <inheritdoc />
    public Task<TenantGroupPage> ListGroupsAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(page);
        return CallAsync(
            (TenantId: tenantId, Page: page),
            static (client, state, ct) => client.ListGroupsAsync(state.TenantId, state.Page, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGroupDescriptor?> GetGroupAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        return CallAsync(
            (TenantId: tenantId, GroupName: groupName),
            static (client, state, ct) => client.GetGroupAsync(state.TenantId, state.GroupName, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGroupDescriptor> UpsertGroupAsync(string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(group);
        return CallAsync(
            (TenantId: tenantId, Group: group),
            static (client, state, ct) => client.UpsertGroupAsync(state.TenantId, state.Group, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGroupRemovalResult> RemoveGroupAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        return CallAsync(
            (TenantId: tenantId, GroupName: groupName),
            static (client, state, ct) => client.RemoveGroupAsync(state.TenantId, state.GroupName, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        return CallAsync(
            (TenantId: tenantId, GroupName: groupName),
            static (client, state, ct) => client.ListGroupMembersAsync(state.TenantId, state.GroupName, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> AddGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        return CallAsync(
            (TenantId: tenantId, GroupName: groupName, MemberId: memberId, Kind: memberKind),
            static (client, state, ct) => client.AddGroupMemberAsync(state.TenantId, state.GroupName, state.MemberId, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        return CallAsync(
            (TenantId: tenantId, GroupName: groupName, MemberId: memberId, Kind: memberKind),
            static (client, state, ct) => client.RemoveGroupMemberAsync(state.TenantId, state.GroupName, state.MemberId, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantMemberPage> ListMembersAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(page);
        return CallAsync(
            (TenantId: tenantId, Page: page),
            static (client, state, ct) => client.ListMembersAsync(state.TenantId, state.Page, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> AddMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId, Kind: subjectKind),
            static (client, state, ct) => client.AddMemberAsync(state.TenantId, state.SubjectId, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> RemoveMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId, Kind: subjectKind),
            static (client, state, ct) => client.RemoveMemberAsync(state.TenantId, state.SubjectId, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantSubjectResolution> ResolveSubjectAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            (TenantId: tenantId, SubjectId: subjectId, Kind: subjectKind),
            static (client, state, ct) => client.ResolveSubjectAsync(state.TenantId, state.SubjectId, state.Kind, ct),
            tenantId,
            cancellationToken);
    }

    /// <inheritdoc />
    protected override Exception MapFault(RpcException exception, string? subject, CancellationToken cancellationToken) =>
        ShellTenantAccessFaults.Map(exception, subject, cancellationToken);
}
