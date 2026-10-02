using System.ComponentModel;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The thin adapter methods the tenant access tools expose: one stateless static
/// shim per tool over <see cref="ILatticeTenantDirectoryAdmin"/> (this file) or
/// <see cref="ILatticeTenantPolicyAdmin"/> (the policy partial). Each resolves its
/// facade from dependency injection (bound by the MCP SDK from the request service
/// provider), marshals the tool arguments onto one facade call, maps the facade's
/// typed failures through <see cref="TenantAccessToolFaults"/>, and projects the
/// result through <see cref="TenantAccessToolMappings"/>.
/// </summary>
/// <remarks>
/// No authorization, confinement, cap or feature-flag logic lives here: the facades
/// own all of it and fail closed. Only the facade call itself sits inside the fault
/// mapping, so a defect in the projection still fails loudly. The methods are held
/// as static method groups, so each tool's delegate is materialised once when the
/// tool group builds its tool list, never per call.
/// </remarks>
internal static partial class TenantAccessToolHandlers
{
    private const string TenantIdDescription =
        "The tenant whose access to administer. Must be a registered tenant other than the reserved default tenant.";

    private const string PageSizeDescription =
        "The maximum entries to return; 0 or omitted for the default page size (100), capped at 1000.";

    private const string PageTokenDescription =
        "The nextPageToken from the previous page, or omitted for the first page.";

    private const string GroupNameDescription =
        "The tenant group's tenant-local name (not the composed t/{tenant}/{name} id).";

    private const string SubjectKindDescription =
        "Whether the subject is a User, one of this tenant's own groups (TenantGroup, named by its tenant-local "
        + "name), or a ClusterGroup. Defaults to User.";

    // ----- Groups -----

    /// <summary>Lists one page of the tenant's own groups.</summary>
    public static async Task<McpTenantGroupListResult> ListGroupsAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(PageSizeDescription)] int pageSize = 0,
        [Description(PageTokenDescription)] string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantGroupPage page;
        try
        {
            page = await directory
                .ListGroupsAsync(tenantId, Page(pageSize, pageToken), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(tenantId, page);
    }

    /// <summary>Reads one of the tenant's groups.</summary>
    public static async Task<McpTenantGroupGetResult> GetGroupAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(GroupNameDescription)] string name,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantGroupDescriptor? group;
        try
        {
            group = await directory.GetGroupAsync(tenantId, name, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcpGet(tenantId, name, group);
    }

    /// <summary>Creates or updates one of the tenant's groups.</summary>
    public static async Task<McpTenantGroupResult> UpsertGroupAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(GroupNameDescription)] string name,
        [Description("An optional human-readable display name for the group.")] string? displayName = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantGroupDescriptor written;
        try
        {
            written = await directory
                .UpsertGroupAsync(tenantId, new TenantGroupDescriptor { Name = name, DisplayName = displayName }, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(tenantId, written);
    }

    /// <summary>Removes one of the tenant's groups and cascades the removal.</summary>
    public static async Task<McpTenantGroupRemovalResult> RemoveGroupAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(GroupNameDescription)] string name,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantGroupRemovalResult result;
        try
        {
            result = await directory.RemoveGroupAsync(tenantId, name, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(result);
    }

    // ----- Group members -----

    /// <summary>Lists the direct members of one of the tenant's groups.</summary>
    public static async Task<McpTenantGroupMembersResult> ListGroupMembersAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(GroupNameDescription)] string groupName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        IReadOnlyList<TenantGroupMember> members;
        try
        {
            members = await directory.ListGroupMembersAsync(tenantId, groupName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(tenantId, groupName, members);
    }

    /// <summary>Adds a direct member to one of the tenant's groups.</summary>
    public static async Task<McpTenantMembershipChangeResult> AddGroupMemberAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(GroupNameDescription)] string groupName,
        [Description("The member to add: a user id, a cluster group id, or one of this tenant's group names, as memberKind says.")] string memberId,
        [Description(SubjectKindDescription)] TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantMembershipChangeResult result;
        try
        {
            result = await directory
                .AddGroupMemberAsync(tenantId, groupName, memberId, memberKind, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(result);
    }

    /// <summary>Removes a direct member from one of the tenant's groups.</summary>
    public static async Task<McpTenantMembershipChangeResult> RemoveGroupMemberAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(GroupNameDescription)] string groupName,
        [Description("The member to remove, as memberKind says.")] string memberId,
        [Description(SubjectKindDescription)] TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantMembershipChangeResult result;
        try
        {
            result = await directory
                .RemoveGroupMemberAsync(tenantId, groupName, memberId, memberKind, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(result);
    }

    // ----- Tenant member set -----

    /// <summary>Lists one page of the tenant member set.</summary>
    public static async Task<McpTenantMemberListResult> ListMembersAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description(PageSizeDescription)] int pageSize = 0,
        [Description(PageTokenDescription)] string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantMemberPage page;
        try
        {
            page = await directory
                .ListMembersAsync(tenantId, Page(pageSize, pageToken), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(tenantId, page);
    }

    /// <summary>Adds an entry to the tenant member set.</summary>
    public static async Task<McpTenantMembershipChangeResult> AddMemberAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description("The subject to add to the member set: a user id, a cluster group id, or one of this tenant's group names, as subjectKind says.")] string subjectId,
        [Description(SubjectKindDescription)] TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantMembershipChangeResult result;
        try
        {
            result = await directory.AddMemberAsync(tenantId, subjectId, subjectKind, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(result);
    }

    /// <summary>Removes an entry from the tenant member set.</summary>
    public static async Task<McpTenantMembershipChangeResult> RemoveMemberAsync(
        ILatticeTenantDirectoryAdmin directory,
        [Description(TenantIdDescription)] string tenantId,
        [Description("The subject to remove from the member set, as subjectKind says.")] string subjectId,
        [Description(SubjectKindDescription)] TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(directory);
        TenantMembershipChangeResult result;
        try
        {
            result = await directory.RemoveMemberAsync(tenantId, subjectId, subjectKind, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (TenantAccessToolFaults.TryTranslate(ex, out var fault))
        {
            throw fault;
        }

        return TenantAccessToolMappings.ToMcp(result);
    }

    private static TenantAccessPageRequest Page(int pageSize, string? pageToken)
        => new() { PageSize = pageSize, PageToken = pageToken };
}
