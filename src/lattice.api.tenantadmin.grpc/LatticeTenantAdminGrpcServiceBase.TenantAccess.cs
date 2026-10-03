using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The delegated tenant access administration RPCs of the tenant-administration
/// control-API service: eleven over <see cref="ILatticeTenantDirectoryAdmin"/> and
/// seven over <see cref="ILatticeTenantPolicyAdmin"/>. Every one is an
/// <b>operator-or-tenant-admin</b> action enforced fail-closed by its facade, and
/// stays behind the transport authorization interceptor.
/// </summary>
internal abstract partial class LatticeTenantAdminGrpcServiceBase
{
    /// <summary>Reads one page of a tenant's groups. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantGroupPage> ListTenantGroups(TenantAdminAccessListRequest request, ServerCallContext context);

    /// <summary>Reads one of a tenant's groups by local name. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantAdminGroupLookup> GetTenantGroup(TenantAdminGroupRequest request, ServerCallContext context);

    /// <summary>Creates or replaces one of a tenant's groups. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantGroupDescriptor> UpsertTenantGroup(TenantAdminGroupUpsertRequest request, ServerCallContext context);

    /// <summary>Removes one of a tenant's groups, with its cascade. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantGroupRemovalResult> RemoveTenantGroup(TenantAdminGroupRequest request, ServerCallContext context);

    /// <summary>Lists the direct members of one of a tenant's groups. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantAdminGroupMemberList> ListTenantGroupMembers(TenantAdminGroupRequest request, ServerCallContext context);

    /// <summary>Adds a direct member to one of a tenant's groups. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantMembershipChangeResult> AddTenantGroupMember(TenantAdminGroupMemberRequest request, ServerCallContext context);

    /// <summary>Removes a direct member from one of a tenant's groups. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantMembershipChangeResult> RemoveTenantGroupMember(TenantAdminGroupMemberRequest request, ServerCallContext context);

    /// <summary>Reads one page of a tenant's member set. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantMemberPage> ListTenantMembers(TenantAdminAccessListRequest request, ServerCallContext context);

    /// <summary>Adds an entry to a tenant's member set. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantMembershipChangeResult> AddTenantMember(TenantAdminMemberRequest request, ServerCallContext context);

    /// <summary>Removes an entry from a tenant's member set. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantMembershipChangeResult> RemoveTenantMember(TenantAdminMemberRequest request, ServerCallContext context);

    /// <summary>Resolves a subject's standing in a tenant. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantSubjectResolution> ResolveTenantSubject(TenantAdminMemberRequest request, ServerCallContext context);

    /// <summary>Creates or replaces a tenant-tier rule. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantRuleView> PutTenantRule(TenantAdminRulePutRequest request, ServerCallContext context);

    /// <summary>Reads one of a tenant's tenant-tier rules by local id. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantAdminRuleLookup> GetTenantRule(TenantAdminRuleRequest request, ServerCallContext context);

    /// <summary>Removes one of a tenant's tenant-tier rules by local id. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantAdminRuleRemoval> RemoveTenantRule(TenantAdminRuleRequest request, ServerCallContext context);

    /// <summary>Reads one page of the rules governing a tenant. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantRulePage> ListTenantRules(TenantAdminAccessListRequest request, ServerCallContext context);

    /// <summary>Explains a decision on one of a tenant's trees. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantExplanation> ExplainTenantAccess(TenantAdminExplainRequest request, ServerCallContext context);

    /// <summary>Reads a subject's effective permissions on a tenant's trees. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.</summary>
    public abstract Task<TenantEffectivePermissions> GetTenantEffectivePermissions(TenantAdminEffectivePermissionsRequest request, ServerCallContext context);

    /// <summary>
    /// Reads a tenant's access posture. Answers while delegated tenant access
    /// administration is disabled. Implemented in <see cref="LatticeTenantAdminGrpcService"/>.
    /// </summary>
    public abstract Task<TenantAccessPosture> GetTenantAccessPosture(TenantAdminTenantRequest request, ServerCallContext context);

    /// <summary>
    /// Records the metadata of the delegated tenant access RPCs on the metadata-only
    /// binding pass <see cref="BindService"/> makes with no service instance.
    /// </summary>
    private static void BindTenantAccessMetadata(ServiceBinderBase binder, LatticeTenantAdminGrpcMethods methods)
    {
        binder.AddMethod(methods.ListTenantGroups, (UnaryServerMethod<TenantAdminAccessListRequest, TenantGroupPage>?)null);
        binder.AddMethod(methods.GetTenantGroup, (UnaryServerMethod<TenantAdminGroupRequest, TenantAdminGroupLookup>?)null);
        binder.AddMethod(methods.UpsertTenantGroup, (UnaryServerMethod<TenantAdminGroupUpsertRequest, TenantGroupDescriptor>?)null);
        binder.AddMethod(methods.RemoveTenantGroup, (UnaryServerMethod<TenantAdminGroupRequest, TenantGroupRemovalResult>?)null);
        binder.AddMethod(methods.ListTenantGroupMembers, (UnaryServerMethod<TenantAdminGroupRequest, TenantAdminGroupMemberList>?)null);
        binder.AddMethod(methods.AddTenantGroupMember, (UnaryServerMethod<TenantAdminGroupMemberRequest, TenantMembershipChangeResult>?)null);
        binder.AddMethod(methods.RemoveTenantGroupMember, (UnaryServerMethod<TenantAdminGroupMemberRequest, TenantMembershipChangeResult>?)null);
        binder.AddMethod(methods.ListTenantMembers, (UnaryServerMethod<TenantAdminAccessListRequest, TenantMemberPage>?)null);
        binder.AddMethod(methods.AddTenantMember, (UnaryServerMethod<TenantAdminMemberRequest, TenantMembershipChangeResult>?)null);
        binder.AddMethod(methods.RemoveTenantMember, (UnaryServerMethod<TenantAdminMemberRequest, TenantMembershipChangeResult>?)null);
        binder.AddMethod(methods.ResolveTenantSubject, (UnaryServerMethod<TenantAdminMemberRequest, TenantSubjectResolution>?)null);
        binder.AddMethod(methods.PutTenantRule, (UnaryServerMethod<TenantAdminRulePutRequest, TenantRuleView>?)null);
        binder.AddMethod(methods.GetTenantRule, (UnaryServerMethod<TenantAdminRuleRequest, TenantAdminRuleLookup>?)null);
        binder.AddMethod(methods.RemoveTenantRule, (UnaryServerMethod<TenantAdminRuleRequest, TenantAdminRuleRemoval>?)null);
        binder.AddMethod(methods.ListTenantRules, (UnaryServerMethod<TenantAdminAccessListRequest, TenantRulePage>?)null);
        binder.AddMethod(methods.ExplainTenantAccess, (UnaryServerMethod<TenantAdminExplainRequest, TenantExplanation>?)null);
        binder.AddMethod(methods.GetTenantEffectivePermissions, (UnaryServerMethod<TenantAdminEffectivePermissionsRequest, TenantEffectivePermissions>?)null);
        binder.AddMethod(methods.GetTenantAccessPosture, (UnaryServerMethod<TenantAdminTenantRequest, TenantAccessPosture>?)null);
    }

    /// <summary>Binds the delegated tenant access RPCs to <paramref name="serviceImpl"/>.</summary>
    private static void BindTenantAccess(
        ServiceBinderBase binder, LatticeTenantAdminGrpcMethods methods, LatticeTenantAdminGrpcServiceBase serviceImpl)
    {
        binder.AddMethod(methods.ListTenantGroups, new UnaryServerMethod<TenantAdminAccessListRequest, TenantGroupPage>(serviceImpl.ListTenantGroups));
        binder.AddMethod(methods.GetTenantGroup, new UnaryServerMethod<TenantAdminGroupRequest, TenantAdminGroupLookup>(serviceImpl.GetTenantGroup));
        binder.AddMethod(methods.UpsertTenantGroup, new UnaryServerMethod<TenantAdminGroupUpsertRequest, TenantGroupDescriptor>(serviceImpl.UpsertTenantGroup));
        binder.AddMethod(methods.RemoveTenantGroup, new UnaryServerMethod<TenantAdminGroupRequest, TenantGroupRemovalResult>(serviceImpl.RemoveTenantGroup));
        binder.AddMethod(methods.ListTenantGroupMembers, new UnaryServerMethod<TenantAdminGroupRequest, TenantAdminGroupMemberList>(serviceImpl.ListTenantGroupMembers));
        binder.AddMethod(methods.AddTenantGroupMember, new UnaryServerMethod<TenantAdminGroupMemberRequest, TenantMembershipChangeResult>(serviceImpl.AddTenantGroupMember));
        binder.AddMethod(methods.RemoveTenantGroupMember, new UnaryServerMethod<TenantAdminGroupMemberRequest, TenantMembershipChangeResult>(serviceImpl.RemoveTenantGroupMember));
        binder.AddMethod(methods.ListTenantMembers, new UnaryServerMethod<TenantAdminAccessListRequest, TenantMemberPage>(serviceImpl.ListTenantMembers));
        binder.AddMethod(methods.AddTenantMember, new UnaryServerMethod<TenantAdminMemberRequest, TenantMembershipChangeResult>(serviceImpl.AddTenantMember));
        binder.AddMethod(methods.RemoveTenantMember, new UnaryServerMethod<TenantAdminMemberRequest, TenantMembershipChangeResult>(serviceImpl.RemoveTenantMember));
        binder.AddMethod(methods.ResolveTenantSubject, new UnaryServerMethod<TenantAdminMemberRequest, TenantSubjectResolution>(serviceImpl.ResolveTenantSubject));
        binder.AddMethod(methods.PutTenantRule, new UnaryServerMethod<TenantAdminRulePutRequest, TenantRuleView>(serviceImpl.PutTenantRule));
        binder.AddMethod(methods.GetTenantRule, new UnaryServerMethod<TenantAdminRuleRequest, TenantAdminRuleLookup>(serviceImpl.GetTenantRule));
        binder.AddMethod(methods.RemoveTenantRule, new UnaryServerMethod<TenantAdminRuleRequest, TenantAdminRuleRemoval>(serviceImpl.RemoveTenantRule));
        binder.AddMethod(methods.ListTenantRules, new UnaryServerMethod<TenantAdminAccessListRequest, TenantRulePage>(serviceImpl.ListTenantRules));
        binder.AddMethod(methods.ExplainTenantAccess, new UnaryServerMethod<TenantAdminExplainRequest, TenantExplanation>(serviceImpl.ExplainTenantAccess));
        binder.AddMethod(methods.GetTenantEffectivePermissions, new UnaryServerMethod<TenantAdminEffectivePermissionsRequest, TenantEffectivePermissions>(serviceImpl.GetTenantEffectivePermissions));
        binder.AddMethod(methods.GetTenantAccessPosture, new UnaryServerMethod<TenantAdminTenantRequest, TenantAccessPosture>(serviceImpl.GetTenantAccessPosture));
    }
}
