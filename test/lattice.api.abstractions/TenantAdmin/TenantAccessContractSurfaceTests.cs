using System.Reflection;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Pins the two delegated tenant-access contracts every facade, binding and
/// Explorer page of epic #4154 binds to, so a change to either is a deliberate,
/// reviewed edit here rather than a silent drift between the items that implement
/// and consume them. Also enforces the shape every member shares: an explicit
/// tenant id first and a defaulted cancellation token last.
/// </summary>
[TestFixture]
public sealed class TenantAccessContractSurfaceTests
{
    private static readonly string[] DirectoryAdminMembers =
    [
        "Task<TenantGroupPage> ListGroupsAsync(String tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)",
        "Task<TenantGroupDescriptor?> GetGroupAsync(String tenantId, String groupName, CancellationToken cancellationToken = default)",
        "Task<TenantGroupDescriptor> UpsertGroupAsync(String tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)",
        "Task<TenantGroupRemovalResult> RemoveGroupAsync(String tenantId, String groupName, CancellationToken cancellationToken = default)",
        "Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(String tenantId, String groupName, CancellationToken cancellationToken = default)",
        "Task<TenantMembershipChangeResult> AddGroupMemberAsync(String tenantId, String groupName, String memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
        "Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(String tenantId, String groupName, String memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
        "Task<TenantMemberPage> ListMembersAsync(String tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)",
        "Task<TenantMembershipChangeResult> AddMemberAsync(String tenantId, String subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
        "Task<TenantMembershipChangeResult> RemoveMemberAsync(String tenantId, String subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
        "Task<TenantSubjectResolution> ResolveSubjectAsync(String tenantId, String subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
    ];

    private static readonly string[] PolicyAdminMembers =
    [
        "Task<TenantRuleView> PutRuleAsync(String tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)",
        "Task<TenantRuleView?> GetRuleAsync(String tenantId, String ruleId, CancellationToken cancellationToken = default)",
        "Task<Boolean> RemoveRuleAsync(String tenantId, String ruleId, CancellationToken cancellationToken = default)",
        "Task<TenantRulePage> ListRulesAsync(String tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)",
        "Task<TenantExplanation> ExplainAsync(String tenantId, String subjectId, String treeName, String? key, LatticeOperation operation, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
        "Task<TenantEffectivePermissions> EffectivePermissionsAsync(String tenantId, String subjectId, String? treeName = null, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)",
        "Task<TenantAccessPosture> GetPostureAsync(String tenantId, CancellationToken cancellationToken = default)",
    ];

    [Test]
    public void ILatticeTenantDirectoryAdmin_has_exactly_the_agreed_members() =>
        Assert.That(InterfaceSignature.Render(typeof(ILatticeTenantDirectoryAdmin)), Is.EquivalentTo(DirectoryAdminMembers));

    [Test]
    public void ILatticeTenantPolicyAdmin_has_exactly_the_agreed_members() =>
        Assert.That(InterfaceSignature.Render(typeof(ILatticeTenantPolicyAdmin)), Is.EquivalentTo(PolicyAdminMembers));

    [TestCase(typeof(ILatticeTenantDirectoryAdmin))]
    [TestCase(typeof(ILatticeTenantPolicyAdmin))]
    public void Every_member_names_its_tenant_first_and_takes_a_defaulted_cancellation_token_last(Type contract)
    {
        var methods = contract.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);

        Assert.That(methods, Is.Not.Empty);
        Assert.Multiple(() =>
        {
            foreach (var method in methods)
            {
                var parameters = method.GetParameters();
                Assert.That(parameters[0].Name, Is.EqualTo("tenantId"), method.Name);
                Assert.That(parameters[0].ParameterType, Is.EqualTo(typeof(string)), method.Name);
                Assert.That(parameters[^1].ParameterType, Is.EqualTo(typeof(CancellationToken)), method.Name);
                Assert.That(parameters[^1].HasDefaultValue, Is.True, method.Name);
            }
        });
    }

    [TestCase(typeof(ILatticeTenantDirectoryAdmin))]
    [TestCase(typeof(ILatticeTenantPolicyAdmin))]
    public void The_delegated_contracts_are_public_interfaces_in_the_tenant_admin_namespace(Type contract) =>
        Assert.Multiple(() =>
        {
            Assert.That(contract.IsInterface && contract.IsPublic, Is.True);
            Assert.That(contract.Namespace, Is.EqualTo("Orleans.Lattice.Api.TenantAdmin"));
        });
}
