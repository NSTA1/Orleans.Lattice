namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// The interceptor's decode of the eighteen delegated tenant access RPCs: each maps
/// onto its own <see cref="LatticeTenantAdminApiOperation"/> and its target tenant is
/// decoded from the request, and none is exempt from authorization (the posture
/// probe included, although its facade answers while the feature is off).
/// </summary>
[TestFixture]
public sealed class TenantAdminGrpcTenantAccessInterceptorMappingTests
{
    private const string Svc = "/orleans.lattice.api.tenantadmin/";

    private static IEnumerable<TestCaseData> Calls()
    {
        var list = new TenantAdminAccessListRequest { TenantId = "acme", Page = new TenantAccessPageRequest() };
        var group = new TenantAdminGroupRequest { TenantId = "acme", GroupName = "ops" };
        var edge = new TenantAdminGroupMemberRequest { TenantId = "acme", GroupName = "ops", MemberId = "bob" };
        var member = new TenantAdminMemberRequest { TenantId = "acme", SubjectId = "bob" };
        var rule = new TenantAdminRuleRequest { TenantId = "acme", RuleId = "r" };

        yield return Case(LatticeTenantAdminGrpcMethods.ListTenantGroupsMethodName, list, LatticeTenantAdminApiOperation.ListTenantGroups);
        yield return Case(LatticeTenantAdminGrpcMethods.GetTenantGroupMethodName, group, LatticeTenantAdminApiOperation.GetTenantGroup);
        yield return Case(
            LatticeTenantAdminGrpcMethods.UpsertTenantGroupMethodName,
            new TenantAdminGroupUpsertRequest { TenantId = "acme", Group = new TenantGroupDescriptor { Name = "ops" } },
            LatticeTenantAdminApiOperation.UpsertTenantGroup);
        yield return Case(LatticeTenantAdminGrpcMethods.RemoveTenantGroupMethodName, group, LatticeTenantAdminApiOperation.RemoveTenantGroup);
        yield return Case(LatticeTenantAdminGrpcMethods.ListTenantGroupMembersMethodName, group, LatticeTenantAdminApiOperation.ListTenantGroupMembers);
        yield return Case(LatticeTenantAdminGrpcMethods.AddTenantGroupMemberMethodName, edge, LatticeTenantAdminApiOperation.AddTenantGroupMember);
        yield return Case(LatticeTenantAdminGrpcMethods.RemoveTenantGroupMemberMethodName, edge, LatticeTenantAdminApiOperation.RemoveTenantGroupMember);
        yield return Case(LatticeTenantAdminGrpcMethods.ListTenantMembersMethodName, list, LatticeTenantAdminApiOperation.ListTenantMembers);
        yield return Case(LatticeTenantAdminGrpcMethods.AddTenantMemberMethodName, member, LatticeTenantAdminApiOperation.AddTenantMember);
        yield return Case(LatticeTenantAdminGrpcMethods.RemoveTenantMemberMethodName, member, LatticeTenantAdminApiOperation.RemoveTenantMember);
        yield return Case(LatticeTenantAdminGrpcMethods.ResolveTenantSubjectMethodName, member, LatticeTenantAdminApiOperation.ResolveTenantSubject);
        yield return Case(
            LatticeTenantAdminGrpcMethods.PutTenantRuleMethodName,
            new TenantAdminRulePutRequest { TenantId = "acme", Rule = new TenantRuleDraft { RuleId = "r", SubjectId = "bob" } },
            LatticeTenantAdminApiOperation.PutTenantRule);
        yield return Case(LatticeTenantAdminGrpcMethods.GetTenantRuleMethodName, rule, LatticeTenantAdminApiOperation.GetTenantRule);
        yield return Case(LatticeTenantAdminGrpcMethods.RemoveTenantRuleMethodName, rule, LatticeTenantAdminApiOperation.RemoveTenantRule);
        yield return Case(LatticeTenantAdminGrpcMethods.ListTenantRulesMethodName, list, LatticeTenantAdminApiOperation.ListTenantRules);
        yield return Case(
            LatticeTenantAdminGrpcMethods.ExplainTenantAccessMethodName,
            new TenantAdminExplainRequest { TenantId = "acme", SubjectId = "bob", TreeName = "orders" },
            LatticeTenantAdminApiOperation.ExplainTenantAccess);
        yield return Case(
            LatticeTenantAdminGrpcMethods.GetTenantEffectivePermissionsMethodName,
            new TenantAdminEffectivePermissionsRequest { TenantId = "acme", SubjectId = "bob" },
            LatticeTenantAdminApiOperation.GetTenantEffectivePermissions);
        yield return Case(
            LatticeTenantAdminGrpcMethods.GetTenantAccessPostureMethodName,
            new TenantAdminTenantRequest { TenantId = "acme" },
            LatticeTenantAdminApiOperation.GetTenantAccessPosture);

        static TestCaseData Case(string method, object request, LatticeTenantAdminApiOperation operation) =>
            new TestCaseData(method, request, operation).SetArgDisplayNames(method);
    }

    [TestCaseSource(nameof(Calls))]
    public void DescribeCall_maps_the_rpc_and_decodes_the_target_tenant(
        string method, object request, LatticeTenantAdminApiOperation operation)
    {
        var described = LatticeTenantAdminApiGrpcAuthInterceptor.DescribeCall(Svc + method, request);

        Assert.That(described, Is.EqualTo((operation, "acme")));
    }

    [TestCaseSource(nameof(Calls))]
    public void The_rpc_is_neither_unauthenticated_nor_self_service_exempt(
        string method, object request, LatticeTenantAdminApiOperation operation)
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsUnauthenticatedMethod(Svc + method), Is.False);
            Assert.That(LatticeTenantAdminApiGrpcAuthInterceptor.IsSelfServiceMethod(Svc + method), Is.False);
        });
    }
}
