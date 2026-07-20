using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Asserts that the authorization seam describes every auth-API RPC faithfully:
/// each gRPC method maps to its own <see cref="LatticeAuthApiOperation"/> and
/// surfaces the primary id the call administers, the catalog-wide list operations
/// present no single target, and an unrecognised method falls through to
/// <see cref="LatticeAuthApiOperation.Unknown"/> rather than masquerading as a
/// benign operation. This is the seam a host's
/// <see cref="ILatticeAuthApiAuthorizer"/> reasons over, so per-operation /
/// per-target fidelity here is a security property.
/// </summary>
[TestFixture]
public sealed class LatticeAuthApiInterceptorDescribeCallTests
{
    private static string Method(string methodName) =>
        $"/{LatticeAuthApiGrpcMethods.ServiceName}/{methodName}";

    private static (LatticeAuthApiOperation Operation, string? TargetId) Describe<TRequest>(
        string methodName,
        TRequest request) =>
        LatticeAuthApiGrpcAuthInterceptor.DescribeCall(Method(methodName), request);

    private static LatticeAuthorizationRule SampleRule(string treeId) =>
        new("r1", LatticeSubjectSelector.User("u"), LatticeScope.Tree(treeId), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void UpsertUser_targets_the_user_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.UpsertUserMethodName,
            new AuthUser { UserId = "alice" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.UpsertUser));
            Assert.That(result.TargetId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void GetUser_targets_the_user_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.GetUserMethodName,
            new AuthUserRef { UserId = "alice" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.GetUser));
            Assert.That(result.TargetId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void RemoveUser_targets_the_user_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.RemoveUserMethodName,
            new AuthUserRef { UserId = "alice" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.RemoveUser));
            Assert.That(result.TargetId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void ListUsers_has_no_single_target()
    {
        var result = Describe(LatticeAuthApiGrpcMethods.ListUsersMethodName, new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ListUsers));
            Assert.That(result.TargetId, Is.Null, "a catalog-wide list targets no single id");
        });
    }

    [Test]
    public void UpsertGroup_targets_the_group_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.UpsertGroupMethodName,
            new AuthGroup { GroupId = "admins" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.UpsertGroup));
            Assert.That(result.TargetId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void GetGroup_targets_the_group_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.GetGroupMethodName,
            new AuthGroupRef { GroupId = "admins" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.GetGroup));
            Assert.That(result.TargetId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void RemoveGroup_targets_the_group_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.RemoveGroupMethodName,
            new AuthGroupRef { GroupId = "admins" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.RemoveGroup));
            Assert.That(result.TargetId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void ListGroups_has_no_single_target()
    {
        var result = Describe(LatticeAuthApiGrpcMethods.ListGroupsMethodName, new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ListGroups));
            Assert.That(result.TargetId, Is.Null);
        });
    }

    [Test]
    public void AddMember_targets_the_group_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.AddMemberMethodName,
            new AuthMemberEdge { GroupId = "admins", MemberId = "bob" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.AddMember));
            Assert.That(result.TargetId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void RemoveMember_targets_the_group_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.RemoveMemberMethodName,
            new AuthMemberEdge { GroupId = "admins", MemberId = "bob" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.RemoveMember));
            Assert.That(result.TargetId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void ListGroupMembers_targets_the_group_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.ListGroupMembersMethodName,
            new AuthGroupRef { GroupId = "admins" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ListGroupMembers));
            Assert.That(result.TargetId, Is.EqualTo("admins"));
        });
    }

    [Test]
    public void ListSubjectGroups_targets_the_member_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.ListSubjectGroupsMethodName,
            new AuthMemberRef { MemberId = "bob" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ListSubjectGroups));
            Assert.That(result.TargetId, Is.EqualTo("bob"));
        });
    }

    [Test]
    public void PutRule_targets_the_governed_tree_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.PutRuleMethodName,
            new AuthPutRule { Rule = SampleRule("policy-tree") });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.PutRule));
            Assert.That(result.TargetId, Is.EqualTo("policy-tree"));
        });
    }

    [Test]
    public void GetRule_targets_the_tree_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.GetRuleMethodName,
            new AuthRuleRef { TreeId = "policy-tree", RuleId = "r1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.GetRule));
            Assert.That(result.TargetId, Is.EqualTo("policy-tree"));
        });
    }

    [Test]
    public void RemoveRule_targets_the_tree_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.RemoveRuleMethodName,
            new AuthRuleRef { TreeId = "policy-tree", RuleId = "r1" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.RemoveRule));
            Assert.That(result.TargetId, Is.EqualTo("policy-tree"));
        });
    }

    [Test]
    public void ListRules_has_no_single_target()
    {
        var result = Describe(LatticeAuthApiGrpcMethods.ListRulesMethodName, new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ListRules));
            Assert.That(result.TargetId, Is.Null);
        });
    }

    [Test]
    public void ListRulesForTree_targets_the_tree_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.ListRulesForTreeMethodName,
            new AuthTreeRulesPage { TreeId = "policy-tree" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ListRulesForTree));
            Assert.That(result.TargetId, Is.EqualTo("policy-tree"));
        });
    }

    [Test]
    public void Explain_targets_the_subject_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.ExplainMethodName,
            new AuthExplainQuery
            {
                SubjectId = "alice",
                Operation = LatticeOperation.Read,
                Scope = LatticeScope.Tree("t"),
            });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.Explain));
            Assert.That(result.TargetId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void EffectivePermissions_targets_the_subject_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.EffectivePermissionsMethodName,
            new AuthSubjectRef { SubjectId = "alice" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.EffectivePermissions));
            Assert.That(result.TargetId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void SearchDirectory_has_no_single_target()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.SearchDirectoryMethodName,
            new DirectorySearchRequest { Term = "al" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.SearchDirectory));
            Assert.That(result.TargetId, Is.Null, "a directory search targets no single id");
        });
    }

    [Test]
    public void ResolveDirectoryPrincipal_targets_the_principal_id()
    {
        var result = Describe(
            LatticeAuthApiGrpcMethods.ResolveDirectoryPrincipalMethodName,
            new AuthPrincipalRef { PrincipalId = "alice@contoso.com" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.ResolveDirectoryPrincipal));
            Assert.That(result.TargetId, Is.EqualTo("alice@contoso.com"));
        });
    }

    [Test]
    public void GetAccessModel_has_no_single_target()
    {
        var result = Describe(LatticeAuthApiGrpcMethods.GetAccessModelMethodName, new AuthAccessModelQuery());

        Assert.Multiple(() =>
        {
            Assert.That(result.Operation, Is.EqualTo(LatticeAuthApiOperation.GetAccessModel));
            Assert.That(result.TargetId, Is.Null, "an access-model read targets no single id");
        });
    }

    [Test]
    public void An_unrecognised_method_maps_to_Unknown()
    {
        var result = Describe("SomeFutureRpc", new AuthUserRef { UserId = "alice" });

        Assert.That(
            result.Operation,
            Is.EqualTo(LatticeAuthApiOperation.Unknown),
            "An unmapped method must never default to a benign operation.");
    }
}
