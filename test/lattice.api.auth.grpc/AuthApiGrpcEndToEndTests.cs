using Grpc.Core;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// End-to-end coverage for the auth-API control gRPC surface driven over an
/// in-process <c>TestServer</c> whose silo runs the live
/// <see cref="ILatticeAuthAdmin"/> facade. Exercises the full membership and
/// policy CRUD lifecycle, plus the introspection verbs, through the public
/// <see cref="LatticeAuthApiGrpcClient"/> as the bootstrap administrator. The
/// transport meta-authorizer is left disabled so these tests isolate the wire
/// round-trip and the facade's own administrator check; the meta-authorizer and
/// anonymous-denial paths are covered separately. Proves client / server Orleans
/// serializer parity by construction: every request and response crosses the
/// wire through the shared marshallers.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiGrpcEndToEndTests
{
    private AuthApiGrpcClusterFixture _fixture = null!;
    private GrpcAuthHost _host = null!;
    private LatticeAuthApiGrpcClient _admin = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiGrpcClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync(requireAuthorization: false);
        _admin = _host.ClientFor(AuthApiGrpcClusterFixture.BootstrapAdmin);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task group_and_membership_crud_round_trips_over_the_wire()
    {
        await _admin.UpsertGroupAsync(new AuthGroup { GroupId = "e2e-admins", DisplayName = "Admins" });

        var group = await _admin.GetGroupAsync(new AuthGroupRef { GroupId = "e2e-admins" });
        Assert.That(group.Group, Is.Not.Null);
        Assert.That(group.Group!.DisplayName, Is.EqualTo("Admins"));

        var groups = await _admin.ListGroupsAsync(new AuthPageRequest { PageSize = 100 });
        Assert.That(groups.Entries.Select(g => g.GroupId), Does.Contain("e2e-admins"));

        await _admin.AddMemberAsync(new AuthMemberEdge
        {
            GroupId = "e2e-admins",
            MemberId = "e2e-bob",
            MemberKind = MembershipMemberKind.User,
        });

        var members = await _admin.ListGroupMembersAsync(new AuthGroupRef { GroupId = "e2e-admins" });
        Assert.That(members.Values, Does.Contain("e2e-bob"));

        var memberGroups = await _admin.ListSubjectGroupsAsync(new AuthMemberRef { MemberId = "e2e-bob" });
        Assert.That(memberGroups.Values, Does.Contain("e2e-admins"));

        await _admin.RemoveMemberAsync(new AuthMemberEdge { GroupId = "e2e-admins", MemberId = "e2e-bob" });
        var afterRemoval = await _admin.ListGroupMembersAsync(new AuthGroupRef { GroupId = "e2e-admins" });
        Assert.That(afterRemoval.Values, Does.Not.Contain("e2e-bob"));

        await _admin.RemoveGroupAsync(new AuthGroupRef { GroupId = "e2e-admins" });
        var goneGroup = await _admin.GetGroupAsync(new AuthGroupRef { GroupId = "e2e-admins" });
        Assert.That(goneGroup.Group, Is.Null);
    }

    [Test]
    public async Task rule_crud_round_trips_over_the_wire()
    {
        const string tree = "e2e-rule-tree";
        var rule = new LatticeAuthorizationRule(
            "e2e-rule-1",
            LatticeSubjectSelector.User("e2e-carol"),
            LatticeScope.Tree(tree),
            LatticeOperation.Read,
            LatticeEffect.Allow);

        await _admin.PutRuleAsync(new AuthPutRule { Rule = rule });

        var fetched = await _admin.GetRuleAsync(new AuthRuleRef { TreeId = tree, RuleId = "e2e-rule-1" });
        Assert.That(fetched.Rule, Is.Not.Null);
        Assert.That(fetched.Rule!.Effect, Is.EqualTo(LatticeEffect.Allow));

        var all = await _admin.ListRulesAsync(new AuthPageRequest { PageSize = 100 });
        Assert.That(all.Entries.Select(r => r.RuleId), Does.Contain("e2e-rule-1"));

        var forTree = await _admin.ListRulesForTreeAsync(new AuthTreeRulesPage
        {
            TreeId = tree,
            Page = new AuthPageRequest { PageSize = 100 },
        });
        Assert.That(forTree.Entries.Select(r => r.RuleId), Does.Contain("e2e-rule-1"));

        var removed = await _admin.RemoveRuleAsync(new AuthRuleRef { TreeId = tree, RuleId = "e2e-rule-1" });
        Assert.That(removed.Removed, Is.True);

        var afterRemoval = await _admin.GetRuleAsync(new AuthRuleRef { TreeId = tree, RuleId = "e2e-rule-1" });
        Assert.That(afterRemoval.Rule, Is.Null);

        var removedAgain = await _admin.RemoveRuleAsync(new AuthRuleRef { TreeId = tree, RuleId = "e2e-rule-1" });
        Assert.That(removedAgain.Removed, Is.False, "removing an absent rule reports not-removed");
    }

    [Test]
    public async Task explain_over_the_wire_reflects_an_authored_rule()
    {
        const string tree = "e2e-explain-tree";
        var rule = new LatticeAuthorizationRule(
            "e2e-explain-rule",
            LatticeSubjectSelector.User("e2e-dave"),
            LatticeScope.Tree(tree),
            LatticeOperation.Read,
            LatticeEffect.Allow);

        await _admin.PutRuleAsync(new AuthPutRule { Rule = rule });

        AuthExplanation? allowed = null;
        await AuthApiGrpcClusterFixture.WaitUntilAsync(async () =>
        {
            allowed = await _admin.ExplainAsync(new AuthExplainQuery
            {
                SubjectId = "e2e-dave",
                Operation = LatticeOperation.Read,
                Scope = LatticeScope.Tree(tree),
            });
            return allowed.Allowed;
        }, "the authored Read rule must become visible through Explain");

        Assert.Multiple(() =>
        {
            Assert.That(allowed!.Allowed, Is.True, "the granted Read must be allowed");
            Assert.That(allowed!.MatchedRules.Select(r => r.RuleId), Does.Contain("e2e-explain-rule"));
        });

        var denied = await _admin.ExplainAsync(new AuthExplainQuery
        {
            SubjectId = "e2e-dave",
            Operation = LatticeOperation.Write,
            Scope = LatticeScope.Tree(tree),
        });

        Assert.That(denied.Allowed, Is.False, "the ungranted Write must be denied");
    }

    [Test]
    public async Task effective_permissions_over_the_wire_lists_the_subject_rules()
    {
        const string tree = "e2e-eff-tree";
        await _admin.PutRuleAsync(new AuthPutRule
        {
            Rule = new LatticeAuthorizationRule(
                "e2e-eff-rule",
                LatticeSubjectSelector.User("e2e-erin"),
                LatticeScope.Tree(tree),
                LatticeOperation.Read,
                LatticeEffect.Allow),
        });

        AuthEffectivePermissions? effective = null;
        await AuthApiGrpcClusterFixture.WaitUntilAsync(async () =>
        {
            effective = await _admin.EffectivePermissionsAsync(new AuthSubjectRef { SubjectId = "e2e-erin" });
            return effective.Rules.Any(r => r.RuleId == "e2e-eff-rule");
        }, "the authored rule must become visible through EffectivePermissions");

        Assert.Multiple(() =>
        {
            Assert.That(effective!.SubjectId, Is.EqualTo("e2e-erin"));
            Assert.That(effective!.Rules.Select(r => r.RuleId), Does.Contain("e2e-eff-rule"));
        });
    }
}
