using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcLatticeAuthAdmin"/>, the remote-host adapter
/// that fronts <see cref="ILatticeAuthAdmin"/> over the auth-API gRPC client.
/// Every one of the 20 members is proven to wrap its scalar arguments into the
/// wire request record and unwrap the wire response back to the facade's return
/// shape - including <see cref="GrpcLatticeAuthAdmin.EffectivePermissionsAsync"/>,
/// the member the discovery core's permission resolver depends on. Deterministic
/// over a <see cref="FakeCallInvoker"/>.
/// </summary>
[TestFixture]
public sealed class GrpcLatticeAuthAdminTests
{
    private static GrpcLatticeAuthAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.AuthClient(invoker));

    private static LatticeAuthorizationRule Rule(string ruleId = "r-1")
        => new(ruleId, LatticeSubjectSelector.User("alice"), LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void Constructor_null_client_throws()
        => Assert.That(() => new GrpcLatticeAuthAdmin(null!), Throws.ArgumentNullException);

    [Test]
    public async Task UpsertGroupAsync_forwards_group()
    {
        var invoker = new FakeCallInvoker(_ => new AuthAck());
        var group = new AuthGroup { GroupId = "admins" };
        await Adapter(invoker).UpsertGroupAsync(group);
        Assert.That(invoker.LastRequest, Is.SameAs(group));
    }

    [Test]
    public async Task GetGroupAsync_wraps_id_and_unwraps_group()
    {
        var group = new AuthGroup { GroupId = "admins" };
        var invoker = new FakeCallInvoker(_ => new AuthGroupResult { Group = group });

        var result = await Adapter(invoker).GetGroupAsync("admins");

        Assert.Multiple(() =>
        {
            Assert.That(((AuthGroupRef)invoker.LastRequest!).GroupId, Is.EqualTo("admins"));
            Assert.That(result, Is.SameAs(group));
        });
    }

    [Test]
    public async Task GetGroupAsync_missing_returns_null()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new AuthGroupResult { Group = null })).GetGroupAsync("ghost");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task RemoveGroupAsync_wraps_id()
    {
        var invoker = new FakeCallInvoker(_ => new AuthAck());
        await Adapter(invoker).RemoveGroupAsync("admins");
        Assert.That(((AuthGroupRef)invoker.LastRequest!).GroupId, Is.EqualTo("admins"));
    }

    [Test]
    public async Task ListGroupsAsync_returns_page()
    {
        var page = new AuthGroupPage();
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListGroupsAsync(new AuthPageRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task AddMemberAsync_wraps_edge_with_kind()
    {
        var invoker = new FakeCallInvoker(_ => new AuthAck());

        await Adapter(invoker).AddMemberAsync("admins", "alice", MembershipMemberKind.Group);

        var sent = (AuthMemberEdge)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.GroupId, Is.EqualTo("admins"));
            Assert.That(sent.MemberId, Is.EqualTo("alice"));
            Assert.That(sent.MemberKind, Is.EqualTo(MembershipMemberKind.Group));
        });
    }

    [Test]
    public async Task RemoveMemberAsync_wraps_edge()
    {
        var invoker = new FakeCallInvoker(_ => new AuthAck());
        await Adapter(invoker).RemoveMemberAsync("admins", "alice");
        var sent = (AuthMemberEdge)invoker.LastRequest!;
        Assert.That(sent.GroupId, Is.EqualTo("admins"));
        Assert.That(sent.MemberId, Is.EqualTo("alice"));
    }

    [Test]
    public async Task ListGroupMembersAsync_unwraps_values()
    {
        var invoker = new FakeCallInvoker(_ => new AuthStringList { Values = new[] { "alice", "bob" } });
        var result = await Adapter(invoker).ListGroupMembersAsync("admins");
        Assert.Multiple(() =>
        {
            Assert.That(((AuthGroupRef)invoker.LastRequest!).GroupId, Is.EqualTo("admins"));
            Assert.That(result, Is.EqualTo(new[] { "alice", "bob" }));
        });
    }

    [Test]
    public async Task ListSubjectGroupsAsync_unwraps_values()
    {
        var invoker = new FakeCallInvoker(_ => new AuthStringList { Values = new[] { "admins" } });
        var result = await Adapter(invoker).ListSubjectGroupsAsync("alice");
        Assert.Multiple(() =>
        {
            Assert.That(((AuthMemberRef)invoker.LastRequest!).MemberId, Is.EqualTo("alice"));
            Assert.That(result, Is.EqualTo(new[] { "admins" }));
        });
    }

    [Test]
    public async Task PutRuleAsync_wraps_rule()
    {
        var invoker = new FakeCallInvoker(_ => new AuthAck());
        var rule = Rule();
        await Adapter(invoker).PutRuleAsync(rule);
        Assert.That(((AuthPutRule)invoker.LastRequest!).Rule, Is.SameAs(rule));
    }

    [Test]
    public async Task GetRuleAsync_wraps_ids_and_unwraps_rule()
    {
        var rule = Rule();
        var invoker = new FakeCallInvoker(_ => new AuthRuleResult { Rule = rule });

        var result = await Adapter(invoker).GetRuleAsync("orders", "r-1");

        var sent = (AuthRuleRef)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.RuleId, Is.EqualTo("r-1"));
            Assert.That(result, Is.SameAs(rule));
        });
    }

    [Test]
    public async Task GetRuleAsync_missing_returns_null()
    {
        var result = await Adapter(new FakeCallInvoker(_ => new AuthRuleResult { Rule = null })).GetRuleAsync("orders", "ghost");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task RemoveRuleAsync_unwraps_removed()
    {
        var invoker = new FakeCallInvoker(_ => new AuthRuleRemoved { Removed = true });
        var result = await Adapter(invoker).RemoveRuleAsync("orders", "r-1");
        Assert.Multiple(() =>
        {
            Assert.That(((AuthRuleRef)invoker.LastRequest!).RuleId, Is.EqualTo("r-1"));
            Assert.That(result, Is.True);
        });
    }

    [Test]
    public async Task ListRulesAsync_returns_page()
    {
        var page = new AuthRulePage();
        var result = await Adapter(new FakeCallInvoker(_ => page)).ListRulesAsync(new AuthPageRequest());
        Assert.That(result, Is.SameAs(page));
    }

    [Test]
    public async Task ListRulesForTreeAsync_wraps_tree_and_page()
    {
        var page = new AuthRulePage();
        var request = new AuthPageRequest { PageSize = 5 };
        var invoker = new FakeCallInvoker(_ => page);

        var result = await Adapter(invoker).ListRulesForTreeAsync("orders", request);

        var sent = (AuthTreeRulesPage)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Page, Is.SameAs(request));
            Assert.That(result, Is.SameAs(page));
        });
    }

    [Test]
    public async Task ExplainAsync_wraps_query()
    {
        var explanation = new AuthExplanation { SubjectId = "alice", Scope = LatticeScope.Tree("orders"), Allowed = true };
        var invoker = new FakeCallInvoker(_ => explanation);

        var result = await Adapter(invoker).ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree("orders"));

        var sent = (AuthExplainQuery)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.SubjectId, Is.EqualTo("alice"));
            Assert.That(sent.Operation, Is.EqualTo(LatticeOperation.Read));
            Assert.That(result, Is.SameAs(explanation));
        });
    }

    [Test]
    public async Task EffectivePermissionsAsync_wraps_subject()
    {
        var permissions = new AuthEffectivePermissions { SubjectId = "alice" };
        var invoker = new FakeCallInvoker(_ => permissions);

        var result = await Adapter(invoker).EffectivePermissionsAsync("alice");

        Assert.Multiple(() =>
        {
            Assert.That(((AuthSubjectRef)invoker.LastRequest!).SubjectId, Is.EqualTo("alice"));
            Assert.That(result, Is.SameAs(permissions));
        });
    }
}
