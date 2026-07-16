using NSubstitute;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="AuthToolHandlers"/>, the thin adapter methods behind
/// the auth tool module. Every test drives a handler with a substituted
/// <see cref="ILatticeAuthAdmin"/> facade and proves the handler marshals the
/// tool-call arguments into the facade's model types and forwards the call
/// verbatim - it re-implements no authorization, read, or write logic. Covers the
/// four acceptance-criteria administration flows (create a user, create a group,
/// add a membership edge, put a rule) and the introspection explain, plus the
/// null-facade guards. Deterministic - fakes, no cluster, no ordering.
/// </summary>
[TestFixture]
public sealed class AuthToolHandlersTests
{
    private static ILatticeAuthAdmin Admin() => Substitute.For<ILatticeAuthAdmin>();

    [Test]
    public async Task ExplainAsync_builds_the_scope_and_forwards_to_the_facade()
    {
        var admin = Admin();
        var expected = new AuthExplanation { SubjectId = "alice", Scope = LatticeScope.Key("orders", "k1"), Allowed = true };
        admin.ExplainAsync("alice", LatticeOperation.Read, Arg.Any<LatticeScope>(), Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await AuthToolHandlers.ExplainAsync(
            admin, "alice", LatticeOperation.Read, LatticeScopeKind.Key, "orders", "k1", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
        await admin.Received(1).ExplainAsync(
            "alice",
            LatticeOperation.Read,
            Arg.Is<LatticeScope>(s => s.Kind == LatticeScopeKind.Key && s.TreeId == "orders" && s.KeyOrPrefix == "k1"),
            Arg.Any<LatticeSubjectSelectorKind>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EffectivePermissionsAsync_forwards_the_subject()
    {
        var admin = Admin();
        var expected = new AuthEffectivePermissions { SubjectId = "alice" };
        admin.EffectivePermissionsAsync("alice", Arg.Any<LatticeSubjectSelectorKind>(), Arg.Any<CancellationToken>()).Returns(expected);

        var result = await AuthToolHandlers.EffectivePermissionsAsync(admin, "alice", CancellationToken.None);

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public async Task ListUsersAsync_maps_paging_arguments_into_a_page_request()
    {
        var admin = Admin();
        admin.ListUsersAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new AuthUserPage());

        await AuthToolHandlers.ListUsersAsync(admin, pageSize: 25, pageToken: "cursor", CancellationToken.None);

        await admin.Received(1).ListUsersAsync(
            Arg.Is<AuthPageRequest>(r => r.PageSize == 25 && r.PageToken == "cursor"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListRulesForTreeAsync_forwards_the_tree_id_and_paging()
    {
        var admin = Admin();
        admin.ListRulesForTreeAsync("orders", Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new AuthRulePage());

        await AuthToolHandlers.ListRulesForTreeAsync(admin, "orders", pageSize: 10, pageToken: null, CancellationToken.None);

        await admin.Received(1).ListRulesForTreeAsync(
            "orders",
            Arg.Is<AuthPageRequest>(r => r.PageSize == 10 && r.PageToken == null),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UpsertUserAsync_builds_and_writes_the_user_and_echoes_it()
    {
        var admin = Admin();
        var claims = new Dictionary<string, string> { ["team"] = "ops" };

        var written = await AuthToolHandlers.UpsertUserAsync(admin, "alice", "Alice", claims, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(written.UserId, Is.EqualTo("alice"));
            Assert.That(written.DisplayName, Is.EqualTo("Alice"));
            Assert.That(written.Claims, Is.EqualTo(claims));
        });
        await admin.Received(1).UpsertUserAsync(
            Arg.Is<AuthUser>(u => u.UserId == "alice" && u.DisplayName == "Alice" && u.Claims == claims),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UpsertGroupAsync_builds_and_writes_the_group_and_echoes_it()
    {
        var admin = Admin();

        var written = await AuthToolHandlers.UpsertGroupAsync(admin, "ops", "Operations", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(written.GroupId, Is.EqualTo("ops"));
            Assert.That(written.DisplayName, Is.EqualTo("Operations"));
        });
        await admin.Received(1).UpsertGroupAsync(
            Arg.Is<AuthGroup>(g => g.GroupId == "ops" && g.DisplayName == "Operations"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AddMemberAsync_forwards_the_edge_with_the_member_kind()
    {
        var admin = Admin();

        await AuthToolHandlers.AddMemberAsync(admin, "ops", "team-a", MembershipMemberKind.Group, CancellationToken.None);

        await admin.Received(1).AddMemberAsync("ops", "team-a", MembershipMemberKind.Group, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AddMemberAsync_defaults_the_member_kind_to_user()
    {
        var admin = Admin();

        await AuthToolHandlers.AddMemberAsync(admin, "ops", "alice");

        await admin.Received(1).AddMemberAsync("ops", "alice", MembershipMemberKind.User, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PutRuleAsync_builds_the_rule_from_scalar_arguments_and_echoes_it()
    {
        var admin = Admin();

        var rule = await AuthToolHandlers.PutRuleAsync(
            admin,
            ruleId: "r1",
            subjectKind: LatticeSubjectSelectorKind.Group,
            subjectId: "ops",
            scopeKind: LatticeScopeKind.Prefix,
            treeId: "orders",
            operations: LatticeOperation.Read | LatticeOperation.Write,
            effect: LatticeEffect.Allow,
            keyOrPrefix: "eu/",
            condition: null,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(rule.RuleId, Is.EqualTo("r1"));
            Assert.That(rule.Subject.Kind, Is.EqualTo(LatticeSubjectSelectorKind.Group));
            Assert.That(rule.Subject.Id, Is.EqualTo("ops"));
            Assert.That(rule.Scope.Kind, Is.EqualTo(LatticeScopeKind.Prefix));
            Assert.That(rule.Scope.TreeId, Is.EqualTo("orders"));
            Assert.That(rule.Scope.KeyOrPrefix, Is.EqualTo("eu/"));
            Assert.That(rule.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
            Assert.That(rule.Effect, Is.EqualTo(LatticeEffect.Allow));
        });
        await admin.Received(1).PutRuleAsync(
            Arg.Is<LatticeAuthorizationRule>(r => r.RuleId == "r1" && r.Scope.KeyOrPrefix == "eu/"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RemoveRuleAsync_returns_the_facade_result()
    {
        var admin = Admin();
        admin.RemoveRuleAsync("orders", "r1", Arg.Any<CancellationToken>()).Returns(true);

        var removed = await AuthToolHandlers.RemoveRuleAsync(admin, "orders", "r1", CancellationToken.None);

        Assert.That(removed, Is.True);
    }

    [Test]
    public async Task RemoveUserAsync_forwards_to_the_facade()
    {
        var admin = Admin();

        await AuthToolHandlers.RemoveUserAsync(admin, "alice", CancellationToken.None);

        await admin.Received(1).RemoveUserAsync("alice", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Handlers_reject_a_null_facade()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => AuthToolHandlers.ExplainAsync(null!, "s", LatticeOperation.Read, LatticeScopeKind.Tree, "t"),
                Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.GetUserAsync(null!, "u"), Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.UpsertUserAsync(null!, "u"), Throws.ArgumentNullException);
            Assert.That(
                () => AuthToolHandlers.PutRuleAsync(
                    null!, "r", LatticeSubjectSelectorKind.User, "u", LatticeScopeKind.Tree, "t",
                    LatticeOperation.Read, LatticeEffect.Allow),
                Throws.ArgumentNullException);
            Assert.That(() => AuthToolHandlers.RemoveRuleAsync(null!, "t", "r"), Throws.ArgumentNullException);
        });
    }
}
