using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppRoleGate"/>, the single definition of "holds an app role": a caller holds a role
/// if and only if it is a member of a group bound to it and the role confers anything (#3902).
/// </summary>
[TestFixture]
public sealed class AppRoleGateTests
{
    private static readonly LatticeScope[] Notes = [LatticeScope.Tree("t1")];

    private static LatticeSubject Caller(params string[] groups) => new("alice", new HashSet<string>(groups, StringComparer.Ordinal));

    [Test]
    public void A_member_of_a_bound_group_holds_the_role()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-a", "g-b"]);

        Assert.Multiple(() =>
        {
            Assert.That(role.IsHeldBy(Caller("g-b")), Is.True);
            Assert.That(role.IsHeldBy(Caller("g-x", "g-a")), Is.True);
            Assert.That(role.IsHeldBy(Caller("g-x")), Is.False);
            Assert.That(role.IsHeldBy(Caller()), Is.False, "a caller with no group holds nothing");
        });
    }

    [Test]
    public void Group_membership_is_matched_ordinally_over_any_collection()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-a"]);

        Assert.Multiple(() =>
        {
            Assert.That(role.IsHeldBy(new LatticeSubject("alice", ["g-x", "g-a"])), Is.True, "a list closure");
            Assert.That(role.IsHeldBy(new LatticeSubject("alice", ["G-A"])), Is.False);
        });
    }

    [Test]
    public void The_anonymous_or_an_unnamed_subject_holds_nothing()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-a"]);

        Assert.Multiple(() =>
        {
            Assert.That(role.IsHeldBy(LatticeSubject.Anonymous with { GroupIds = ["g-a"] }), Is.False);
            Assert.That(role.IsHeldBy(new LatticeSubject(string.Empty, ["g-a"])), Is.False);
            Assert.That(role.IsHeldBy(default), Is.False);
        });
    }

    [Test]
    public void A_role_that_confers_nothing_is_never_held()
    {
        var member = Caller("g-a");

        Assert.Multiple(() =>
        {
            Assert.That(new AppRoleGate(LatticeOperation.None, Notes, ["g-a"]).IsHeldBy(member), Is.False, "no operations");
            Assert.That(new AppRoleGate(LatticeOperation.Read, [], ["g-a"]).IsHeldBy(member), Is.False, "no scopes");
            Assert.That(new AppRoleGate(LatticeOperation.Read, Notes, []).IsHeldBy(member), Is.False, "unbound");
            Assert.That(new AppRoleGate(LatticeOperation.Read, Notes, []).ConfersAnything, Is.False);
            Assert.That(new AppRoleGate(LatticeOperation.Read, Notes, ["g-a"]).ConfersAnything, Is.True);
        });
    }

    [Test]
    public void IsMember_fails_closed_on_a_missing_closure_or_group()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppRoleGate.IsMember(null, "g-a"), Is.False);
            Assert.That(AppRoleGate.IsMember([], "g-a"), Is.False);
            Assert.That(AppRoleGate.IsMember(["g-a"], string.Empty), Is.False);
            Assert.That(AppRoleGate.IsMember(new HashSet<string>(["g-a"], StringComparer.Ordinal), "g-a"), Is.True);
            Assert.That(AppRoleGate.IsMember(["g-b", "g-a"], "g-a"), Is.True);
        });
    }

    [Test]
    public void Constructor_rejects_null_scopes_or_groups()
    {
        Assert.Throws<ArgumentNullException>(() => new AppRoleGate(LatticeOperation.Read, null!, []));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGate(LatticeOperation.Read, Notes, null!));
    }

    [Test]
    public void The_tool_gate_delegates_to_the_shared_role_gate()
    {
        var held = new AppRoleGate(LatticeOperation.Read, Notes, ["g-a"]);
        var notHeld = new AppRoleGate(LatticeOperation.Write, Notes, ["g-b"]);

        Assert.That(AppMcpRoleGate.IsHeld(held, Caller("g-a")), Is.True);
        Assert.That(AppMcpRoleGate.IsHeld(notHeld, Caller("g-a")), Is.False);
        Assert.Throws<ArgumentNullException>(() => AppMcpRoleGate.IsHeld(null!, Caller("g-a")));
    }

    /// <summary>
    /// Security regression (#3863), restated for the binding definition. A key-filtered allow that happens to
    /// spell a prefix used to certify a whole prefix role; now no right held outside the app's own rules - a
    /// filtered allow, an unfiltered allow, a cluster-wide allow - makes a caller hold a role, because the
    /// access gate is never consulted to decide one.
    /// </summary>
    [Test]
    public async Task No_right_outside_the_app_rules_holds_a_role_and_the_gate_is_not_consulted()
    {
        var host = new AppMcpTestHost()
            .Provide(AppMcpTestData.Slug("notes"), AppMcpTestData.Tool("read"))
            .Publish(1, AppMcpTestData.Record(TenantId.Default, AppMcpTestData.Slug("notes"), AppMcpTestData.V1, bindings: [AppRoleBinding.Create("reader", "g-readers")]))
            .Member("alice", "g-operators");
        host.Source.Add(AppMcpTestData.Manifest(
            AppMcpTestData.Slug("notes"),
            AppMcpTestData.V1,
            [AppMcpTestData.Role("reader", LatticeOperation.Read, new AppScopeTemplate { Tree = "notes", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "p/" })],
            [AppMcpTestData.ToolDecl("read", "reader")]));

        foreach (var decision in new[]
        {
            LatticeAccessDecision.Filtered(static key => string.Equals(key, "p/", StringComparison.Ordinal)),
            LatticeAccessDecision.Filtered(static _ => true),
            LatticeAccessDecision.Allow(),
        })
        {
            host.Gate.Override = _ => decision;
            Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
        }

        Assert.That(host.Gate.Requests, Is.Empty);

        // A bound member holds the prefix role: the app-owned rule for the binding grants the whole prefix.
        host.Member("alice", "g-readers");
        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities", "notes_read" }));
    }
}
