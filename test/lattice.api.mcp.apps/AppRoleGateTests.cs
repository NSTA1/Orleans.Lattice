using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppRoleGate"/>, the single definition of "holds an app role": a role is held by
/// binding - membership of a group the install binds to it - never by the caller's other rights, and the access gate
/// can only take it away.
/// </summary>
[TestFixture]
public sealed class AppRoleGateTests
{
    private static readonly LatticeScope[] Notes = [LatticeScope.Tree("t1")];

    private static LatticeSubject Member(string subject, params string[] groups) =>
        new(subject, new HashSet<string>(groups, StringComparer.Ordinal));

    [Test]
    public void A_member_of_a_bound_group_holds_the_role()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-viewers"]);

        Assert.Multiple(() =>
        {
            Assert.That(role.IsHeld(Member("alice", "g-viewers")), Is.True);
            Assert.That(role.IsHeld(Member("alice", "g-other", "g-viewers")), Is.True);
            Assert.That(role.ConfersGrant, Is.True);
        });
    }

    [Test]
    public void A_role_bound_to_several_groups_is_held_through_any_of_them()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-a", "g-b"]);

        Assert.That(role.IsHeld(Member("alice", "g-b")), Is.True);
    }

    [Test]
    public void A_caller_outside_every_bound_group_does_not_hold_the_role_whatever_else_it_belongs_to()
    {
        var editor = new AppRoleGate(LatticeOperation.Read | LatticeOperation.Write, Notes, ["g-editors"]);

        Assert.That(editor.IsHeld(Member("bob", "g-viewers", "cluster-admins")), Is.False);
    }

    /// <summary>
    /// Security regression (#3863, re-stated under #3902). A caller whose only right on a prefix-scoped role
    /// is its own grant on the single key that spells the prefix must not hold the role. Under the binding
    /// definition no right of the caller's own is consulted at all, so a key-filtered allow, an unfiltered allow
    /// or any other rule outside the app's bindings is equally unable to confer the prefix.
    /// </summary>
    [Test]
    public void A_prefix_role_is_never_held_through_the_callers_own_rights()
    {
        var prefix = new AppRoleGate(LatticeOperation.Read, [LatticeScope.Prefix("t1", "p/")], ["g-readers"]);

        Assert.Multiple(() =>
        {
            Assert.That(prefix.IsHeld(Member("alice", "exact-key-p-readers")), Is.False);
            Assert.That(prefix.IsHeld(Member("alice", "g-readers")), Is.True);
        });
    }

    [Test]
    public void A_role_that_confers_nothing_is_never_held()
    {
        var member = Member("alice", "g");

        Assert.Multiple(() =>
        {
            var noOperations = new AppRoleGate(LatticeOperation.None, Notes, ["g"]);
            var noScopes = new AppRoleGate(LatticeOperation.Read, [], ["g"]);
            var unbound = new AppRoleGate(LatticeOperation.Read, Notes, []);
            Assert.That(noOperations.IsHeld(member), Is.False);
            Assert.That(noScopes.IsHeld(member), Is.False);
            Assert.That(unbound.IsHeld(member), Is.False);
            Assert.That(noOperations.ConfersGrant || noScopes.ConfersGrant || unbound.ConfersGrant, Is.False);
        });
    }

    [Test]
    public void A_caller_without_a_resolved_identity_or_group_closure_holds_nothing()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g"]);

        Assert.Multiple(() =>
        {
            Assert.That(role.IsHeld(LatticeSubject.Anonymous with { GroupIds = new[] { "g" } }), Is.False);
            Assert.That(role.IsHeld(new LatticeSubject(string.Empty, ["g"])), Is.False);
            Assert.That(role.IsHeld(new LatticeSubject("alice")), Is.False);
            Assert.That(role.IsHeld(default), Is.False);
        });
    }

    [Test]
    public void Group_membership_is_matched_ordinally_for_every_closure_shape()
    {
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-Viewers"]);

        Assert.Multiple(() =>
        {
            Assert.That(role.IsHeld(new LatticeSubject("alice", new[] { "g-Viewers" })), Is.True, "array");
            Assert.That(role.IsHeld(new LatticeSubject("alice", new List<string> { "g-Viewers" })), Is.True, "list");
            Assert.That(role.IsHeld(new LatticeSubject("alice", new[] { "g-viewers" })), Is.False, "array, case differs");
            Assert.That(role.IsHeld(new LatticeSubject("alice", new List<string> { "g-viewers" })), Is.False, "list, case differs");
            Assert.That(AppRoleGate.IsMember(new HashSet<string>(["g-Viewers"], StringComparer.Ordinal), "g-Viewers"), Is.True, "set");
        });
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new AppRoleGate(LatticeOperation.Read, null!, []));
            Assert.Throws<ArgumentNullException>(() => new AppRoleGate(LatticeOperation.Read, Notes, null!));
            Assert.Throws<ArgumentNullException>(() => AppRoleGate.IsMember(null!, "g"));
            Assert.ThrowsAsync<ArgumentNullException>(async () => await new AppRoleGate(LatticeOperation.Read, Notes, ["g"]).IsHeldAsync(null!, Member("alice"), CancellationToken.None));
        });
    }

    [Test]
    public async Task The_gate_holds_a_bound_role_and_withholds_an_unbound_one()
    {
        var gate = new GrantingAccessGate { AllowByDefault = true };
        var held = new AppRoleGate(LatticeOperation.Read, Notes, ["g-readers"]);
        var notHeld = new AppRoleGate(LatticeOperation.Write, Notes, ["g-writers"]);
        var alice = Member("alice", "g-readers");

        Assert.Multiple(async () =>
        {
            Assert.That(await held.IsHeldAsync(gate, alice, CancellationToken.None), Is.True);
            Assert.That(await notHeld.IsHeldAsync(gate, alice, CancellationToken.None), Is.False);
        });
    }

    // The access gate can only take a role away.

    [Test]
    public async Task An_explicit_deny_takes_the_role_from_a_bound_member()
    {
        var role = new AppRoleGate(LatticeOperation.Read | LatticeOperation.Write, Notes, ["g"]);
        var alice = Member("alice", "g");
        var onTree = new GrantingAccessGate { AllowByDefault = true }.Deny("alice", "t1", LatticeOperation.Write);
        var clusterWide = new GrantingAccessGate { AllowByDefault = true }.Deny("alice", LatticeScope.ClusterWideTreeId, LatticeOperation.Read);

        Assert.Multiple(async () =>
        {
            Assert.That(await role.IsHeldAsync(new GrantingAccessGate { AllowByDefault = true }, alice, CancellationToken.None), Is.True);
            Assert.That(await role.IsHeldAsync(onTree, alice, CancellationToken.None), Is.False, "a deny on any one operation");
            Assert.That(await role.IsHeldAsync(clusterWide, alice, CancellationToken.None), Is.False, "a cluster-wide deny");
        });
    }

    [Test]
    public async Task The_gate_is_never_asked_for_a_caller_the_binding_does_not_hold()
    {
        var everything = new GrantingAccessGate { AllowByDefault = true };
        var role = new AppRoleGate(LatticeOperation.Read, Notes, ["g-editors"]);

        Assert.That(await role.IsHeldAsync(everything, Member("bob", "cluster-admins"), CancellationToken.None), Is.False);
        Assert.That(everything.Requests, Is.Empty, "the caller's own rights never add a role");
    }

    [Test]
    public async Task A_role_survives_a_deny_on_one_scope_through_another_scope()
    {
        var role = new AppRoleGate(LatticeOperation.Read, [LatticeScope.Tree("t1"), LatticeScope.Tree("t2")], ["g"]);
        var gate = new GrantingAccessGate { AllowByDefault = true }.Deny("alice", "t1", LatticeOperation.Read);

        Assert.That(await role.IsHeldAsync(gate, Member("alice", "g"), CancellationToken.None), Is.True);
    }

    [Test]
    public async Task Each_operation_the_role_confers_is_asked_in_each_scopes_own_shape()
    {
        var gate = new GrantingAccessGate { AllowByDefault = true };
        var role = new AppRoleGate(LatticeOperation.Read | LatticeOperation.Write, [LatticeScope.Key("t1", "k"), LatticeScope.Prefix("t1", "p/")], ["g"]);
        gate.Deny("alice", "t1", LatticeOperation.Write);

        await role.IsHeldAsync(gate, Member("alice", "g"), CancellationToken.None);

        Assert.That(
            gate.Requests.Select(r => (r.TreeId, r.Operation, r.Key)),
            Is.EqualTo(new[]
            {
                ("t1", LatticeOperation.Read, (string?)"k"), ("t1", LatticeOperation.Write, (string?)"k"),
                ("t1", LatticeOperation.Read, (string?)null), ("t1", LatticeOperation.Write, (string?)null),
            }));
    }

    /// <summary>
    /// A key-filtered answer is resolved at the scope's representative key: the prefix itself for a prefix scope,
    /// the empty key for a whole tree. The filter can only take the role away - a filter that keeps every key
    /// leaves the bound member its role, one that drops the representative key removes it.
    /// </summary>
    [Test]
    public async Task A_filtered_answer_is_resolved_at_the_scopes_representative_key()
    {
        var alice = Member("alice", "g");
        var prefix = new AppRoleGate(LatticeOperation.Read, [LatticeScope.Prefix("t1", "p/")], ["g"]);
        var tree = new AppRoleGate(LatticeOperation.Read, Notes, ["g"]);
        string? probed = null;
        var recording = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(k => { probed = k; return true; }) };
        var dropsPrefix = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(k => k != "p/") };
        var dropsEmpty = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(k => k.Length > 0) };

        Assert.That(await prefix.IsHeldAsync(recording, alice, CancellationToken.None), Is.True);
        Assert.That(probed, Is.EqualTo("p/"));
        Assert.That(await tree.IsHeldAsync(recording, alice, CancellationToken.None), Is.True);
        Assert.That(probed, Is.EqualTo(string.Empty));
        Assert.That(await prefix.IsHeldAsync(dropsPrefix, alice, CancellationToken.None), Is.False);
        Assert.That(await tree.IsHeldAsync(dropsEmpty, alice, CancellationToken.None), Is.False);
    }
}
