using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppRoleGate"/>, the single definition of "holds an app role": a role is held by
/// binding - membership of a group the install binds to it - never by the caller's other rights.
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
            Assert.Throws<ArgumentNullException>(() => AppMcpRoleGate.IsHeldAsync(null!, Member("alice")));
        });
    }

    [Test]
    public async Task The_tool_gate_delegates_to_the_shared_role_gate_and_completes_synchronously()
    {
        var held = new AppRoleGate(LatticeOperation.Read, Notes, ["g-readers"]);
        var notHeld = new AppRoleGate(LatticeOperation.Write, Notes, ["g-writers"]);
        var alice = Member("alice", "g-readers");

        var heldTask = AppMcpRoleGate.IsHeldAsync(held, alice);
        var notHeldTask = AppMcpRoleGate.IsHeldAsync(notHeld, alice);

        Assert.Multiple(async () =>
        {
            Assert.That(heldTask.IsCompletedSuccessfully && notHeldTask.IsCompletedSuccessfully, Is.True);
            Assert.That(await heldTask, Is.True);
            Assert.That(await notHeldTask, Is.False);
        });
    }
}
