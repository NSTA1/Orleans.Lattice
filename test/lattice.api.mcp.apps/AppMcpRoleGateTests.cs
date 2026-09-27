using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

[TestFixture]
public sealed class AppMcpRoleGateTests
{
    private static readonly LatticeSubject Alice = new("alice");

    [Test]
    public async Task A_role_is_held_only_when_every_operation_is_allowed_on_one_scope()
    {
        var gate = new GrantingAccessGate().Grant("alice", "t1", LatticeOperation.Read);
        var role = new AppMcpRoleGate(LatticeOperation.Read | LatticeOperation.Write, [LatticeScope.Tree("t1")]);

        Assert.That(await role.IsHeldAsync(gate, Alice, CancellationToken.None), Is.False);

        gate.Grant("alice", "t1", LatticeOperation.Write);
        Assert.That(await role.IsHeldAsync(gate, Alice, CancellationToken.None), Is.True);
    }

    [Test]
    public async Task A_role_is_held_when_any_one_scope_carries_every_operation()
    {
        var gate = new GrantingAccessGate()
            .Grant("alice", "t1", LatticeOperation.Read)
            .Grant("alice", "t2", LatticeOperation.Read | LatticeOperation.Write);
        var role = new AppMcpRoleGate(
            LatticeOperation.Read | LatticeOperation.Write,
            [LatticeScope.Tree("t1"), LatticeScope.Tree("t2")]);

        Assert.That(await role.IsHeldAsync(gate, Alice, CancellationToken.None), Is.True);
    }

    [Test]
    public async Task Operations_split_across_scopes_do_not_hold_the_role()
    {
        var gate = new GrantingAccessGate()
            .Grant("alice", "t1", LatticeOperation.Read)
            .Grant("alice", "t2", LatticeOperation.Write);
        var role = new AppMcpRoleGate(
            LatticeOperation.Read | LatticeOperation.Write,
            [LatticeScope.Tree("t1"), LatticeScope.Tree("t2")]);

        Assert.That(await role.IsHeldAsync(gate, Alice, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task Each_operation_bit_is_asked_separately_with_the_key_of_a_key_scope()
    {
        var gate = new GrantingAccessGate().Grant("alice", "t1", LatticeOperation.Read | LatticeOperation.Write);
        var role = new AppMcpRoleGate(LatticeOperation.Read | LatticeOperation.Write, [LatticeScope.Key("t1", "k")]);

        await role.IsHeldAsync(gate, Alice, CancellationToken.None);

        Assert.That(
            gate.Requests.Select(r => (r.TreeId, r.Operation, r.Key, r.Subject.SubjectId)),
            Is.EqualTo(new[] { ("t1", LatticeOperation.Read, (string?)"k", "alice"), ("t1", LatticeOperation.Write, (string?)"k", "alice") }));
    }

    [Test]
    public async Task A_filtered_allow_holds_a_prefix_scope_only_when_the_filter_keeps_the_prefix()
    {
        var keeps = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(k => k.StartsWith("p/", StringComparison.Ordinal)) };
        var drops = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(_ => false) };
        var prefix = new AppMcpRoleGate(LatticeOperation.Read, [LatticeScope.Prefix("t1", "p/")]);

        Assert.Multiple(async () =>
        {
            Assert.That(await prefix.IsHeldAsync(keeps, Alice, CancellationToken.None), Is.True);
            Assert.That(await prefix.IsHeldAsync(drops, Alice, CancellationToken.None), Is.False);
        });
    }

    [Test]
    public async Task A_filtered_allow_never_holds_a_whole_tree_scope()
    {
        var gate = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(_ => true) };
        var role = new AppMcpRoleGate(LatticeOperation.Read, [LatticeScope.Tree("t1")]);

        Assert.That(await role.IsHeldAsync(gate, Alice, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task A_role_with_no_operations_or_no_scopes_is_never_held()
    {
        var gate = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Allow() };

        Assert.Multiple(async () =>
        {
            Assert.That(await new AppMcpRoleGate(LatticeOperation.None, [LatticeScope.Tree("t1")]).IsHeldAsync(gate, Alice, CancellationToken.None), Is.False);
            Assert.That(await new AppMcpRoleGate(LatticeOperation.Read, []).IsHeldAsync(gate, Alice, CancellationToken.None), Is.False);
        });
    }

    [Test]
    public void Constructor_rejects_null_scopes()
        => Assert.Throws<ArgumentNullException>(() => new AppMcpRoleGate(LatticeOperation.Read, null!));
}
