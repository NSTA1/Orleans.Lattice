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

    /// <summary>
    /// Security regression. A key filter is a per-key predicate, so probing it with
    /// the prefix string itself asks about the single key equal to that prefix -
    /// not about the prefix. A policy of "deny by default, plus one Allow/Read on
    /// the exact key <c>p/</c>" compiles to exactly this filter, and used to report
    /// a role scoped to the whole <c>p/</c> prefix as held. Only an unfiltered
    /// allow spans a prefix, so any filtered decision must now fail closed.
    /// </summary>
    [Test]
    public async Task A_filtered_allow_never_holds_a_prefix_scope()
    {
        var exactKeyOnly = new GrantingAccessGate
        {
            Override = _ => LatticeAccessDecision.Filtered(k => string.Equals(k, "p/", StringComparison.Ordinal)),
        };
        var keepsEverything = new GrantingAccessGate { Override = _ => LatticeAccessDecision.Filtered(_ => true) };
        var prefix = new AppMcpRoleGate(LatticeOperation.Read, [LatticeScope.Prefix("t1", "p/")]);

        Assert.Multiple(async () =>
        {
            Assert.That(
                await prefix.IsHeldAsync(exactKeyOnly, Alice, CancellationToken.None),
                Is.False,
                "A filter that admits only the key equal to the prefix must not hold the prefix.");
            Assert.That(
                await prefix.IsHeldAsync(keepsEverything, Alice, CancellationToken.None),
                Is.False,
                "A filtered decision is per-key and cannot certify a whole prefix.");
        });
    }

    [Test]
    public async Task An_unfiltered_allow_still_holds_a_prefix_scope()
    {
        // The fix must fail closed on filtered decisions only - an outright allow
        // over the prefix still holds the role.
        var gate = new GrantingAccessGate().Grant("alice", "t1", LatticeOperation.Read);
        var prefix = new AppMcpRoleGate(LatticeOperation.Read, [LatticeScope.Prefix("t1", "p/")]);

        Assert.That(await prefix.IsHeldAsync(gate, Alice, CancellationToken.None), Is.True);
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
