namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins the two outcome records the replication config authority returns
/// (<see cref="LatticeReplicationEnableResult"/> /
/// <see cref="LatticeReplicationDisableResult"/>) and the default no-op tenant
/// isolation gate core replication ships until the tenancy add-on displaces it.
/// <para>
/// The outcome records are public value types an operator-facing API surfaces
/// verbatim, so structural equality and a readable <c>ToString</c> are part of
/// their contract. The gate matters for a different reason: it is the fail-open
/// default, and a core cluster with no tenancy add-on must behave byte-for-byte
/// as it did before tenancy existed - inactive, and admitting every tree.
/// </para>
/// </summary>
[TestFixture]
public class ReplicationConfigOutcomeAndNullGateTests
{
    // ----- LatticeReplicationEnableResult -----

    [Test]
    public void EnableResult_exposes_every_positional_slot()
    {
        var result = new LatticeReplicationEnableResult(
            TreeId: "orders",
            Mode: LatticeMergeMode.LwwRegister,
            AlreadyEnabled: false,
            BootstrapRequested: true);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(result.AlreadyEnabled, Is.False);
            Assert.That(result.BootstrapRequested, Is.True);
        });
    }

    [Test]
    public void EnableResult_deconstructs_in_declaration_order()
    {
        var result = new LatticeReplicationEnableResult("orders", LatticeMergeMode.LwwRegister, true, false);

        var (treeId, mode, alreadyEnabled, bootstrapRequested) = result;

        Assert.Multiple(() =>
        {
            Assert.That(treeId, Is.EqualTo("orders"));
            Assert.That(mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(alreadyEnabled, Is.True);
            Assert.That(bootstrapRequested, Is.False);
        });
    }

    [Test]
    public void EnableResult_equality_is_structural()
    {
        var a = new LatticeReplicationEnableResult("orders", LatticeMergeMode.LwwRegister, false, false);
        var b = new LatticeReplicationEnableResult("orders", LatticeMergeMode.LwwRegister, false, false);
        var idempotentNoOp = a with { AlreadyEnabled = true };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(idempotentNoOp),
                "a fresh enable and an idempotent no-op must not compare equal");
            Assert.That(a == b, Is.True);
            Assert.That(a != idempotentNoOp, Is.True);
        });
    }

    [Test]
    public void EnableResult_ToString_names_the_tree_and_mode()
    {
        var text = new LatticeReplicationEnableResult("orders", LatticeMergeMode.LwwRegister, false, true).ToString();

        Assert.That(text, Does.Contain("orders"));
        Assert.That(text, Does.Contain(nameof(LatticeReplicationEnableResult.BootstrapRequested)));
    }

    // ----- LatticeReplicationDisableResult -----

    [Test]
    public void DisableResult_exposes_every_positional_slot()
    {
        var result = new LatticeReplicationDisableResult(TreeId: "orders", AlreadyDisabled: true);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.AlreadyDisabled, Is.True);
        });
    }

    [Test]
    public void DisableResult_deconstructs_in_declaration_order()
    {
        var (treeId, alreadyDisabled) = new LatticeReplicationDisableResult("orders", false);

        Assert.Multiple(() =>
        {
            Assert.That(treeId, Is.EqualTo("orders"));
            Assert.That(alreadyDisabled, Is.False);
        });
    }

    [Test]
    public void DisableResult_equality_is_structural()
    {
        var fresh = new LatticeReplicationDisableResult("orders", false);
        var same = new LatticeReplicationDisableResult("orders", false);
        var noOp = fresh with { AlreadyDisabled = true };

        Assert.Multiple(() =>
        {
            Assert.That(fresh, Is.EqualTo(same));
            Assert.That(fresh.GetHashCode(), Is.EqualTo(same.GetHashCode()));
            Assert.That(fresh, Is.Not.EqualTo(noOp));
            Assert.That(fresh == same, Is.True);
            Assert.That(fresh != noOp, Is.True);
        });
    }

    [Test]
    public void DisableResult_ToString_names_the_tree()
        => Assert.That(new LatticeReplicationDisableResult("orders", true).ToString(), Does.Contain("orders"));

    // ----- NullReplicationTenantIsolationGate -----

    [Test]
    public void Null_isolation_gate_reports_itself_inactive()
    {
        IReplicationTenantIsolationGate gate = new NullReplicationTenantIsolationGate();

        Assert.That(gate.IsActive, Is.False,
            "an inactive gate is what lets the inbound apply path skip tenant isolation entirely");
    }

    [Test]
    public async Task Null_isolation_gate_admits_every_tree()
    {
        IReplicationTenantIsolationGate gate = new NullReplicationTenantIsolationGate();

        var decision = await gate.EvaluateAsync("orders", CancellationToken.None);

        Assert.That(decision, Is.EqualTo(ReplicationTenantIsolationDecision.Admit),
            "a core cluster with no tenancy add-on must apply exactly as it did before tenancy existed");
    }

    [Test]
    public async Task Null_isolation_gate_admits_synchronously_so_the_apply_path_pays_nothing()
    {
        IReplicationTenantIsolationGate gate = new NullReplicationTenantIsolationGate();

        var pending = gate.EvaluateAsync("orders", CancellationToken.None);

        Assert.That(pending.IsCompletedSuccessfully, Is.True,
            "the default seam must not force the hot inbound apply path onto an await");
        Assert.That(await pending, Is.EqualTo(ReplicationTenantIsolationDecision.Admit));
    }
}
