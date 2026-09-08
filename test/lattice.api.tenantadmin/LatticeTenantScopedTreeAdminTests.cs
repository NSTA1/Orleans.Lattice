using System.Reflection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantScopedTreeAdmin"/>: the tenant-scoped
/// composition of the whole-tree lifecycle and schema surfaces. They prove the
/// fail-closed tenant derivation (no active tenant refuses every op), the
/// structural namespace confinement (every op delegates under
/// <c>t/{activeTenant}/</c>, even for adversarial local names), and the quota
/// admission on create. All doubles are deterministic and hold no timing or
/// ordering assumptions.
/// </summary>
[TestFixture]
public sealed class LatticeTenantScopedTreeAdminTests
{
    private const string TenantValue = "acme";

    [SetUp]
    public void ClearAmbientTenantBefore() => LatticeActiveTenantContext.Current = null;

    [TearDown]
    public void ClearAmbientTenantAfter() => LatticeActiveTenantContext.Current = null;

    // ----- constructor guards -------------------------------------------------

    [Test]
    public void Constructor_null_tree_admin_throws()
        => Assert.That(
            () => new LatticeTenantScopedTreeAdmin(
                null!, Substitute.For<ILatticeSchemaAdmin>(),
                new TenantAdminTestSupport.FixedGate(allow: true)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_schema_admin_throws()
        => Assert.That(
            () => new LatticeTenantScopedTreeAdmin(
                Substitute.For<ILatticeTreeAdmin>(), null!,
                new TenantAdminTestSupport.FixedGate(allow: true)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_gate_throws()
        => Assert.That(
            () => new LatticeTenantScopedTreeAdmin(
                Substitute.For<ILatticeTreeAdmin>(), Substitute.For<ILatticeSchemaAdmin>(), null!),
            Throws.ArgumentNullException);

    // ----- fail-closed: no active tenant refuses every op ---------------------

    [TestCaseSource(nameof(NameTakingOps))]
    public void Op_without_active_tenant_throws_TenantScopeRequired(
        Func<ILatticeTenantScopedTreeAdmin, string, Task> op)
    {
        var facade = CreateFacade(out _, out _);

        // No ambient tenant is in scope (cleared in SetUp); a valid local name
        // still cannot resolve a namespace, so the op is refused fail-closed.
        Assert.That(async () => await op(facade, "orders"), Throws.TypeOf<TenantScopeRequiredException>());
    }

    // ----- argument guards ----------------------------------------------------

    [TestCaseSource(nameof(NameTakingOps))]
    public void Op_with_empty_name_throws_ArgumentException(
        Func<ILatticeTenantScopedTreeAdmin, string, Task> op)
    {
        using var scope = ActiveTenant();
        var facade = CreateFacade(out _, out _);

        Assert.That(async () => await op(facade, string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [TestCaseSource(nameof(NameTakingOps))]
    public void Op_with_null_name_throws_ArgumentException(
        Func<ILatticeTenantScopedTreeAdmin, string, Task> op)
    {
        using var scope = ActiveTenant();
        var facade = CreateFacade(out _, out _);

        Assert.That(async () => await op(facade, null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetSchemaPolicy_null_policy_throws_ArgumentNullException()
    {
        using var scope = ActiveTenant();
        var facade = CreateFacade(out _, out _);

        Assert.That(
            async () => await facade.SetSchemaPolicyAsync("orders", null!),
            Throws.ArgumentNullException);
    }

    // ----- namespace composition (happy-path delegation) ----------------------

    [Test]
    public async Task CreateTree_delegates_with_composed_id_and_passes_sizing_through()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        var result = await facade.CreateTreeAsync("orders", shardCount: 4, maxLeafKeys: 8, maxInternalChildren: 16);

        Assert.That(result.TreeId, Is.EqualTo("t/acme/orders"));
        await treeAdmin.Received(1).CreateTreeAsync("t/acme/orders", 4, 8, 16, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CheckTreeExists_delegates_with_composed_id()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CheckTreeExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeExistenceResult { TreeId = (string)ci[0]!, Exists = true }));

        using var scope = ActiveTenant();
        var result = await facade.CheckTreeExistsAsync("orders");

        Assert.That(result.TreeId, Is.EqualTo("t/acme/orders"));
        await treeAdmin.Received(1).CheckTreeExistsAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteTree_delegates_with_composed_id()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .DeleteTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeDeletionStatus { TreeId = (string)ci[0]!, IsDeleted = true }));

        using var scope = ActiveTenant();
        var result = await facade.DeleteTreeAsync("orders");

        Assert.That(result.TreeId, Is.EqualTo("t/acme/orders"));
        await treeAdmin.Received(1).DeleteTreeAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RecoverTree_delegates_with_composed_id()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .RecoverTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeDeletionStatus { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        var result = await facade.RecoverTreeAsync("orders");

        Assert.That(result.TreeId, Is.EqualTo("t/acme/orders"));
        await treeAdmin.Received(1).RecoverTreeAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PurgeTree_delegates_with_composed_id_and_passes_confirm_through()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .PurgeTreeAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeDeletionStatus { TreeId = (string)ci[0]!, PurgeComplete = true }));

        using var scope = ActiveTenant();
        var result = await facade.PurgeTreeAsync("orders", confirm: true);

        Assert.That(result.TreeId, Is.EqualTo("t/acme/orders"));
        await treeAdmin.Received(1).PurgeTreeAsync("t/acme/orders", true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PurgeTree_passes_confirm_false_through_unchanged()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .PurgeTreeAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeDeletionStatus { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.PurgeTreeAsync("orders", confirm: false);

        await treeAdmin.Received(1).PurgeTreeAsync("t/acme/orders", false, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetTreeDeletionStatus_delegates_with_composed_id()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .GetTreeDeletionStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeDeletionStatus { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        var result = await facade.GetTreeDeletionStatusAsync("orders");

        Assert.That(result.TreeId, Is.EqualTo("t/acme/orders"));
        await treeAdmin.Received(1).GetTreeDeletionStatusAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetSchemaPolicy_delegates_with_composed_id_and_same_policy()
    {
        var facade = CreateFacade(out _, out var schemaAdmin);
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());

        using var scope = ActiveTenant();
        await facade.SetSchemaPolicyAsync("orders", policy);

        await schemaAdmin.Received(1).SetPolicyAsync("t/acme/orders", policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearSchemaPolicy_delegates_with_composed_id_and_returns_result()
    {
        var facade = CreateFacade(out _, out var schemaAdmin);
        schemaAdmin.ClearPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));

        using var scope = ActiveTenant();
        var removed = await facade.ClearSchemaPolicyAsync("orders");

        Assert.That(removed, Is.True);
        await schemaAdmin.Received(1).ClearPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetSchemaPolicy_delegates_with_composed_id_and_returns_null_passthrough()
    {
        var facade = CreateFacade(out _, out var schemaAdmin);
        schemaAdmin.GetPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeSchemaPolicy?>(null));

        using var scope = ActiveTenant();
        var policy = await facade.GetSchemaPolicyAsync("orders");

        Assert.That(policy, Is.Null);
        await schemaAdmin.Received(1).GetPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    // ----- quota admission on create ------------------------------------------

    [Test]
    public async Task CreateTree_delegates_without_charging_admission_itself()
    {
        // Quota admission moved down to LatticeTreeAdmin, the narrowest seam every
        // create funnels through. It must not also be charged here: admission
        // consumes a request-rate token, so charging at both layers would bill a
        // single create twice.
        //
        // Asserted structurally as well as behaviourally. A facade that merely
        // holds an unread ITenantAdmissionController is dead security config - a
        // future reader sees an admission controller wired in and reasonably
        // concludes this layer enforces - so the field must be absent, not merely
        // unused.
        Assert.That(
            typeof(LatticeTenantScopedTreeAdmin)
                .GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .Select(f => f.FieldType),
            Has.None.EqualTo(typeof(ITenantAdmissionController)),
            "the facade must not retain an admission controller it never consults");

        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.CreateTreeAsync("orders");

        await treeAdmin.Received(1).CreateTreeAsync("t/acme/orders", null, null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void CreateTree_propagates_a_tenancy_refusal_from_the_delegated_facade()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns<Task<TreeCreationResult>>(_ => throw new LatticeTenantAccessDeniedException("refused"));

        using var scope = ActiveTenant();
        Assert.That(
            async () => await facade.CreateTreeAsync("orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public async Task CreateTree_delegates_the_composed_id_unconditionally()
    {
        // With admission removed from this layer there is no longer a branch that
        // can skip delegation: every authorized create reaches the inner facade,
        // which is the seam that accounts for it.
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.CreateTreeAsync("orders");

        await treeAdmin.Received(1).CreateTreeAsync("t/acme/orders", null, null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void CreateTree_propagates_quota_exceeded_from_the_delegated_facade()
    {
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns<Task<TreeCreationResult>>(_ => throw new LatticeQuotaExceededException("quota"));

        using var scope = ActiveTenant();
        Assert.That(
            async () => await facade.CreateTreeAsync("orders"),
            Throws.TypeOf<LatticeQuotaExceededException>());
    }

    // ----- authorize-before-account ordering (cross-tenant regression) --------

    [Test]
    public async Task CreateTree_when_gate_denies_never_delegates()
    {
        // The active tenant is a client-supplied assertion that only the access
        // gate validates. Delegating first let an unauthorized caller nominate any
        // victim tenant and have a stateful, quota-consuming, rate-limiting
        // evaluation charged to it by the inner facade - confirming the tenant's
        // existence, draining its rate budget, and leaking its current usage and
        // ceiling through the quota exception's message.
        var facade = CreateFacade(
            out var treeAdmin, out _, new TenantAdminTestSupport.FixedGate(allow: false));

        using var scope = ActiveTenant("victim");
        Assert.That(
            async () => await facade.CreateTreeAsync("orders"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        await treeAdmin.DidNotReceive().CreateTreeAsync(
            Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void CreateTree_when_gate_denies_reports_authorization_not_quota()
    {
        // A denied caller must not be able to distinguish "no such tenant" from
        // "tenant over quota": the refusal is an authorization denial carrying no
        // tenant usage figures, even though the delegated facade would have thrown
        // a quota breach naming them had it ever been reached.
        var facade = CreateFacade(
            out var treeAdmin, out _, new TenantAdminTestSupport.FixedGate(allow: false));
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns<Task<TreeCreationResult>>(
                _ => throw new LatticeQuotaExceededException("current=41 ceiling=42"));

        using var scope = ActiveTenant("victim");
        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await facade.CreateTreeAsync("orders"));

        Assert.That(ex!.Message, Does.Not.Contain("41"));
        Assert.That(ex.Message, Does.Not.Contain("42"));
    }

    [Test]
    public async Task CreateTree_authorizes_the_composed_id_as_a_whole_tree_admin_operation()
    {
        var gate = new TenantAdminTestSupport.RecordingGate();
        var facade = CreateFacade(out var treeAdmin, out _, gate);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.CreateTreeAsync("orders");

        Assert.That(gate.Calls, Is.EqualTo(1));
        Assert.That(gate.LastOperation, Is.EqualTo(LatticeOperation.Admin));
        Assert.That(gate.LastScope, Is.EqualTo("t/acme/orders"));
    }

    // ----- structural confinement: adversarial local names --------------------

    private static readonly string[] AdversarialNames =
    {
        "t/other/orders",
        "../evil",
        "/x",
        "other/orders",
        "..",
        "t/acme/../root",
        "..%2f..%2froot",
    };

    [TestCaseSource(nameof(AdversarialNames))]
    public async Task CreateTree_confines_any_local_name_to_the_active_tenant_namespace(string name)
    {
        string? captured = null;
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CreateTreeAsync(Arg.Do<string>(id => captured = id), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.CreateTreeAsync(name);

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.StartsWith("t/acme/", StringComparison.Ordinal), Is.True);
        Assert.That(LatticeTenantTrees.TryGetTenant(captured, out var owner), Is.True);
        Assert.That(owner, Is.EqualTo(TenantId.Parse(TenantValue)));
    }

    [TestCaseSource(nameof(AdversarialNames))]
    public async Task SetSchemaPolicy_confines_any_local_name_to_the_active_tenant_namespace(string name)
    {
        string? captured = null;
        var facade = CreateFacade(out _, out var schemaAdmin);
        schemaAdmin
            .SetPolicyAsync(Arg.Do<string>(id => captured = id), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        using var scope = ActiveTenant();
        await facade.SetSchemaPolicyAsync(name, new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>()));

        Assert.That(captured, Is.Not.Null);
        Assert.That(LatticeTenantTrees.TryGetTenant(captured!, out var owner), Is.True);
        Assert.That(owner, Is.EqualTo(TenantId.Parse(TenantValue)));
    }

    [Test]
    public async Task Same_name_under_different_active_tenants_composes_distinct_namespaces()
    {
        var captured = new List<string>();
        var facade = CreateFacade(out var treeAdmin, out _);
        treeAdmin
            .CheckTreeExistsAsync(Arg.Do<string>(id => captured.Add(id)), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeExistenceResult { TreeId = (string)ci[0]!, Exists = false }));

        using (var scope = ActiveTenant("acme"))
        {
            await facade.CheckTreeExistsAsync("orders");
        }

        using (var scope = ActiveTenant("other"))
        {
            await facade.CheckTreeExistsAsync("orders");
        }

        Assert.That(captured, Is.EqualTo(new[] { "t/acme/orders", "t/other/orders" }));
    }

    // ----- helpers ------------------------------------------------------------

    private static IEnumerable<TestCaseData> NameTakingOps()
    {
        yield return Op("CreateTree", (f, n) => f.CreateTreeAsync(n));
        yield return Op("CheckTreeExists", (f, n) => f.CheckTreeExistsAsync(n));
        yield return Op("DeleteTree", (f, n) => f.DeleteTreeAsync(n));
        yield return Op("RecoverTree", (f, n) => f.RecoverTreeAsync(n));
        yield return Op("PurgeTree", (f, n) => f.PurgeTreeAsync(n, confirm: true));
        yield return Op("GetTreeDeletionStatus", (f, n) => f.GetTreeDeletionStatusAsync(n));
        yield return Op("SetSchemaPolicy", (f, n) => f.SetSchemaPolicyAsync(n, new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>())));
        yield return Op("ClearSchemaPolicy", (f, n) => f.ClearSchemaPolicyAsync(n));
        yield return Op("GetSchemaPolicy", (f, n) => f.GetSchemaPolicyAsync(n));
    }

    private static TestCaseData Op(string name, Func<ILatticeTenantScopedTreeAdmin, string, Task> op)
        => new TestCaseData(op).SetName(name);

    private static IDisposable ActiveTenant(string value = TenantValue)
        => LatticeActiveTenantContext.With(TenantId.Parse(value));


    private static LatticeTenantScopedTreeAdmin CreateFacade(
        out ILatticeTreeAdmin treeAdmin,
        out ILatticeSchemaAdmin schemaAdmin,
        ILatticeAccessGate? gate = null)
    {
        treeAdmin = Substitute.For<ILatticeTreeAdmin>();
        schemaAdmin = Substitute.For<ILatticeSchemaAdmin>();
        return new LatticeTenantScopedTreeAdmin(
            treeAdmin, schemaAdmin, gate ?? new TenantAdminTestSupport.FixedGate(allow: true));
    }
}
