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
                null!, Substitute.For<ILatticeSchemaAdmin>(), Admission(active: false, admit: true),
                new TenantAdminTestSupport.FixedGate(allow: true)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_schema_admin_throws()
        => Assert.That(
            () => new LatticeTenantScopedTreeAdmin(
                Substitute.For<ILatticeTreeAdmin>(), null!, Admission(active: false, admit: true),
                new TenantAdminTestSupport.FixedGate(allow: true)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_admission_throws()
        => Assert.That(
            () => new LatticeTenantScopedTreeAdmin(
                Substitute.For<ILatticeTreeAdmin>(), Substitute.For<ILatticeSchemaAdmin>(), null!,
                new TenantAdminTestSupport.FixedGate(allow: true)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_gate_throws()
        => Assert.That(
            () => new LatticeTenantScopedTreeAdmin(
                Substitute.For<ILatticeTreeAdmin>(), Substitute.For<ILatticeSchemaAdmin>(),
                Admission(active: false, admit: true), null!),
            Throws.ArgumentNullException);

    // ----- fail-closed: no active tenant refuses every op ---------------------

    [TestCaseSource(nameof(NameTakingOps))]
    public void Op_without_active_tenant_throws_TenantScopeRequired(
        Func<ILatticeTenantScopedTreeAdmin, string, Task> op)
    {
        var facade = CreateFacade(out _, out _, Admission(active: false, admit: true));

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
        var facade = CreateFacade(out _, out _, Admission(active: false, admit: true));

        Assert.That(async () => await op(facade, string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [TestCaseSource(nameof(NameTakingOps))]
    public void Op_with_null_name_throws_ArgumentException(
        Func<ILatticeTenantScopedTreeAdmin, string, Task> op)
    {
        using var scope = ActiveTenant();
        var facade = CreateFacade(out _, out _, Admission(active: false, admit: true));

        Assert.That(async () => await op(facade, null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetSchemaPolicy_null_policy_throws_ArgumentNullException()
    {
        using var scope = ActiveTenant();
        var facade = CreateFacade(out _, out _, Admission(active: false, admit: true));

        Assert.That(
            async () => await facade.SetSchemaPolicyAsync("orders", null!),
            Throws.ArgumentNullException);
    }

    // ----- namespace composition (happy-path delegation) ----------------------

    [Test]
    public async Task CreateTree_delegates_with_composed_id_and_passes_sizing_through()
    {
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out _, out var schemaAdmin, Admission(active: false, admit: true));
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());

        using var scope = ActiveTenant();
        await facade.SetSchemaPolicyAsync("orders", policy);

        await schemaAdmin.Received(1).SetPolicyAsync("t/acme/orders", policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ClearSchemaPolicy_delegates_with_composed_id_and_returns_result()
    {
        var facade = CreateFacade(out _, out var schemaAdmin, Admission(active: false, admit: true));
        schemaAdmin.ClearPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));

        using var scope = ActiveTenant();
        var removed = await facade.ClearSchemaPolicyAsync("orders");

        Assert.That(removed, Is.True);
        await schemaAdmin.Received(1).ClearPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetSchemaPolicy_delegates_with_composed_id_and_returns_null_passthrough()
    {
        var facade = CreateFacade(out _, out var schemaAdmin, Admission(active: false, admit: true));
        schemaAdmin.GetPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeSchemaPolicy?>(null));

        using var scope = ActiveTenant();
        var policy = await facade.GetSchemaPolicyAsync("orders");

        Assert.That(policy, Is.Null);
        await schemaAdmin.Received(1).GetPolicyAsync("t/acme/orders", Arg.Any<CancellationToken>());
    }

    // ----- quota admission on create ------------------------------------------

    [Test]
    public async Task CreateTree_when_admission_active_and_admits_delegates_and_records_scope()
    {
        var admission = Admission(active: true, admit: true);
        var facade = CreateFacade(out var treeAdmin, out _, admission);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.CreateTreeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(admission.AdmitCalls, Is.EqualTo(1));
            Assert.That(admission.LastTenant, Is.EqualTo(TenantId.Parse(TenantValue)));
            Assert.That(admission.LastTreeId, Is.EqualTo("t/acme/orders"));
        });
        await treeAdmin.Received(1).CreateTreeAsync("t/acme/orders", null, null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateTree_when_admission_refuses_throws_and_does_not_create()
    {
        var admission = Admission(active: true, admit: false);
        var facade = CreateFacade(out var treeAdmin, out _, admission);

        using var scope = ActiveTenant();
        Assert.That(
            async () => await facade.CreateTreeAsync("orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());

        Assert.That(admission.AdmitCalls, Is.EqualTo(1));
        await treeAdmin.DidNotReceive().CreateTreeAsync(
            Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateTree_when_admission_inactive_skips_admission_and_delegates()
    {
        var admission = Admission(active: false, admit: true);
        var facade = CreateFacade(out var treeAdmin, out _, admission);
        treeAdmin
            .CreateTreeAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(new TreeCreationResult { TreeId = (string)ci[0]! }));

        using var scope = ActiveTenant();
        await facade.CreateTreeAsync("orders");

        Assert.That(admission.AdmitCalls, Is.EqualTo(0));
        await treeAdmin.Received(1).CreateTreeAsync("t/acme/orders", null, null, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CreateTree_propagates_quota_exceeded_from_controller_and_does_not_create()
    {
        var admission = Admission(active: true, admit: true, throwOnAdmit: new LatticeQuotaExceededException("quota"));
        var facade = CreateFacade(out var treeAdmin, out _, admission);

        using var scope = ActiveTenant();
        Assert.That(
            async () => await facade.CreateTreeAsync("orders"),
            Throws.TypeOf<LatticeQuotaExceededException>());

        await treeAdmin.DidNotReceive().CreateTreeAsync(
            Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    // ----- authorize-before-account ordering (cross-tenant regression) --------

    [Test]
    public async Task CreateTree_when_gate_denies_never_consults_admission()
    {
        // The active tenant is a client-supplied assertion that only the access
        // gate validates. Consulting the quota controller first let an
        // unauthorized caller nominate any victim tenant and have a stateful,
        // quota-consuming, rate-limiting evaluation charged to it - confirming
        // the tenant's existence, draining its rate budget, and leaking its
        // current usage and ceiling through the quota exception's message.
        var admission = Admission(active: true, admit: true);
        var facade = CreateFacade(
            out var treeAdmin, out _, admission, new TenantAdminTestSupport.FixedGate(allow: false));

        using var scope = ActiveTenant("victim");
        Assert.That(
            async () => await facade.CreateTreeAsync("orders"),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());

        Assert.That(admission.AdmitCalls, Is.Zero,
            "admission must not be consulted for a create the access gate denies");
        await treeAdmin.DidNotReceive().CreateTreeAsync(
            Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void CreateTree_when_gate_denies_reports_authorization_not_quota()
    {
        // A denied caller must not be able to distinguish "no such tenant" from
        // "tenant over quota": the refusal is an authorization denial carrying no
        // tenant usage figures, even when the controller would have thrown a
        // quota breach naming them.
        var admission = Admission(
            active: true, admit: true, throwOnAdmit: new LatticeQuotaExceededException("current=41 ceiling=42"));
        var facade = CreateFacade(
            out _, out _, admission, new TenantAdminTestSupport.FixedGate(allow: false));

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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true), gate);
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out _, out var schemaAdmin, Admission(active: false, admit: true));
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
        var facade = CreateFacade(out var treeAdmin, out _, Admission(active: false, admit: true));
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

    private static FakeAdmissionController Admission(bool active, bool admit, Exception? throwOnAdmit = null)
        => new(active, admit, throwOnAdmit);

    private static LatticeTenantScopedTreeAdmin CreateFacade(
        out ILatticeTreeAdmin treeAdmin,
        out ILatticeSchemaAdmin schemaAdmin,
        ITenantAdmissionController admission,
        ILatticeAccessGate? gate = null)
    {
        treeAdmin = Substitute.For<ILatticeTreeAdmin>();
        schemaAdmin = Substitute.For<ILatticeSchemaAdmin>();
        return new LatticeTenantScopedTreeAdmin(
            treeAdmin, schemaAdmin, admission, gate ?? new TenantAdminTestSupport.FixedGate(allow: true));
    }

    /// <summary>
    /// A deterministic <see cref="ITenantAdmissionController"/> double that records
    /// the scope it was consulted with and returns (or throws) a configured
    /// decision. No timing or ordering assumptions.
    /// </summary>
    private sealed class FakeAdmissionController : ITenantAdmissionController
    {
        private readonly bool _active;
        private readonly bool _admit;
        private readonly Exception? _throw;

        public FakeAdmissionController(bool active, bool admit, Exception? throwOnAdmit)
        {
            _active = active;
            _admit = admit;
            _throw = throwOnAdmit;
        }

        public int AdmitCalls { get; private set; }

        public TenantId? LastTenant { get; private set; }

        public string? LastTreeId { get; private set; }

        public bool IsActive => _active;

        public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
        {
            AdmitCalls++;
            LastTenant = tenant;
            LastTreeId = treeId;

            if (_throw is not null)
            {
                throw _throw;
            }

            return new ValueTask<bool>(_admit);
        }
    }
}
