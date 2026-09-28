namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppTreeOwnershipLedger"/> over an in-memory ledger, registry store and
/// scripted tree facts: claim standing, fresh-claim refusals, release, describe, cross-app owner
/// resolution and the alias ownership rule.
/// </summary>
[TestFixture]
public sealed partial class AppTreeOwnershipLedgerTests
{
    private static readonly TenantId Default = TenantId.Default;
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly AppSlug Crm = AppSlug.Parse("crm");
    private static readonly AppSlug Billing = AppSlug.Parse("billing");
    private static readonly AppTreeOwner CrmOwner = new(Default, Crm, "first-party");
    private static readonly AppTreeOwner BillingOwner = new(Default, Billing, "first-party");

    private InMemoryAppRegistryStore _registry = null!;
    private InMemoryAppTreeLedgerStore _store = null!;
    private FakeAppTreeFacts _facts = null!;
    private AppTreeOwnershipLedger _ledger = null!;

    [SetUp]
    public void SetUp()
    {
        _registry = new InMemoryAppRegistryStore();
        _store = new InMemoryAppTreeLedgerStore();
        _facts = new FakeAppTreeFacts();
        _ledger = AppRegistryTestData.CreateLedger(_registry, _store, _facts);
    }

    private static AppManifest Manifest(AppSlug slug, params AppTreeDeclaration[] trees) => new()
    {
        Identity = new AppIdentity { Slug = slug, Version = AppRegistryTestData.V1 },
        Trees = trees,
        Roles = [],
        Subscriptions = [],
        McpTools = [],
    };

    private static AppTreeDeclaration Tree(string name, string? adopted = null) => new() { Name = name, AdoptedTreeId = adopted };

    private static AppTreeClaimPlan Structural(AppSlug slug, string name, TenantId? tenant = null) =>
        new(AppActivationTreeNames.StructuralTree(tenant ?? Default, slug, name), name, AppTreeClaimKind.Structural);

    private static AppTreeClaimPlan Adopted(string treeId, string name) => new(treeId, name, AppTreeClaimKind.Adopted);

    private void Install(AppTreeOwner owner, AppRegistryLifecycleState state = AppRegistryLifecycleState.Installed) =>
        _registry.Seed(
            AppRegistryTreeNames.ComposeKey(owner.Tenant, owner.Slug),
            AppRegistryTestData.Record(state, owner.Tenant, owner.Slug) with { Provenance = new AppProvenance { Publisher = owner.Publisher } });

    private Task<AppTreeOwnershipConflict?> ClaimAsync(AppTreeOwner owner, params AppTreeClaimPlan[] plan) =>
        _ledger.ClaimAsync(owner, 1, plan, null, CancellationToken.None);

    [Test]
    public void Plan_maps_structural_and_adopted_trees_to_tenant_composed_keys_in_ordinal_order()
    {
        var manifest = Manifest(Crm, Tree("zeta"), Tree("legacy", adopted: "crm-legacy"), Tree("alpha"));

        var plan = AppTreeOwnershipLedger.Plan(manifest, Acme);

        Assert.That(plan, Is.EqualTo(new[]
        {
            new AppTreeClaimPlan("t/acme/a/crm/alpha", "alpha", AppTreeClaimKind.Structural),
            new AppTreeClaimPlan("t/acme/a/crm/zeta", "zeta", AppTreeClaimKind.Structural),
            new AppTreeClaimPlan("t/acme/crm-legacy", "legacy", AppTreeClaimKind.Adopted),
        }));
    }

    [Test]
    public void Plan_skips_an_adopted_id_that_can_never_be_adopted()
    {
        var plan = AppTreeOwnershipLedger.Plan(Manifest(Crm, Tree("registry", adopted: "sys-app-registry")), Default);

        Assert.That(plan, Is.Empty);
    }

    [Test]
    public async Task ClaimAsync_writes_a_fresh_claim_recording_the_owner_kind_and_revision()
    {
        var acquired = new List<string>();

        var conflict = await _ledger.ClaimAsync(CrmOwner, 7, [Structural(Crm, "contacts")], acquired, CancellationToken.None);

        Assert.That(conflict, Is.Null);
        Assert.That(acquired, Is.EqualTo(new[] { "a/crm/contacts" }));
        var claim = _store.Peek("a/crm/contacts")!;
        Assert.That(claim.Owner, Is.EqualTo(CrmOwner));
        Assert.That(claim.Kind, Is.EqualTo(AppTreeClaimKind.Structural));
        Assert.That(claim.InstallRevision, Is.EqualTo(7));
        Assert.That(claim.ClaimedAtUtc, Is.EqualTo(AppRegistryTestData.Start));
        Assert.That(claim.Released, Is.False);
    }

    [Test]
    public async Task ClaimAsync_is_idempotent_for_the_same_owner()
    {
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));
        _facts.Registered.Add("a/crm/contacts");
        var acquired = new List<string>();

        var conflict = await _ledger.ClaimAsync(CrmOwner, 2, [Structural(Crm, "contacts")], acquired, CancellationToken.None);

        Assert.That(conflict, Is.Null);
        Assert.That(acquired, Is.Empty, "a held claim is confirmed, not rewritten");
        Assert.That(_store.AppliedWrites, Is.EqualTo(1));
    }

    [Test]
    public async Task ClaimAsync_refuses_a_tree_another_installed_app_owns_naming_the_owner()
    {
        Install(CrmOwner);
        await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));

        var conflict = await ClaimAsync(BillingOwner, Adopted("legacy", "old-data"));

        Assert.That(conflict, Is.Not.Null);
        Assert.That(conflict!.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.OwnedByAnotherApp));
        Assert.That(conflict.OwningApp, Is.EqualTo(Crm));
        Assert.That(conflict.TreeName, Is.EqualTo("old-data"));
        Assert.That(conflict.Message, Is.EqualTo("Tree 'old-data' is owned by app 'crm'."));
    }

    [Test]
    public async Task ClaimAsync_refuses_the_same_slug_from_a_different_publisher_while_the_structural_tree_exists()
    {
        Install(CrmOwner);
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));
        _facts.Registered.Add("a/crm/contacts");
        Install(CrmOwner, AppRegistryLifecycleState.Uninstalled);
        var impostor = CrmOwner with { Publisher = "contoso" };

        var conflict = await ClaimAsync(impostor, Structural(Crm, "contacts"));

        Assert.That(conflict!.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.OwnedByAnotherApp));
        Assert.That(conflict.Message, Does.Contain("different publisher"));
    }

    [Test]
    public async Task A_structural_claim_of_an_uninstalled_owner_is_free_once_its_tree_is_purged()
    {
        Install(CrmOwner);
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));
        Install(CrmOwner, AppRegistryLifecycleState.Uninstalled);
        var impostor = CrmOwner with { Publisher = "contoso" };

        var conflict = await ClaimAsync(impostor, Structural(Crm, "contacts"));

        Assert.That(conflict, Is.Null);
        Assert.That(_store.Peek("a/crm/contacts")!.Publisher, Is.EqualTo("contoso"));
    }

    [Test]
    public async Task An_adopted_claim_of_an_uninstalled_owner_is_free()
    {
        Install(CrmOwner);
        await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));
        _facts.Registered.Add("legacy");
        Install(CrmOwner, AppRegistryLifecycleState.Uninstalled);

        var conflict = await ClaimAsync(BillingOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict, Is.Null);
        Assert.That(_store.Peek("legacy")!.Slug, Is.EqualTo(Billing));
    }

    [Test]
    public async Task ClaimAsync_refuses_a_pre_existing_unowned_structural_tree()
    {
        _facts.Registered.Add("a/crm/contacts");

        var conflict = await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));

        Assert.That(conflict!.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.PreExistingUnownedTree));
        Assert.That(conflict.OwningApp, Is.Null);
        Assert.That(_store.Peek("a/crm/contacts"), Is.Null);
    }

    [Test]
    public async Task ClaimAsync_adopts_a_pre_existing_tree()
    {
        _facts.Registered.Add("legacy");

        var conflict = await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict, Is.Null);
    }

    [Test]
    public async Task ClaimAsync_refuses_a_derived_tree()
    {
        _facts.Registered.Add("orders/resized/op-1");
        _facts.DerivedFrom["orders/resized/op-1"] = "orders";

        var conflict = await ClaimAsync(CrmOwner, Adopted("orders/resized/op-1", "copy"));

        Assert.That(conflict!.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.DerivedTree));
    }

    [Test]
    public async Task ClaimAsync_refuses_a_tree_whose_backing_is_another_trees_alias_target()
    {
        _facts.Registered.Add("legacy");
        _facts.Aliases["mine"] = "legacy";

        var conflict = await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict!.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.AliasTarget));
    }

    [Test]
    public async Task ClaimAsync_refuses_a_tree_aliased_to_a_tree_not_derived_from_it()
    {
        _facts.Registered.Add("legacy");
        _facts.Aliases["legacy"] = "unrelated";

        var conflict = await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict!.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.AliasTarget));
    }

    [Test]
    public async Task ClaimAsync_accepts_a_tree_aliased_to_its_own_derived_copy()
    {
        _facts.Registered.Add("legacy");
        _facts.Aliases["legacy"] = "legacy/resized/op-1";
        _facts.DerivedFrom["legacy/resized/op-1"] = "legacy";

        var conflict = await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict, Is.Null);
    }

    [Test]
    public async Task ClaimAsync_stops_at_the_first_conflict_and_reports_only_new_claims()
    {
        Install(BillingOwner);
        await ClaimAsync(BillingOwner, Adopted("shared", "shared"));
        var acquired = new List<string>();

        var conflict = await _ledger.ClaimAsync(
            CrmOwner, 1, [Structural(Crm, "a-first"), Adopted("shared", "shared"), Structural(Crm, "z-last")], acquired, CancellationToken.None);

        Assert.That(conflict!.TreeName, Is.EqualTo("shared"));
        Assert.That(acquired, Is.EqualTo(new[] { "a/crm/a-first" }));
        Assert.That(_store.Peek("a/crm/z-last"), Is.Null);
    }

    [Test]
    public async Task ClaimAsync_retries_after_losing_a_compare_and_set_and_then_sees_the_winner()
    {
        Install(BillingOwner);
        var interposed = false;
        _store.BeforeSet = async key =>
        {
            if (interposed)
                return;
            interposed = true;
            _store.Seed(key, new AppTreeClaim { Tenant = Default, Slug = Billing, Publisher = "first-party", Kind = AppTreeClaimKind.Adopted });
            await Task.CompletedTask;
        };

        var conflict = await ClaimAsync(CrmOwner, Adopted("legacy", "legacy"));

        Assert.That(conflict!.OwningApp, Is.EqualTo(Billing), "the competing claimant won the create-if-absent write");
    }

    [Test]
    public async Task Same_tree_names_in_different_tenants_never_conflict()
    {
        var acme = CrmOwner with { Tenant = Acme };
        Install(CrmOwner);
        await ClaimAsync(CrmOwner, Structural(Crm, "contacts"));

        var conflict = await ClaimAsync(acme, Structural(Crm, "contacts", Acme));

        Assert.That(conflict, Is.Null);
        Assert.That(_store.Keys, Is.EquivalentTo(new[] { "a/crm/contacts", "t/acme/a/crm/contacts" }));
    }
}
