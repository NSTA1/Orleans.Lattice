namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Registry-level tree ownership: install and upgrade claim through the ledger, conflicts refuse the
/// transition without recording it, concurrent installs of one tree cannot both win, and uninstall
/// and upgrade release adopted claims while structural claims are held.
/// </summary>
[TestFixture]
public sealed class AppRegistryOwnershipTests
{
    private static readonly AppSlug Crm = AppSlug.Parse("crm");
    private static readonly AppSlug Billing = AppSlug.Parse("billing");
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private InMemoryAppRegistryStore _store = null!;
    private InMemoryAppTreeLedgerStore _ledger = null!;
    private FakeAppTreeFacts _facts = null!;
    private ActivationAppSource _source = null!;
    private AppRegistry _registry = null!;

    [SetUp]
    public void SetUp()
    {
        _store = new InMemoryAppRegistryStore();
        _ledger = new InMemoryAppTreeLedgerStore();
        _facts = new FakeAppTreeFacts();
        _source = new ActivationAppSource();
        _registry = AppRegistryTestData.CreateRegistry(
            _store, source: _source, ownership: AppRegistryTestData.CreateLedger(_store, _ledger, _facts));
    }

    private AppManifest Publish(AppSlug slug, AppVersion? version = null, params AppTreeDeclaration[] trees)
    {
        var manifest = ActivationHarness.Manifest(version, slug, trees.Length == 0 ? [ActivationHarness.Tree("records")] : trees);
        _source.Publish(manifest);
        return manifest;
    }

    private static AppRegistryInstallRequest Request(AppSlug slug, AppVersion? version = null, TenantId? tenant = null, string publisher = "first-party") =>
        AppRegistryTestData.Request(version, tenant, slug) with
        {
            Identity = new AppIdentity { Slug = slug, Version = version ?? AppRegistryTestData.V1, Provenance = new AppProvenance { Publisher = publisher } },
        };

    [Test]
    public async Task InstallAsync_claims_every_declared_tree_for_the_install()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("contacts"), ActivationHarness.Tree("legacy", adopted: "crm-legacy")]);

        var result = await _registry.InstallAsync(Request(Crm));

        Assert.That(result.Succeeded, Is.True, result.Message);
        Assert.That(_ledger.Peek("a/crm/contacts")!.Kind, Is.EqualTo(AppTreeClaimKind.Structural));
        Assert.That(_ledger.Peek("crm-legacy")!.Kind, Is.EqualTo(AppTreeClaimKind.Adopted));
        Assert.That(_ledger.Peek("crm-legacy")!.InstallRevision, Is.EqualTo(result.Record!.Revision));
    }

    [Test]
    public async Task A_second_app_cannot_adopt_a_tree_another_install_owns_and_nothing_is_recorded()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("legacy", adopted: "shared-legacy")]);
        Publish(Billing, trees: [ActivationHarness.Tree("old", adopted: "shared-legacy")]);
        await _registry.InstallAsync(Request(Crm));

        var result = await _registry.InstallAsync(Request(Billing));

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.TreeOwnershipConflict));
        Assert.That(result.Message, Is.EqualTo("Tree 'old' is owned by app 'crm'."));
        Assert.That(result.Record, Is.Null);
        Assert.That(_store.Peek(AppRegistryTreeNames.ComposeKey(TenantId.Default, Billing)), Is.Null);
        Assert.That(_ledger.Peek("shared-legacy")!.Slug, Is.EqualTo(Crm));
    }

    [Test]
    public async Task Of_two_concurrent_installs_adopting_one_tree_exactly_one_succeeds()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("legacy", adopted: "shared-legacy")]);
        Publish(Billing, trees: [ActivationHarness.Tree("old", adopted: "shared-legacy")]);

        // Interleave: crm's record lands, then billing's whole install runs before crm claims.
        var crmKey = AppRegistryTreeNames.ComposeKey(TenantId.Default, Crm);
        Task<AppRegistryTransitionResult>? billing = null;
        _ledger.BeforeSet = async _ =>
        {
            if (billing is null && _store.Peek(crmKey) is not null)
            {
                _ledger.BeforeSet = null;
                billing = _registry.InstallAsync(Request(Billing));
                await billing;
            }
        };

        var crm = await _registry.InstallAsync(Request(Crm));
        var billingResult = await billing!;

        Assert.That(new[] { crm.Succeeded, billingResult.Succeeded }.Count(s => s), Is.EqualTo(1));
        var loser = crm.Succeeded ? billingResult : crm;
        var winner = crm.Succeeded ? Crm : Billing;
        Assert.That(loser.Error, Is.EqualTo(AppRegistryTransitionError.TreeOwnershipConflict));
        Assert.That(loser.Record!.State, Is.EqualTo(AppRegistryLifecycleState.Uninstalled), "the loser's written record is rolled back");
        Assert.That(_ledger.Peek("shared-legacy")!.Slug, Is.EqualTo(winner));
    }

    [Test]
    public async Task InstallAsync_refuses_a_pre_existing_unowned_structural_tree()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("contacts")]);
        _facts.Registered.Add("a/crm/contacts");

        var result = await _registry.InstallAsync(Request(Crm));

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.TreeOwnershipConflict));
        Assert.That(result.Message, Does.Contain("pre-existing unowned tree"));
        Assert.That(_store.Peek(AppRegistryTreeNames.ComposeKey(TenantId.Default, Crm)), Is.Null);
    }

    [Test]
    public async Task A_different_publisher_cannot_reuse_an_uninstalled_slug_while_its_trees_are_retained()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("contacts")]);
        await _registry.InstallAsync(Request(Crm));
        _facts.Registered.Add("a/crm/contacts");
        await _registry.UninstallAsync(TenantId.Default, Crm);

        var impostor = await _registry.InstallAsync(Request(Crm, publisher: "contoso"));
        var sameOwner = await _registry.InstallAsync(Request(Crm));

        Assert.That(impostor.Error, Is.EqualTo(AppRegistryTransitionError.TreeOwnershipConflict));
        Assert.That(sameOwner.Succeeded, Is.True, "the same owner re-attaches to its held structural claim");
        Assert.That(_ledger.Peek("a/crm/contacts")!.Publisher, Is.EqualTo("first-party"));
    }

    [Test]
    public async Task UninstallAsync_releases_adopted_claims_and_holds_structural_ones()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("contacts"), ActivationHarness.Tree("legacy", adopted: "crm-legacy")]);
        await _registry.InstallAsync(Request(Crm));

        var result = await _registry.UninstallAsync(TenantId.Default, Crm);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(_ledger.Peek("crm-legacy")!.Released, Is.True);
        Assert.That(_ledger.Peek("a/crm/contacts")!.Released, Is.False);
    }

    [Test]
    public async Task UpgradeAsync_releases_an_adoption_the_new_version_drops_and_claims_new_trees()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("legacy", adopted: "crm-legacy")]);
        await _registry.InstallAsync(Request(Crm));
        Publish(Crm, AppRegistryTestData.V2, ActivationHarness.Tree("contacts"));

        var result = await _registry.UpgradeAsync(Request(Crm, AppRegistryTestData.V2));

        Assert.That(result.Succeeded, Is.True, result.Message);
        Assert.That(_ledger.Peek("crm-legacy")!.Released, Is.True);
        Assert.That(_ledger.Peek("a/crm/contacts")!.Released, Is.False);
    }

    [Test]
    public async Task UpgradeAsync_refused_on_conflict_keeps_the_installed_version()
    {
        Publish(Billing, trees: [ActivationHarness.Tree("old", adopted: "shared-legacy")]);
        await _registry.InstallAsync(Request(Billing));
        Publish(Crm);
        await _registry.InstallAsync(Request(Crm));
        Publish(Crm, AppRegistryTestData.V2, ActivationHarness.Tree("legacy", adopted: "shared-legacy"));

        var result = await _registry.UpgradeAsync(Request(Crm, AppRegistryTestData.V2));

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.TreeOwnershipConflict));
        Assert.That(_store.Peek(AppRegistryTreeNames.ComposeKey(TenantId.Default, Crm))!.Version, Is.EqualTo(AppRegistryTestData.V1));
    }

    [Test]
    public async Task The_same_tree_names_in_two_tenants_never_conflict()
    {
        Publish(Crm, trees: [ActivationHarness.Tree("contacts")]);

        var first = await _registry.InstallAsync(Request(Crm));
        var second = await _registry.InstallAsync(Request(Crm, tenant: Acme));

        Assert.That(first.Succeeded && second.Succeeded, Is.True);
        Assert.That(_ledger.Keys, Is.EquivalentTo(new[] { "a/crm/contacts", "t/acme/a/crm/contacts" }));
    }

    [Test]
    public async Task InstallAsync_leaves_claiming_to_activation_when_the_source_cannot_supply_the_version()
    {
        var result = await _registry.InstallAsync(Request(Crm));

        Assert.That(result.Succeeded, Is.True);
        Assert.That(_ledger.Keys, Is.Empty);
    }

    [Test]
    public async Task GetTreeOwnershipConflictsAsync_reports_conflicts_without_writing()
    {
        var crm = Publish(Crm, trees: [ActivationHarness.Tree("legacy", adopted: "shared-legacy")]);
        var billing = Publish(Billing, trees: [ActivationHarness.Tree("old", adopted: "shared-legacy"), ActivationHarness.Tree("own")]);
        await _registry.InstallAsync(Request(Crm));
        var writes = _ledger.AppliedWrites;

        var forBilling = await _registry.GetTreeOwnershipConflictsAsync(TenantId.Default, billing, new AppProvenance());
        var forCrm = await _registry.GetTreeOwnershipConflictsAsync(TenantId.Default, crm, new AppProvenance());

        Assert.That(forBilling.Single().TreeName, Is.EqualTo("old"));
        Assert.That(forBilling.Single().OwningApp, Is.EqualTo(Crm));
        Assert.That(forCrm, Is.Empty);
        Assert.That(_ledger.AppliedWrites, Is.EqualTo(writes));
    }

    [Test]
    public void GetTreeOwnershipConflictsAsync_validates_its_arguments()
    {
        var manifest = ActivationHarness.Manifest();

        Assert.That(() => _registry.GetTreeOwnershipConflictsAsync(TenantId.Default, null!, new AppProvenance()), Throws.ArgumentNullException);
        Assert.That(() => _registry.GetTreeOwnershipConflictsAsync(TenantId.Default, manifest, null!), Throws.ArgumentNullException);
        Assert.That(() => _registry.GetTreeOwnershipConflictsAsync(default, manifest, new AppProvenance()), Throws.ArgumentException);
    }
}
