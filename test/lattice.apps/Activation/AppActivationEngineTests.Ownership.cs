using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Activation re-verifies tree ownership authoritatively: it takes a missing claim, fails closed on a
/// conflict introduced after install, and compiles cross-app scopes only against installed owners.
/// </summary>
public sealed partial class AppActivationEngineTests
{
    private static readonly AppSlug Billing = AppSlug.Parse("billing");

    private static void SeedInstalledOwner(ActivationHarness harness, AppSlug slug, string treeKey, AppTreeClaimKind kind = AppTreeClaimKind.Structural)
    {
        harness.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(TenantId.Default, slug),
            AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: slug));
        harness.Ledger.Seed(treeKey, new AppTreeClaim
        {
            Tenant = TenantId.Default,
            Slug = slug,
            Publisher = new AppProvenance().Publisher,
            Kind = kind,
        });
    }

    [Test]
    public async Task Enable_takes_a_claim_the_install_could_not_take()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest();
        await harness.Registry.InstallAsync(new AppRegistryInstallRequest
        {
            Identity = manifest.Identity,
            Ceiling = AppCapabilityCeiling.Structural(ActivationHarness.ReadWrite),
            RoleBindings = [AppRoleBinding.Create("reader", "readers")],
        });
        Assert.That(harness.Ledger.Keys, Is.Empty, "the source could not supply the manifest at install");
        harness.Source.Publish(manifest);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
        Assert.That(harness.Ledger.Peek(RecordsTree)!.Slug, Is.EqualTo(ActivationHarness.Slug));
    }

    [Test]
    public async Task Enable_fails_closed_on_an_ownership_conflict_introduced_after_install()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        SeedInstalledOwner(harness, Billing, RecordsTree);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.TreeOwnershipConflict));
        var diagnostic = outcome.Diagnostics.Single();
        Assert.That(diagnostic.Code, Is.EqualTo("tree-ownership"));
        Assert.That(diagnostic.Path, Is.EqualTo("$.trees[records]"));
        Assert.That(diagnostic.Message, Is.EqualTo("Tree 'records' is owned by app 'billing'."));
        Assert.That(harness.Trees.Ensured, Is.Empty, "nothing is provisioned for a tree the install does not own");
        Assert.That(harness.Rules.Rules, Is.Empty);
        Assert.That((await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug))!.State,
            Is.EqualTo(AppRegistryLifecycleState.Installed));
    }

    [Test]
    public async Task Reconcile_withdraws_the_rules_of_an_enabled_app_that_lost_a_tree()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        Assert.That(harness.Rules.Rules, Has.Count.EqualTo(1));
        SeedInstalledOwner(harness, Billing, RecordsTree);

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.TreeOwnershipConflict));
        Assert.That(harness.Rules.Rules, Is.Empty, "an ownership conflict fails closed");
    }

    [Test]
    public async Task Cross_app_scope_fails_activation_until_the_named_app_is_an_installed_owner()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(roles:
        [
            new AppRoleDeclaration
            {
                Name = "reader",
                Operations = LatticeOperation.Read,
                Scopes = [new AppScopeTemplate { Tree = "invoices", App = Billing }],
            },
        ]);
        var ceiling = AppCapabilityCeiling.Structural(ActivationHarness.ReadWrite) with
        {
            ApprovedExceptionScopes = [LatticeScope.Tree("a/billing/invoices")],
        };
        await harness.InstallAsync(manifest, ceiling: ceiling);

        var absent = await harness.RunAsync(AppActivationOperation.Enable);
        SeedInstalledOwner(harness, Billing, "a/billing/invoices");
        var present = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(absent.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        Assert.That(absent.Diagnostics.Single().Code, Is.EqualTo("ceiling-scope"));
        Assert.That(present.Succeeded, Is.True, () => string.Join("; ", present.Diagnostics.Select(d => d.Message)));
        Assert.That(harness.Rules.Rules.Single().Scope, Is.EqualTo(LatticeScope.Tree("a/billing/invoices")));
    }
}
