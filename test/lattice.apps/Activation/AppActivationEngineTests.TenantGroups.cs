using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppActivationEngine"/> tenant confinement of role bindings: a binding to the installing
/// tenant's own group activates, and a stored binding to another tenant's group fails closed with
/// <see cref="AppActivationFailure.AppRoleBindingTenantMismatch"/>.
/// </summary>
public sealed partial class AppActivationEngineTests
{
    [Test]
    public async Task A_binding_to_the_installing_tenants_own_group_activates_and_grants_that_group()
    {
        var harness = new ActivationHarness();
        var acme = AppRegistryTestData.Acme;
        await harness.InstallAsync(ActivationHarness.Manifest(), acme, bindings: [AppRoleBinding.Create("reader", "t/acme/readers")]);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable, acme);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics));
        var rule = harness.Rules.Rules.Single(r => LatticeAppRuleIds.IsAppOwned(r.RuleId));
        Assert.That(rule.Subject, Is.EqualTo(LatticeSubjectSelector.Group("t/acme/readers")));
        Assert.That(rule.Scope.TreeId, Is.EqualTo("t/acme/a/notes/records"));
    }

    [Test]
    public async Task A_stored_binding_to_another_tenants_group_fails_closed_and_withdraws_earlier_rules()
    {
        var harness = new ActivationHarness();
        var acme = AppRegistryTestData.Acme;
        await harness.InstallAsync(ActivationHarness.Manifest(), acme);
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable, acme)).Succeeded, Is.True);
        Assert.That(harness.OwnedRuleIds(acme), Is.Not.Empty);

        // A record written before confinement existed, or by-passing the registry's write-time refusal.
        var enabled = await harness.Registry.GetAsync(acme, ActivationHarness.Slug);
        harness.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(acme, ActivationHarness.Slug),
            enabled! with { RoleBindings = [AppRoleBinding.Create("reader", "t/globex/readers")], Revision = enabled.Revision + 1 });

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile, acme);

        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.AppRoleBindingTenantMismatch));
        var diagnostic = outcome.Diagnostics.Single();
        Assert.That(diagnostic.Code, Is.EqualTo("role-binding-tenant-mismatch"));
        Assert.That(diagnostic.Message, Does.Contain("t/globex/readers"));
        Assert.That(harness.OwnedRuleIds(acme), Is.Empty, "the earlier grant is withdrawn, failing closed");
        Assert.That(harness.Rules.Rules.Any(r => r.Subject.Id == "t/globex/readers"), Is.False);
    }

    [Test]
    public async Task A_tenant_mismatch_is_the_reported_failure_even_when_the_ceiling_is_also_exceeded()
    {
        var harness = new ActivationHarness();
        var acme = AppRegistryTestData.Acme;
        harness.Source.Publish(ActivationHarness.Manifest(roles: [ActivationHarness.Role("reader", LatticeOperation.Read | LatticeOperation.Delete, "records")]));
        harness.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(acme, ActivationHarness.Slug),
            AppRegistryTestData.Record(AppRegistryLifecycleState.Installed, tenant: acme) with
            {
                RoleBindings = [AppRoleBinding.Create("reader", "t/globex/readers")],
            });

        var outcome = await harness.RunAsync(AppActivationOperation.Enable, acme);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.AppRoleBindingTenantMismatch));
        Assert.That(outcome.Diagnostics.Select(d => d.Code), Is.EquivalentTo(new[] { "ceiling-operations", "role-binding-tenant-mismatch" }));
        Assert.That(harness.Rules.Rules, Is.Empty);
        Assert.That(harness.Trees.Created, Is.Empty);
    }

    [Test]
    public async Task A_cluster_group_binding_still_activates_under_a_tenant()
    {
        var harness = new ActivationHarness();
        var acme = AppRegistryTestData.Acme;
        await harness.InstallAsync(ActivationHarness.Manifest(), acme, bindings: [AppRoleBinding.Create("reader", "readers")]);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable, acme);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics));
        Assert.That(harness.Rules.Rules.Single(r => LatticeAppRuleIds.IsAppOwned(r.RuleId)).Subject, Is.EqualTo(LatticeSubjectSelector.Group("readers")));
    }
}
