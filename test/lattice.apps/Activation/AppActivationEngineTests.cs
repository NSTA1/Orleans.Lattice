using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppActivationEngine"/>: successful activation end to end over an
/// in-memory registry, rule store, and tree provisioner.
/// </summary>
[TestFixture]
public sealed partial class AppActivationEngineTests
{
    private static readonly string RecordsTree = AppActivationTreeNames.LocalStructuralTree(ActivationHarness.Slug, "records");

    [Test]
    public async Task Enable_activates_end_to_end_and_marks_enabled()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.None));
        Assert.That(outcome.Operation, Is.EqualTo(AppActivationOperation.Enable));
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(outcome.Version, Is.EqualTo(ActivationHarness.V1));
        Assert.That(outcome.Changed, Is.True);
        Assert.That(outcome.Diagnostics, Is.Empty);
        Assert.That(outcome.CompletedAtUtc, Is.EqualTo(AppRegistryTestData.Start));

        Assert.That(harness.Trees.Created.Keys, Is.EquivalentTo(new[] { RecordsTree }));
        var rule = harness.Rules.Rules.Single();
        Assert.That(rule.RuleId, Does.StartWith("app:notes:reader:"));
        Assert.That(rule.Scope.TreeId, Is.EqualTo(RecordsTree));
        Assert.That(rule.Subject, Is.EqualTo(LatticeSubjectSelector.Group("readers")));
        Assert.That(rule.Operations, Is.EqualTo(LatticeOperation.Read));

        var record = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);
        Assert.That(record!.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));

        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status!.LastOutcome, Is.EqualTo(outcome));
        Assert.That(status.AppliedManifest!.Identity.Version, Is.EqualTo(ActivationHarness.V1));
        Assert.That(status.Tenant, Is.EqualTo(TenantId.Default));
        Assert.That(status.Slug, Is.EqualTo(ActivationHarness.Slug));
    }

    [Test]
    public async Task Enable_is_idempotent_and_writes_nothing_the_second_time()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var puts = harness.Rules.Puts;

        var again = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(again.Succeeded, Is.True);
        Assert.That(again.Changed, Is.False);
        Assert.That(harness.Rules.Puts, Is.EqualTo(puts));
        Assert.That(harness.Rules.Removes, Is.Zero);
        Assert.That(harness.Rules.Rules, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Enable_writes_rules_only_under_system_origin()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());

        // The in-memory store throws for an app-owned write outside system origin, mirroring R4.
        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
    }

    [Test]
    public async Task Enable_leaves_operator_and_other_app_rules_untouched()
    {
        var harness = new ActivationHarness();
        var operatorRule = new LatticeAuthorizationRule(
            "operator-rule", LatticeSubjectSelector.Group("ops"), LatticeScope.Tree(RecordsTree), LatticeOperation.Read, LatticeEffect.Allow);
        var otherApp = new LatticeAuthorizationRule(
            "app:notes-x:reader:0", LatticeSubjectSelector.Group("ops"), LatticeScope.Tree("a/notes-x/records"), LatticeOperation.Read, LatticeEffect.Allow);
        harness.Rules.Seed(operatorRule);
        harness.Rules.Seed(otherApp);
        await harness.InstallAsync(ActivationHarness.Manifest());

        await harness.RunAsync(AppActivationOperation.Enable);
        await harness.RunAsync(AppActivationOperation.Disable);

        Assert.That(harness.Rules.Rules, Is.EquivalentTo(new[] { operatorRule, otherApp }));
    }

    [Test]
    public async Task Enable_does_not_provision_adopted_trees()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("legacy", adopted: "legacy-tree") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records", "legacy") });
        await harness.InstallAsync(
            manifest,
            ceiling: new AppCapabilityCeiling
            {
                AllowedOperations = LatticeOperation.Read,
                ApprovedExceptionScopes = new[] { LatticeScope.Tree("legacy-tree") },
            });

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(harness.Trees.Created.Keys, Is.EquivalentTo(new[] { RecordsTree }));
        Assert.That(harness.Rules.Rules.Select(r => r.Scope.TreeId), Is.EquivalentTo(new[] { RecordsTree, "legacy-tree" }));
    }

    [Test]
    public async Task Enable_composes_tenant_trees_and_isolates_rule_sets_per_tenant()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.InstallAsync(ActivationHarness.Manifest(), tenant: AppRegistryTestData.Acme);
        await harness.RunAsync(AppActivationOperation.Enable);
        await harness.RunAsync(AppActivationOperation.Enable, AppRegistryTestData.Acme);

        Assert.That(harness.Trees.Created.Keys, Is.EquivalentTo(new[] { RecordsTree, "t/acme/" + RecordsTree }));
        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(1));
        Assert.That(harness.OwnedRuleIds(AppRegistryTestData.Acme), Has.Length.EqualTo(1));

        // Disabling one tenant's app must not withdraw the other tenant's rules.
        await harness.RunAsync(AppActivationOperation.Disable, AppRegistryTestData.Acme);

        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(1));
        Assert.That(harness.OwnedRuleIds(AppRegistryTestData.Acme), Is.Empty);
    }

    [Test]
    public async Task Enable_does_not_touch_app_code()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());

        // The fake source's activation handle throws if invoked.
        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True);
    }

    [Test]
    public void ExecuteAsync_rejects_uninitialised_arguments()
    {
        var harness = new ActivationHarness();

        Assert.ThrowsAsync<ArgumentException>(() => harness.Engine.ExecuteAsync(AppActivationOperation.Enable, default, ActivationHarness.Slug, default));
        Assert.ThrowsAsync<ArgumentException>(() => harness.Engine.ExecuteAsync(AppActivationOperation.Enable, TenantId.Default, default, default));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => harness.Engine.ExecuteAsync((AppActivationOperation)99, TenantId.Default, ActivationHarness.Slug, default));
    }

    [Test]
    public async Task A_status_store_failure_does_not_fail_the_run()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Status.FailWrites = true;

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True);
    }
}
