using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppActivationEngine"/> lifecycle runs beyond enable: disable, uninstall,
/// reconcile, and upgrade.
/// </summary>
public sealed partial class AppActivationEngineTests
{
    [Test]
    public async Task Disable_withdraws_rules_keeps_trees_and_marks_disabled()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);

        var outcome = await harness.RunAsync(AppActivationOperation.Disable);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Disabled));
        Assert.That(outcome.Changed, Is.True);
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
        Assert.That(harness.Trees.SoftDeleted, Is.Empty);
        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status!.AppliedManifest, Is.Not.Null);
    }

    [Test]
    public async Task Disable_does_not_require_membership()
    {
        var harness = new ActivationHarness(withMembership: false);
        harness.Source.Publish(ActivationHarness.Manifest());
        harness.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug),
            AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled));

        var outcome = await harness.RunAsync(AppActivationOperation.Disable);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Disabled));
    }

    [Test]
    public async Task Disable_of_an_installed_app_is_an_invalid_transition()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());

        var outcome = await harness.RunAsync(AppActivationOperation.Disable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.InvalidTransition));
    }

    [Test]
    public async Task Uninstall_withdraws_rules_soft_deletes_structural_trees_only_and_marks_uninstalled()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("legacy", adopted: "legacy-tree") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records") });
        await harness.InstallAsync(manifest);
        await harness.RunAsync(AppActivationOperation.Enable);

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Uninstalled));
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
        Assert.That(harness.Trees.SoftDeleted, Is.EquivalentTo(new[] { RecordsTree }));
        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status!.AppliedManifest, Is.Null);
    }

    [Test]
    public async Task Uninstall_of_a_never_activated_app_retires_the_declared_trees()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Trees.Created[RecordsTree] = ActivationHarness.Tree("records");

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(harness.Trees.SoftDeleted, Is.EquivalentTo(new[] { RecordsTree }));
    }

    [Test]
    public async Task Uninstall_tree_failure_is_recorded_and_leaves_the_record_installed()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        harness.Trees.FailDelete = _ => new InvalidOperationException("view source");

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.TreeProvisioningFailed));
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
    }

    [Test]
    public async Task Uninstall_again_is_an_idempotent_no_op()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Uninstall);

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.Changed, Is.False);
    }

    [Test]
    public async Task Reinstall_after_uninstall_recovers_the_trees()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        await harness.RunAsync(AppActivationOperation.Uninstall);
        Assert.That(harness.Trees.SoftDeleted, Does.Contain(RecordsTree));

        await harness.InstallAsync(ActivationHarness.Manifest());
        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(harness.Trees.SoftDeleted, Is.Empty);
    }

    [Test]
    public async Task Reconcile_of_an_enabled_app_reconverges_its_rules()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var stale = harness.Rules.Rules.Single();
        await LatticeRemoveAsync(harness, stale);

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.Changed, Is.False);
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
        Assert.That(harness.OwnedRuleIds(), Is.EqualTo(new[] { stale.RuleId }));
    }

    [Test]
    public async Task Reconcile_of_a_disabled_app_withdraws_leftover_rules()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var leftover = harness.Rules.Rules.Single();
        await harness.RunAsync(AppActivationOperation.Disable);
        harness.Rules.Seed(leftover);

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Disabled));
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
    }

    [Test]
    public async Task Reconcile_of_an_app_that_is_not_installed_fails()
    {
        var harness = new ActivationHarness();

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.NotInstalled));
    }

    [Test]
    public async Task Upgrade_that_drops_a_tree_soft_deletes_it_and_replaces_the_rule_set()
    {
        var harness = new ActivationHarness();
        var v1 = ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("drafts") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records", "drafts") });
        await harness.InstallAsync(v1);
        await harness.RunAsync(AppActivationOperation.Enable);
        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(2));

        await harness.UpgradeAsync(ActivationHarness.Manifest(version: ActivationHarness.V2));
        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        var drafts = AppActivationTreeNames.LocalStructuralTree(ActivationHarness.Slug, "drafts");
        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(outcome.Version, Is.EqualTo(ActivationHarness.V2));
        Assert.That(harness.Trees.SoftDeleted, Is.EquivalentTo(new[] { drafts }));
        Assert.That(harness.Rules.Rules.Select(r => r.Scope.TreeId), Is.EquivalentTo(new[] { RecordsTree }));
        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status!.AppliedManifest!.Identity.Version, Is.EqualTo(ActivationHarness.V2));
    }

    [Test]
    public async Task Upgrade_changing_the_virtual_shard_count_fails_validation()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest(trees: new[] { ActivationHarness.Tree("records", virtualShards: 16) }));
        await harness.RunAsync(AppActivationOperation.Enable);

        await harness.UpgradeAsync(ActivationHarness.Manifest(version: ActivationHarness.V2, trees: new[] { ActivationHarness.Tree("records", virtualShards: 32) }));
        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.InvalidManifest));
        Assert.That(harness.Trees.Created[RecordsTree].VirtualShardCount, Is.EqualTo(16));
    }

    private static async Task LatticeRemoveAsync(ActivationHarness harness, LatticeAuthorizationRule rule)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await harness.Rules.RemoveRuleAsync(rule.Scope.TreeId, rule.RuleId);
        }
    }
}
