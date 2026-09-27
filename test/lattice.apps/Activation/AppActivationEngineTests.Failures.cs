using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppActivationEngine"/> failure paths: every failure is a structured, recorded
/// outcome and never an exception.
/// </summary>
public sealed partial class AppActivationEngineTests
{
    [Test]
    public async Task Over_ceiling_manifest_fails_activation_with_ceiling_diagnostics()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read | LatticeOperation.Delete, "records") });
        await harness.InstallAsync(manifest, ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("ceiling-operations"));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("Delete"));
        Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
        Assert.That(harness.Rules.Rules, Is.Empty);
        Assert.That(harness.Trees.Created, Is.Empty);
        var record = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);
        Assert.That(record!.State, Is.EqualTo(AppRegistryLifecycleState.Installed));

        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status!.LastOutcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        Assert.That(status.AppliedManifest, Is.Null);
    }

    [Test]
    public async Task Unapproved_foreign_scope_fails_activation_with_a_scope_diagnostic()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("legacy", adopted: "legacy-tree") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "legacy") });
        await harness.InstallAsync(manifest, ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("ceiling-scope"));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("legacy-tree"));
    }

    [Test]
    public async Task Binding_to_an_undeclared_role_fails_activation()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest(), bindings: new[] { AppRoleBinding.Create("writer", "writers") });

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.UnknownRoleBinding));
        Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("unknown-role-binding"));
    }

    [Test]
    public async Task Invalid_manifest_from_the_source_fails_activation_with_its_errors()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Source.Fail(AppSourceResult.InvalidManifest(ActivationHarness.Slug, new[] { new AppManifestError("json", "$", "broken") }));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.InvalidManifest));
        Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("json"));
        Assert.That(harness.Trees.Created, Is.Empty);
    }

    [Test]
    public async Task Manifest_failing_validation_fails_activation()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "missing") });
        await harness.InstallAsync(manifest);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.InvalidManifest));
        Assert.That(outcome.Diagnostics, Is.Not.Empty);
    }

    [Test]
    public async Task Source_version_differing_from_the_installed_version_fails_activation()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Source.Publish(ActivationHarness.Manifest(version: ActivationHarness.V2));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.VersionMismatch));
        Assert.That(outcome.Version, Is.EqualTo(ActivationHarness.V1));
    }

    [Test]
    public async Task Missing_source_registration_fails_activation()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Source.Fail(AppSourceResult.NotFound(ActivationHarness.Slug));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.SourceUnavailable));
    }

    [Test]
    public async Task Enabling_an_app_that_is_not_installed_fails()
    {
        var harness = new ActivationHarness();

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.NotInstalled));
        Assert.That(outcome.State, Is.Null);
        Assert.That(outcome.Version, Is.Null);
    }

    [Test]
    public async Task Enabling_with_an_unpinned_ceiling_fails()
    {
        var harness = new ActivationHarness();
        harness.Source.Publish(ActivationHarness.Manifest());
        harness.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug),
            AppRegistryTestData.Record(AppRegistryLifecycleState.Installed, ceilingVersion: ActivationHarness.V2));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingNotPinned));
    }

    [Test]
    public async Task Missing_membership_fails_closed_naming_the_chain()
    {
        var harness = new ActivationHarness(withMembership: false);
        await harness.InstallAsync(ActivationHarness.Manifest());

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.MembershipNotRegistered));
        Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("membership-not-registered"));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("App to Auth to Membership"));
        Assert.That(harness.Rules.Rules, Is.Empty);
        Assert.That(harness.Trees.Created, Is.Empty);
        var record = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);
        Assert.That(record!.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
    }

    [Test]
    public async Task Null_membership_context_counts_as_missing_membership()
    {
        var harness = new ActivationHarness(membership: new NullLatticeMembershipContext());
        await harness.InstallAsync(ActivationHarness.Manifest());

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(harness.Engine.IsMembershipRegistered, Is.False);
        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.MembershipNotRegistered));
    }

    [Test]
    public async Task Missing_policy_store_fails_closed_naming_the_chain()
    {
        var harness = new ActivationHarness(withPolicyStore: false);
        await harness.InstallAsync(ActivationHarness.Manifest());

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.AuthorizationNotRegistered));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("App to Auth to Membership"));
    }

    [Test]
    public async Task Tree_provisioning_failure_is_recorded()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Trees.FailEnsure = _ => new TimeoutException("registry busy");

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.TreeProvisioningFailed));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("registry busy"));
        Assert.That(harness.Rules.Rules, Is.Empty);
    }

    [Test]
    public async Task Rule_persistence_failure_is_recorded_and_leaves_the_app_not_enabled()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Rules.FailPut = _ => new InvalidOperationException("store down");

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.RulePersistenceFailed));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("store down"));
        var record = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);
        Assert.That(record!.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
    }

    [Test]
    public async Task Unexpected_fault_becomes_a_faulted_outcome()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.RegistryStore.BeforeSet = _ => throw new InvalidOperationException("boom");

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.Faulted));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("boom"));
        Assert.That(harness.Rules.Rules, Is.Empty, "a faulted enable fails closed");
    }

    [Test]
    public async Task An_unreadable_status_faults_the_run_and_is_never_overwritten()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var writes = harness.Status.Writes;
        harness.Status.FailReads = true;

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.Faulted));
        Assert.That(harness.Status.Writes, Is.EqualTo(writes));
        Assert.That(harness.Trees.SoftDeleted, Is.Empty);
        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(1));
    }

    [Test]
    public async Task A_version_that_no_longer_activates_withdraws_the_previous_rules()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(1));

        // Upgrade to a version whose role exceeds the (re-consented) ceiling.
        await harness.UpgradeAsync(
            ActivationHarness.Manifest(version: ActivationHarness.V2, roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Delete, "records") }),
            ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read));
        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
    }

    [Test]
    public async Task A_transient_failure_keeps_the_previous_rules()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        harness.Trees.FailEnsure = _ => new TimeoutException("slow");

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.TreeProvisioningFailed));
        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(1));
    }
}
