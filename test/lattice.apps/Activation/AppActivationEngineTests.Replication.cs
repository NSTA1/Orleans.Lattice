using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppActivationEngineTests
{
    private static AppManifest ReplicatedManifest(AppVersion? version = null, LatticeMergeMode mode = LatticeMergeMode.LwwRegister) =>
        ActivationHarness.Manifest(version: version) with
        {
            Replication = new[] { new AppReplicationDeclaration { Tree = "records", MergeMode = mode } },
        };

    [TestCase(false)]
    [TestCase(true)]
    public async Task Enable_and_reconcile_enrol_only_the_installs_tenant_composed_trees(bool tenantScoped)
    {
        var tenant = tenantScoped ? AppRegistryTestData.Acme : TenantId.Default;
        var expected = tenantScoped ? "t/acme/a/notes/records" : "a/notes/records";
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest(mode: LatticeMergeMode.OrSet), tenant);
        Assert.That(authority.Trees, Is.Empty);

        Assert.That((await harness.RunAsync(AppActivationOperation.Enable, tenant)).Succeeded, Is.True);
        Assert.That((await harness.RunAsync(AppActivationOperation.Reconcile, tenant)).Succeeded, Is.True);

        Assert.That(authority.Trees.Keys, Is.EquivalentTo(new[] { expected }));
        Assert.That(authority.Trees[expected].Mode, Is.EqualTo(LatticeMergeMode.OrSet));
        Assert.That(authority.Enables, Is.EqualTo(new[] { expected, expected }));
    }

    [Test]
    public async Task Disable_keeps_replication_and_uninstall_disables_it()
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That((await harness.RunAsync(AppActivationOperation.Disable)).Succeeded, Is.True);
        Assert.That((await harness.RunAsync(AppActivationOperation.Reconcile)).Succeeded, Is.True);
        Assert.That(authority.Disables, Is.Empty);
        Assert.That(authority.Trees["a/notes/records"].Enabled, Is.True);

        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall)).Succeeded, Is.True);
        Assert.That(authority.Disables, Is.EqualTo(new[] { "a/notes/records" }));
        Assert.That(authority.Trees["a/notes/records"].Enabled, Is.False);
        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall)).Succeeded, Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Upgrade_dropping_replication_or_the_tree_disables_previous_enrolment(bool dropTree)
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var upgraded = ActivationHarness.Manifest(version: ActivationHarness.V2,
            trees: dropTree ? new[] { ActivationHarness.Tree("replacement") } : null,
            roles: dropTree ? new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "replacement") } : null);
        await harness.UpgradeAsync(upgraded);

        Assert.That((await harness.RunAsync(AppActivationOperation.Reconcile)).Succeeded, Is.True);
        Assert.That(authority.Disables, Is.EqualTo(new[] { "a/notes/records" }));
        Assert.That(authority.Trees["a/notes/records"].Enabled, Is.False);
    }

    [Test]
    public async Task Upgrade_mode_change_rejects_before_changing_any_grants_or_replication()
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var before = harness.Rules.Rules.ToArray();
        var upgraded = ReplicatedManifest(ActivationHarness.V2, LatticeMergeMode.OrSet) with
        {
            Trees = new[] { ActivationHarness.Tree("new-tree"), ActivationHarness.Tree("records") },
            Replication = new[]
            {
                new AppReplicationDeclaration { Tree = "new-tree", MergeMode = LatticeMergeMode.LwwRegister },
                new AppReplicationDeclaration { Tree = "records", MergeMode = LatticeMergeMode.OrSet },
            },
        };
        await harness.UpgradeAsync(upgraded);

        var result = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(result.Failure, Is.EqualTo(AppActivationFailure.ReplicationModeChangeRejected));
        Assert.That(result.Diagnostics.Single().Message, Does.Contain("a/notes/records"));
        Assert.That(harness.Rules.Rules, Is.EqualTo(before));
        Assert.That(authority.Trees.Keys, Is.EquivalentTo(new[] { "a/notes/records" }));
        Assert.That(authority.Trees["a/notes/records"].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        Assert.That(authority.Enables, Has.Count.EqualTo(1));
        Assert.That(authority.Disables, Is.Empty);
    }

    [Test]
    public async Task Enable_without_replication_addon_ignores_intent()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ReplicatedManifest());
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Succeeded, Is.True);
        Assert.That((await harness.RunAsync(AppActivationOperation.Reconcile)).Succeeded, Is.True);
        Assert.That((await harness.RunAsync(AppActivationOperation.Disable)).Succeeded, Is.True);
        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall)).Succeeded, Is.True);
    }

    [Test]
    public async Task Uninstall_one_tenant_keeps_the_other_install_enrolled()
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.InstallAsync(ReplicatedManifest(), AppRegistryTestData.Acme);
        await harness.RunAsync(AppActivationOperation.Enable);
        await harness.RunAsync(AppActivationOperation.Enable, AppRegistryTestData.Acme);

        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall, AppRegistryTestData.Acme)).Succeeded, Is.True);
        Assert.That(authority.Trees["a/notes/records"].Enabled, Is.True);
        Assert.That(authority.Trees["t/acme/a/notes/records"].Enabled, Is.False);
    }

    [Test]
    public async Task Enable_ambiguous_mode_is_rejected_without_mutating_enrolment()
    {
        var authority = new RecordingReplicationAuthority();
        authority.Trees["a/notes/records"] = new("a/notes/records", true, null, true);
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());

        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Failure,
            Is.EqualTo(AppActivationFailure.ReplicationModeChangeRejected));
        Assert.That(authority.Enables, Is.Empty);
        Assert.That(harness.Status.Writes, Is.EqualTo(1), "preflight must not write a pending enrolment journal");
    }

    [Test]
    public async Task Uninstall_authority_failure_is_structured_and_retry_cleans_up()
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        authority.DisableFailure = new IOException("config unavailable");

        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall)).Failure,
            Is.EqualTo(AppActivationFailure.ReplicationEnrolmentFailed));
        Assert.That(authority.Trees["a/notes/records"].Enabled, Is.True);
        authority.DisableFailure = null;
        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall)).Succeeded, Is.True);
        Assert.That(authority.Trees["a/notes/records"].Enabled, Is.False);
    }

    [Test]
    public async Task Enable_journal_failure_prevents_replication_side_effects()
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        harness.Status.FailWrites = true;

        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Failure,
            Is.EqualTo(AppActivationFailure.ReplicationEnrolmentFailed));
        Assert.That(authority.Enables, Is.Empty);
        Assert.That(authority.Disables, Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Interrupted_upgrade_keeps_durable_enrolment_cleanup_even_when_outcome_cannot_be_saved(bool provisioningFailure)
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var upgraded = ReplicatedManifest(ActivationHarness.V2) with
        {
            Trees = new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("extra") },
            Replication = new[]
            {
                new AppReplicationDeclaration { Tree = "records", MergeMode = LatticeMergeMode.LwwRegister },
                new AppReplicationDeclaration { Tree = "extra", MergeMode = LatticeMergeMode.LwwRegister },
            },
        };
        await harness.UpgradeAsync(upgraded);
        authority.FailEnable = tree =>
        {
            harness.Status.FailWrites = true;
            return !provisioningFailure && tree.EndsWith("/extra", StringComparison.Ordinal)
                ? new IOException("enrolment interrupted") : null;
        };
        harness.Trees.FailEnsure = _ => new IOException("provisioning interrupted");

        var failed = await harness.RunAsync(AppActivationOperation.Reconcile);
        Assert.That(failed.Failure, Is.EqualTo(provisioningFailure
            ? AppActivationFailure.TreeProvisioningFailed : AppActivationFailure.ReplicationEnrolmentFailed));
        var pending = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(pending!.AppliedManifest!.Identity.Version, Is.EqualTo(ActivationHarness.V1));
        Assert.That(pending.ReplicationTrees.Keys, Is.EquivalentTo(new[] { "a/notes/records", "a/notes/extra" }));
        Assert.That(pending.ReplicationTrees.Values, Is.All.True);
        Assert.That(pending.LastOutcome.Diagnostics.Single().Code, Is.EqualTo("replication-pending"));

        harness.Status.FailWrites = false;
        authority.FailEnable = null;
        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall)).Succeeded, Is.True);
        Assert.That(authority.Disables, Is.EquivalentTo(new[] { "a/notes/records", "a/notes/extra" }));
        Assert.That(authority.Trees.Values.All(tree => !tree.Enabled), Is.True);
        var final = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(final!.ReplicationTrees, Is.Empty);
    }

    [TestCase(AppActivationFailure.ReplicationPreconditionFailed)]
    [TestCase(AppActivationFailure.ReplicationModeChangeRejected)]
    [TestCase(AppActivationFailure.ReplicationEnrolmentFailed)]
    public async Task Authority_failure_is_structured_and_preserves_previous_grants(AppActivationFailure expected)
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        await harness.InstallAsync(ReplicatedManifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        var before = harness.Rules.Rules.ToArray();
        authority.EnableFailure = expected switch
        {
            AppActivationFailure.ReplicationPreconditionFailed => new LatticeReplicationPreconditionFailedException("ClusterId missing"),
            AppActivationFailure.ReplicationModeChangeRejected => new LatticeReplicationModeChangeRejectedException("mode changed"),
            _ => new IOException("config store unavailable"),
        };

        var result = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(result.Failure, Is.EqualTo(expected));
        Assert.That(result.Diagnostics.Single().Message, Does.Contain("a/notes/records"));
        Assert.That(harness.Rules.Rules, Is.EqualTo(before));
    }

    [Test]
    public async Task Adopted_tree_is_composed_and_unenrolled_without_deleting_its_data()
    {
        var authority = new RecordingReplicationAuthority();
        var harness = new ActivationHarness(replication: authority);
        var manifest = ReplicatedManifest() with
        {
            Trees = new[] { ActivationHarness.Tree("records", adopted: "legacy-tree") },
            Roles = Array.Empty<AppRoleDeclaration>(),
        };
        await harness.InstallAsync(manifest, AppRegistryTestData.Acme, bindings: Array.Empty<AppRoleBinding>());
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable, AppRegistryTestData.Acme)).Succeeded, Is.True);
        Assert.That(authority.Trees.Keys, Is.EquivalentTo(new[] { "t/acme/legacy-tree" }));

        Assert.That((await harness.RunAsync(AppActivationOperation.Uninstall, AppRegistryTestData.Acme)).Succeeded, Is.True);
        Assert.That(authority.Disables, Is.EqualTo(new[] { "t/acme/legacy-tree" }));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Retire_app_keeps_preexisting_adopted_replication(bool upgrade)
    {
        const string tree = "t/acme/legacy-tree";
        var authority = new RecordingReplicationAuthority();
        authority.Trees[tree] = new(tree, true, LatticeMergeMode.LwwRegister, false);
        var harness = new ActivationHarness(replication: authority);
        var manifest = ReplicatedManifest() with
        {
            Trees = new[] { ActivationHarness.Tree("records", adopted: "legacy-tree") },
            Roles = Array.Empty<AppRoleDeclaration>(),
        };
        await harness.InstallAsync(manifest, AppRegistryTestData.Acme, bindings: Array.Empty<AppRoleBinding>());
        await harness.RunAsync(AppActivationOperation.Enable, AppRegistryTestData.Acme);
        await harness.RunAsync(AppActivationOperation.Reconcile, AppRegistryTestData.Acme);
        if (upgrade)
        {
            await harness.UpgradeAsync(manifest with
            {
                Identity = manifest.Identity with { Version = ActivationHarness.V2 },
                Replication = Array.Empty<AppReplicationDeclaration>(),
            }, AppRegistryTestData.Acme, bindings: Array.Empty<AppRoleBinding>());
        }

        var result = await harness.RunAsync(upgrade ? AppActivationOperation.Reconcile : AppActivationOperation.Uninstall,
            AppRegistryTestData.Acme);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(authority.Disables, Is.Empty);
        Assert.That(authority.Trees[tree].Enabled, Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Interrupted_upgrade_restart_preserves_owned_and_preexisting_enrolment_provenance(bool reconcileFirst)
    {
        const string legacy = "t/acme/legacy-tree";
        const string owned = "t/acme/a/notes/extra";
        var authority = new RecordingReplicationAuthority();
        authority.Trees[legacy] = new(legacy, true, LatticeMergeMode.LwwRegister, false);
        var harness = new ActivationHarness(replication: authority);
        var manifest = ReplicatedManifest() with
        {
            Trees = new[] { ActivationHarness.Tree("records", adopted: "legacy-tree") },
            Roles = Array.Empty<AppRoleDeclaration>(),
        };
        await harness.InstallAsync(manifest, AppRegistryTestData.Acme, bindings: Array.Empty<AppRoleBinding>());
        await harness.RunAsync(AppActivationOperation.Enable, AppRegistryTestData.Acme);
        var upgraded = manifest with
        {
            Identity = manifest.Identity with { Version = ActivationHarness.V2 },
            Trees = new[] { manifest.Trees.Single(), ActivationHarness.Tree("extra") },
            Replication = new[]
            {
                new AppReplicationDeclaration { Tree = "records", MergeMode = LatticeMergeMode.LwwRegister },
                new AppReplicationDeclaration { Tree = "extra", MergeMode = LatticeMergeMode.LwwRegister },
            },
        };
        await harness.UpgradeAsync(upgraded, AppRegistryTestData.Acme, bindings: Array.Empty<AppRoleBinding>());
        authority.FailEnable = _ =>
        {
            harness.Status.FailWrites = true;
            return null;
        };
        harness.Trees.FailEnsure = _ => new IOException("interrupted after replication");
        Assert.That((await harness.RunAsync(AppActivationOperation.Reconcile, AppRegistryTestData.Acme)).Failure,
            Is.EqualTo(AppActivationFailure.TreeProvisioningFailed));
        var pending = await harness.Status.GetAsync(AppRegistryTestData.Acme, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(pending!.ReplicationTrees[legacy], Is.False);
        Assert.That(pending.ReplicationTrees[owned], Is.True);
        Assert.That(pending.LastOutcome.Diagnostics.Single().Code, Is.EqualTo("replication-pending"));

        using var services = new ServiceCollection().AddSerializer(builder =>
            builder.AddAssembly(typeof(AppActivationStatus).Assembly)).BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var restoredStatus = serializer.Deserialize<AppActivationStatus>(serializer.SerializeToArray(pending));
        var restartedAuthority = new RecordingReplicationAuthority();
        foreach (var tree in authority.Trees)
            restartedAuthority.Trees.Add(tree.Key, tree.Value);
        var restarted = new ActivationHarness(replication: restartedAuthority);
        await restarted.InstallAsync(upgraded, AppRegistryTestData.Acme, bindings: Array.Empty<AppRoleBinding>());
        await restarted.Status.SetAsync(restoredStatus, CancellationToken.None);
        if (reconcileFirst)
        {
            await restarted.Registry.EnableAsync(AppRegistryTestData.Acme, ActivationHarness.Slug);
            Assert.That((await restarted.RunAsync(AppActivationOperation.Reconcile, AppRegistryTestData.Acme)).Succeeded, Is.True);
        }

        Assert.That((await restarted.RunAsync(AppActivationOperation.Uninstall, AppRegistryTestData.Acme)).Succeeded, Is.True);
        Assert.That(restartedAuthority.Disables, Is.EqualTo(new[] { owned }));
        Assert.That(restartedAuthority.Trees[legacy].Enabled, Is.True);
        Assert.That(restartedAuthority.Trees[owned].Enabled, Is.False);
    }
}
