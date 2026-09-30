using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The <see cref="AppActivationEngine"/> arms that only a failing collaborator can
/// reach: the top-level fault handler, the registry-transition rollback, the
/// lifecycle rejections on disable and uninstall, the rule-withdrawal failures, the
/// source-identity re-check, and the dropped-tree retirement fault.
/// </summary>
/// <remarks>
/// <para>
/// These are the arms that decide what an operator sees when something underneath
/// the engine breaks, and they are exactly the ones a happy-path suite cannot
/// reach. The engine's contract is that a failure is always a recorded outcome
/// rather than an escaping exception, and that a partially-applied activation is
/// wound back rather than left live - so an untested arm here is a silent claim
/// that a fault is handled, on the path where the handling matters most.
/// </para>
/// <para>
/// Each fault is injected at a single collaborator and paired with the same
/// operation succeeding without it, so a test cannot pass because the operation
/// failed earlier for an unrelated reason.
/// </para>
/// </remarks>
public sealed partial class AppActivationEngineTests
{
    private static readonly AppSlug OtherSlug = AppSlug.Parse("other");

    /// <summary>A null-safe rendering of an outcome's diagnostics, for assertion messages.</summary>
    private static string Describe(AppActivationOutcome outcome) =>
        outcome.Diagnostics.Count == 0
            ? $"{outcome.Failure} (no diagnostics)"
            : string.Join("; ", outcome.Diagnostics.Select(d => d.Message));

    // ----- the top-level fault handler, and the enable rollback that reaches it -----

    [Test]
    public async Task Enable_whose_registry_transition_throws_withdraws_the_rules_and_records_a_faulted_outcome()
    {
        // The transition's outcome is unknown when it throws, so the engine must not
        // leave the compiled rules live behind a record that may never have become
        // enabled: an app whose grants outlive its enablement is a standing
        // authorization surface no lifecycle operation can see.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());

        var key = AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug);
        harness.RegistryStore.BeforeSet = k =>
        {
            if (string.Equals(k, key, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("registry store unavailable");
            }
        };

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Succeeded, Is.False);
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.Faulted),
                "an escaping collaborator fault must surface as a recorded outcome, never as an exception");
            Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("registry store unavailable"),
                "the diagnostic must name the underlying fault so an operator can act on it");
            Assert.That(harness.OwnedRuleIds(), Is.Empty,
                "rules compiled for an enable whose transition may not have landed must be withdrawn");
        });

        // Positive control: without the injected fault the same enable succeeds and
        // does leave rules behind, so the assertions above are about the rollback.
        var ok = new ActivationHarness();
        await ok.InstallAsync(ActivationHarness.Manifest());
        var okOutcome = await ok.RunAsync(AppActivationOperation.Enable);
        Assert.Multiple(() =>
        {
            Assert.That(okOutcome.Succeeded, Is.True, Describe(okOutcome));
            Assert.That(ok.OwnedRuleIds(), Is.Not.Empty);
        });
    }

    [Test]
    public async Task A_faulted_run_still_records_status_when_the_status_was_readable()
    {
        // The status write is what makes a fault observable after the call returns.
        // It is skipped only when the status could not be READ, because overwriting
        // an unreadable status would forget which trees were provisioned.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.RegistryStore.BeforeSet = _ => throw new InvalidOperationException("registry store unavailable");

        await harness.RunAsync(AppActivationOperation.Enable);

        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status, Is.Not.Null);
        Assert.That(status!.LastOutcome.Failure, Is.EqualTo(AppActivationFailure.Faulted));
    }

    // ----- lifecycle rejections -----

    [Test]
    public async Task Disable_of_an_app_that_was_never_installed_is_rejected_by_the_lifecycle()
    {
        var harness = new ActivationHarness();

        var outcome = await harness.RunAsync(AppActivationOperation.Disable);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.NotInstalled));
            Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("registry"));
        });
    }

    [Test]
    public async Task Uninstall_of_an_app_that_was_never_installed_is_rejected_by_the_lifecycle()
    {
        var harness = new ActivationHarness();

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.NotInstalled));
            Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("registry"));
        });
    }

    [Test]
    public async Task Reconcile_of_an_enabled_app_whose_ceiling_is_not_pinned_refuses_to_reactivate()
    {
        // A ceiling consented for a different version is not consent for the stored
        // one. Reconcile runs unattended at startup, so re-activating on it would
        // silently re-grant a role set nobody approved for the version now installed.
        var harness = new ActivationHarness();
        harness.Source.Publish(ActivationHarness.Manifest());
        harness.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug),
            AppRegistryTestData.Record(
                AppRegistryLifecycleState.Enabled,
                version: ActivationHarness.V2,
                ceilingVersion: ActivationHarness.V1));

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingNotPinned));
            Assert.That(outcome.Diagnostics.Single().Message, Does.Contain("re-consent"));
            Assert.That(harness.OwnedRuleIds(), Is.Empty,
                "an unpinned ceiling must not produce grants");
        });

        // Positive control: the same seed with the ceiling pinned does reactivate.
        var ok = new ActivationHarness();
        ok.Source.Publish(ActivationHarness.Manifest(version: ActivationHarness.V2));
        ok.RegistryStore.Seed(
            AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug),
            AppRegistryTestData.Record(
                AppRegistryLifecycleState.Enabled,
                version: ActivationHarness.V2,
                ceilingVersion: ActivationHarness.V2));
        var okOutcome = await ok.RunAsync(AppActivationOperation.Reconcile);
        Assert.That(okOutcome.Failure, Is.Not.EqualTo(AppActivationFailure.CeilingNotPinned));
    }

    // ----- rule-withdrawal failures -----

    [Test]
    public async Task Disable_whose_rule_withdrawal_fails_reports_the_failure_and_stays_enabled()
    {
        // Withdrawal precedes the registry transition precisely so a store fault
        // cannot leave a disabled app's grants live. The transition must therefore
        // not run when the withdrawal failed.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        harness.Rules.FailList = new InvalidOperationException("policy store unavailable");

        var outcome = await harness.RunAsync(AppActivationOperation.Disable);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Succeeded, Is.False);
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.RulePersistenceFailed));
        });

        var record = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);
        Assert.That(record!.State, Is.EqualTo(AppRegistryLifecycleState.Enabled),
            "the app must stay enabled while its grants are still live");
    }

    [Test]
    public async Task Uninstall_whose_rule_withdrawal_fails_reports_the_failure_and_retires_nothing()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);
        harness.Trees.SoftDeleted.Clear();
        harness.Rules.FailList = new InvalidOperationException("policy store unavailable");

        var outcome = await harness.RunAsync(AppActivationOperation.Uninstall);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.RulePersistenceFailed));
            Assert.That(harness.Trees.SoftDeleted, Is.Empty,
                "tree retirement must not proceed past a failed withdrawal");
        });
    }

    [Test]
    public async Task A_failed_activation_whose_withdrawal_also_fails_reports_both_diagnostics()
    {
        // Failing closed is itself fallible. When the withdrawal that follows a
        // failed activation also fails, the operator needs both facts: the app did
        // not activate, AND its previous grants may still be live.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        await harness.RunAsync(AppActivationOperation.Enable);

        // A manifest that exceeds the consented ceiling fails activation with a
        // failure the fail-closed path does NOT short-circuit on, so the withdrawal
        // runs - and is made to fail.
        harness.Source.Publish(ActivationHarness.Manifest(
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read | LatticeOperation.Delete, "records") }));
        harness.Rules.FailList = new InvalidOperationException("policy store unavailable");

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded),
                "the original failure is what the caller asked about and must not be replaced");
            Assert.That(outcome.Diagnostics.Count, Is.GreaterThan(1),
                "the withdrawal failure must be appended, not swallowed");
            Assert.That(
                outcome.Diagnostics.Select(d => d.Message),
                Has.Some.Contains("policy store unavailable"));
        });
    }

    // ----- the source-identity re-check -----

    [Test]
    public async Task Activation_refuses_a_manifest_whose_identity_is_not_the_installed_one()
    {
        // The registry records what was consented; the source is a separate system
        // that can be republished under an operator's control. Activating whatever
        // it returns would let a source swap the app behind a consented ceiling,
        // so the identity is re-checked against the record rather than trusted.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Source.SkipVersionCheck = true;
        harness.Source.PublishAs(
            ActivationHarness.Slug,
            ActivationAppSource.ResolvedResult(ActivationHarness.Manifest(slug: OtherSlug)));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.VersionMismatch));
            Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("version-mismatch"));
            Assert.That(outcome.Diagnostics.Single().Path, Is.EqualTo("$.identity"));
            Assert.That(outcome.Diagnostics.Single().Message, Does.Contain(OtherSlug.Value));
            Assert.That(harness.OwnedRuleIds(), Is.Empty);
        });
    }

    [Test]
    public async Task Activation_refuses_a_manifest_whose_version_is_not_the_installed_one()
    {
        // The sibling half of the same re-check: right app, wrong version.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        harness.Source.SkipVersionCheck = true;
        harness.Source.PublishAs(
            ActivationHarness.Slug,
            ActivationAppSource.ResolvedResult(ActivationHarness.Manifest(version: ActivationHarness.V2)));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.VersionMismatch));
        Assert.That(outcome.Diagnostics.Single().Message, Does.Contain(ActivationHarness.V2.ToString()));
    }

    // ----- retiring a dropped tree -----

    [Test]
    public async Task An_upgrade_whose_dropped_tree_cannot_be_retired_fails_with_a_tree_diagnostic()
    {
        // A dropped structural tree is soft-deleted during activation. When that
        // fails the app must not be reported as activated on the new manifest: the
        // old tree is still live and still holds the app's data.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("archive") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records") }));
        await harness.RunAsync(AppActivationOperation.Enable);

        var droppedTreeId = AppActivationTreeNames.StructuralTree(TenantId.Default, ActivationHarness.Slug, "archive");
        await harness.UpgradeAsync(ActivationHarness.Manifest(
            version: ActivationHarness.V2,
            trees: new[] { ActivationHarness.Tree("records") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records") }));
        harness.Trees.FailDelete = id =>
            string.Equals(id, droppedTreeId, StringComparison.Ordinal)
                ? new InvalidOperationException("tree service unavailable")
                : null;

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.TreeProvisioningFailed));
            Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("tree-delete"));
            Assert.That(outcome.Diagnostics.Single().Path, Is.EqualTo("$.trees[archive]"));
            Assert.That(outcome.Diagnostics.Single().Message, Does.Contain(droppedTreeId));
        });

        // The new manifest is still recorded as applied: everything except the
        // retirement did land, and the applied manifest is the only record of which
        // trees were provisioned. The orphan is left to the soft-delete sweeper,
        // which is why a retry does not re-attempt it.
        var status = await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        Assert.That(status!.AppliedManifest!.Identity.Version, Is.EqualTo(ActivationHarness.V2));
        Assert.That(harness.Trees.SoftDeleted, Does.Not.Contain(droppedTreeId));

        // Positive control on a fresh harness: the same upgrade without the injected
        // delete fault does retire the dropped tree, so the failure above is the
        // delete itself and not the shape of the upgrade.
        var ok = new ActivationHarness();
        await ok.InstallAsync(ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("archive") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records") }));
        await ok.RunAsync(AppActivationOperation.Enable);
        await ok.UpgradeAsync(ActivationHarness.Manifest(
            version: ActivationHarness.V2,
            trees: new[] { ActivationHarness.Tree("records") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records") }));

        var okOutcome = await ok.RunAsync(AppActivationOperation.Reconcile);

        Assert.Multiple(() =>
        {
            Assert.That(okOutcome.Failure, Is.EqualTo(AppActivationFailure.None), Describe(okOutcome));
            Assert.That(ok.Trees.SoftDeleted, Does.Contain(droppedTreeId));
        });
    }
}
