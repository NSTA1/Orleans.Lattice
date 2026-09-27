using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppActivationEngine"/> consent integrity: the rules left live after a run are
/// always compiled from the consent the registry holds once the run's own transition lands,
/// and a partial rule write never keeps a grant the current consent has withdrawn.
/// </summary>
public sealed partial class AppActivationEngineTests
{
    private static AppManifest AdoptingManifest() => ActivationHarness.Manifest(
        trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("legacy", adopted: "legacy-tree") },
        roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "records", "legacy") });

    private static AppCapabilityCeiling LegacyApproved() =>
        AppCapabilityCeiling.Structural(LatticeOperation.Read) with
        {
            ApprovedExceptionScopes = new[] { LatticeScope.Tree("legacy-tree") },
        };

    /// <summary>Runs <paramref name="interleave"/> once, the first time activation provisions a tree.</summary>
    private static void InterleaveOnFirstProvision(ActivationHarness harness, Func<Task> interleave)
    {
        var fired = false;
        harness.Trees.FailEnsure = _ =>
        {
            if (!fired)
            {
                fired = true;
                interleave().GetAwaiter().GetResult();
            }

            return null;
        };
    }

    [Test]
    public async Task Enable_does_not_leave_rules_from_a_consent_revoked_while_it_ran()
    {
        var harness = new ActivationHarness();
        var manifest = AdoptingManifest();
        await harness.InstallAsync(manifest, ceiling: LegacyApproved());

        // An operator narrows the consent (withdrawing the legacy-tree exception) after the
        // enable run read the record but before its registry transition lands.
        InterleaveOnFirstProvision(harness, () => harness.UpgradeAsync(manifest, ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read)));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(harness.Rules.Rules.Where(r => r.Scope.TreeId == "legacy-tree"), Is.Empty,
            "a grant the current consent does not approve must not stay live");
        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
    }

    [Test]
    public async Task Enable_applies_bindings_rebound_while_it_ran()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest();
        await harness.InstallAsync(manifest, bindings: new[] { AppRoleBinding.Create("reader", "old-group") });

        InterleaveOnFirstProvision(harness, () => harness.UpgradeAsync(manifest, bindings: new[] { AppRoleBinding.Create("reader", "new-group") }));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
        Assert.That(harness.Rules.Rules.Select(r => r.Subject), Is.EqualTo(new[] { LatticeSubjectSelector.Group("new-group") }));
    }

    [Test]
    public async Task Reconcile_withdraws_a_revoked_binding_even_when_writing_the_new_one_fails()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest();
        await harness.InstallAsync(manifest, bindings: new[] { AppRoleBinding.Create("reader", "revoked-group") });
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Succeeded, Is.True);

        await harness.UpgradeAsync(manifest, bindings: new[] { AppRoleBinding.Create("reader", "new-group") });
        harness.Rules.FailPut = _ => new TimeoutException("policy store unavailable");

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.RulePersistenceFailed));
        Assert.That(harness.Rules.Rules.Where(r => r.Subject == LatticeSubjectSelector.Group("revoked-group")), Is.Empty,
            "the revoked group must lose its grant even though the replacement could not be written");
    }
}
