using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Re-binding an enabled app's roles (issue #3884): the reconcile that follows replaces the
/// app's compiled rules, so a moved or removed binding leaves no grant behind.
/// </summary>
public sealed partial class AppActivationEngineTests
{
    [Test]
    public async Task Reconcile_after_a_rebinding_replaces_the_rules_and_keeps_none_for_a_removed_binding()
    {
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(roles:
        [
            ActivationHarness.Role("reader", LatticeOperation.Read, "records"),
            ActivationHarness.Role("writer", LatticeOperation.Write, "records"),
        ]);
        await harness.InstallAsync(manifest, bindings: [AppRoleBinding.Create("reader", "old-readers"), AppRoleBinding.Create("writer", "writers")]);
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Succeeded, Is.True);
        var enabled = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);

        var rebound = await harness.Registry.UpgradeAsync(new AppRegistryInstallRequest
        {
            Identity = new AppIdentity { Slug = enabled!.Slug, Version = enabled.Version, Provenance = enabled.Provenance },
            Ceiling = enabled.Ceiling,
            RoleBindings = [AppRoleBinding.Create("reader", "new-readers")],
            ExpectedVersion = enabled.Version,
            ExpectedRevision = enabled.Revision,
        });
        Assert.That(rebound.Succeeded, Is.True, rebound.Message);

        var outcome = await harness.RunAsync(AppActivationOperation.Reconcile);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
            Assert.That(outcome.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
            Assert.That(harness.Rules.Rules.Select(r => r.Subject), Is.EqualTo(new[] { LatticeSubjectSelector.Group("new-readers") }));
            Assert.That(harness.Rules.Rules.Single().Operations, Is.EqualTo(LatticeOperation.Read));
        });
    }
}
