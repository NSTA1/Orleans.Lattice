namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppRoleGrantEvaluator"/> binds nothing through a stored binding that names a group outside the
/// install's tenant, so such a binding never confers an app role (fail closed).
/// </summary>
[TestFixture]
public sealed class AppRoleGrantEvaluatorTenantConfinementTests
{
    [Test]
    public void CompileRoles_keeps_cluster_groups_and_the_install_tenants_groups_and_drops_every_other_tenant_group()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, tenant: AppRegistryTestData.Acme) with
        {
            RoleBindings =
            [
                AppRoleBinding.Create("reader", "readers"),
                AppRoleBinding.Create("reader", "t/acme/readers"),
                AppRoleBinding.Create("reader", "t/globex/readers"),
                AppRoleBinding.Create("reader", "t/default/readers"),
                AppRoleBinding.Create("reader", "t/acme/"),
            ],
        };

        var roles = AppRoleGrantEvaluator.CompileRoles(record, ActivationHarness.Manifest());

        Assert.That(roles.Single().GroupIds, Is.EqualTo(new[] { "readers", "t/acme/readers" }));
    }

    [Test]
    public void CompileRoles_confers_no_grant_when_the_only_binding_is_another_tenants_group()
    {
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, tenant: AppRegistryTestData.Acme) with
        {
            RoleBindings = [AppRoleBinding.Create("reader", "t/globex/readers")],
        };

        var role = AppRoleGrantEvaluator.CompileRoles(record, ActivationHarness.Manifest()).Single();

        Assert.That(role.GroupIds, Is.Empty);
        Assert.That(role.ConfersGrant, Is.False);
    }
}
