using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Issue #4164: which groups an app role may be bound to in the installing tenant -
/// the cluster's rule (a cluster group, or one of the tenant's own groups), applied
/// before anything is sent - and where the caller joins a bound group.
/// </summary>
[TestFixture]
public sealed class AppBindingGroupsTests
{
    [TestCase("acme", "operators", true)]
    [TestCase(null, "operators", true)]
    [TestCase("acme", "t/acme/ops", true)]
    [TestCase("acme", "t/globex/ops", false)]
    [TestCase(null, "t/acme/ops", false)]
    [TestCase("default", "t/default/ops", false)]
    [TestCase("acme", "t/acme/Not Valid", false)]
    [TestCase("acme", "t/acme", false)]
    public void A_group_is_bindable_only_when_it_is_a_cluster_group_or_the_installing_tenants_own(string? tenant, string group, bool expected)
    {
        Assert.That(AppBindingGroups.IsBindable(tenant, group), Is.EqualTo(expected));
    }

    [Test]
    public void A_tenant_group_is_joined_on_its_tenants_group_page_and_any_other_on_the_clusters()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppBindingGroups.JoinAddress("t/acme/ops").ToHref(), Is.EqualTo("t/acme/access/groups/ops"));
            Assert.That(AppBindingGroups.JoinAddress("operators").ToHref(), Is.EqualTo("access/groups/operators"));
            Assert.That(() => AppBindingGroups.IsBindable("acme", null!), Throws.ArgumentNullException);
            Assert.That(() => AppBindingGroups.JoinAddress(string.Empty), Throws.ArgumentException);
        });
    }
}
