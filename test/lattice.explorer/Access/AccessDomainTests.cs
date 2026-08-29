using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// The Access plugin's controlled domain contract - the single seam its views
/// are handed - and the internal sub-surface list the panel's tab strip renders.
/// </summary>
[TestFixture]
public sealed class AccessDomainTests
{
    [Test]
    public void The_domain_rejects_every_null_dependency()
    {
        var membership = Substitute.For<IMembershipAdminService>();
        var policy = Substitute.For<IPolicyAdminService>();
        var catalog = Substitute.For<ICatalogReader>();
        var gate = Substitute.For<IAuthAdminCapabilityService>();
        ISubjectSearchDebounce Debounce() => Substitute.For<ISubjectSearchDebounce>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new AccessDomain(null!, policy, catalog, gate, Debounce), Throws.ArgumentNullException);
            Assert.That(() => new AccessDomain(membership, null!, catalog, gate, Debounce), Throws.ArgumentNullException);
            Assert.That(() => new AccessDomain(membership, policy, null!, gate, Debounce), Throws.ArgumentNullException);
            Assert.That(() => new AccessDomain(membership, policy, catalog, null!, Debounce), Throws.ArgumentNullException);
            Assert.That(() => new AccessDomain(membership, policy, catalog, gate, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_domain_projects_its_services_and_the_gates_authentication_mode()
    {
        var membership = Substitute.For<IMembershipAdminService>();
        var policy = Substitute.For<IPolicyAdminService>();
        var catalog = Substitute.For<ICatalogReader>();
        var gate = Substitute.For<IAuthAdminCapabilityService>();
        gate.AuthenticationMode.Returns(ExplorerAccessAuthenticationMode.Basic);

        var domain = new AccessDomain(
            membership, policy, catalog, gate, () => Substitute.For<ISubjectSearchDebounce>());

        Assert.Multiple(() =>
        {
            Assert.That(domain.Membership, Is.SameAs(membership));
            Assert.That(domain.Policy, Is.SameAs(policy));
            Assert.That(domain.Catalog, Is.SameAs(catalog));
            Assert.That(
                domain.AuthenticationMode,
                Is.EqualTo(ExplorerAccessAuthenticationMode.Basic),
                "the mode is the plugin's own advisory state, read live from its gate");
        });
    }

    [Test]
    public void Every_created_picker_gets_its_own_model_over_its_own_debounce()
    {
        var debounces = 0;
        var domain = new AccessDomain(
            Substitute.For<IMembershipAdminService>(),
            Substitute.For<IPolicyAdminService>(),
            Substitute.For<ICatalogReader>(),
            Substitute.For<IAuthAdminCapabilityService>(),
            () =>
            {
                debounces++;
                return Substitute.For<ISubjectSearchDebounce>();
            });

        var first = domain.CreateSubjectPicker();
        var second = domain.CreateSubjectPicker();

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.SameAs(second));
            Assert.That(
                debounces,
                Is.EqualTo(2),
                "the injectable search-timing seam stays one-per-picker after the move onto the domain");
        });
    }

    [Test]
    public void Every_created_label_resolver_is_scoped_to_the_view_that_asked_for_it()
    {
        var domain = new AccessDomain(
            Substitute.For<IMembershipAdminService>(),
            Substitute.For<IPolicyAdminService>(),
            Substitute.For<ICatalogReader>(),
            Substitute.For<IAuthAdminCapabilityService>(),
            () => Substitute.For<ISubjectSearchDebounce>());

        Assert.That(domain.CreateLabelResolver(), Is.Not.SameAs(domain.CreateLabelResolver()));
    }
}

/// <summary>
/// The plugin's three internal sub-surfaces. They are rendered by the design
/// system's single tab primitive over the shared <see cref="LatticeTabItem"/>
/// vocabulary, which is what stops the retired <c>AccessTab</c> enum leaving a
/// third parallel tab registry behind.
/// </summary>
[TestFixture]
public sealed class AccessSurfacesTests
{
    [Test]
    public void The_sub_surfaces_are_declared_as_design_system_tab_items()
    {
        Assert.That(AccessSurfaces.Tabs, Is.All.InstanceOf<LatticeTabItem>());
    }

    [Test]
    public void The_sub_surfaces_are_groups_then_policies_then_explain()
    {
        Assert.That(
            AccessSurfaces.Tabs.Select(tab => tab.Id),
            Is.EqualTo(new[] { AccessSurfaces.Groups, AccessSurfaces.Policies, AccessSurfaces.Explain }));
    }

    [Test]
    public void Every_sub_surface_is_enabled_labelled_and_described()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in AccessSurfaces.Tabs)
            {
                Assert.That(tab.IsEnabled, Is.True);
                Assert.That(tab.Label, Is.Not.Empty);
                Assert.That(tab.Description, Is.Not.Null.And.Not.Empty, "the strip surfaces it as a tooltip");
            }
        });
    }

    [Test]
    public void The_tab_list_is_a_single_cached_instance()
    {
        Assert.That(
            AccessSurfaces.Tabs,
            Is.SameAs(AccessSurfaces.Tabs),
            "the strip re-renders on every state change, so the list must not be rebuilt per render");
    }

    [Test]
    public void No_sub_surface_is_registered_as_a_plugin_id()
    {
        Assert.That(
            AccessSurfaces.Tabs.Select(tab => tab.Id),
            Has.None.StartsWith("orleans.lattice."),
            "sub-surfaces are internal to the plugin and are not entries in the shell's area strip");
    }
}
