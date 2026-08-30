using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Components;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The plugin descriptor the shell enumerates, and the compile-time statement of
/// what the whole surface may reach.
/// </summary>
[TestFixture]
public sealed class MyTenantAreaPluginTests
{
    private sealed class StubGate : IMyTenantAccessGate
    {
        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default) =>
            new(ExplorerPluginAccess.Allowed);
    }

    private static MyTenantAreaPlugin CreatePlugin() => new(new StubGate());

    [Test]
    public void The_descriptor_keys_the_plugin_on_its_own_id()
    {
        var descriptor = CreatePlugin().Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.PluginId, Is.EqualTo(MyTenantPluginKeys.PluginId));
            Assert.That(descriptor.Label, Is.EqualTo("My Tenant"));
            Assert.That(descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
        });
    }

    [Test]
    public void The_plugin_id_is_dotted_and_package_owned_so_it_cannot_collide() =>
        Assert.That(MyTenantPluginKeys.PluginId, Is.EqualTo("orleans.lattice.mytenant"));

    [Test]
    public void The_operator_gate_diagnostic_has_its_own_scope_so_it_gates_nothing() =>
        Assert.That(MyTenantPluginKeys.OperatorGateScope, Is.Not.Empty);

    [Test]
    public void The_declared_domain_contract_is_the_tenancy_seam_and_nothing_wider() =>
        // The whole reach of the surface, stated once in the plugin's signature
        // (epic decision D3). TenancyDomainSurfaceTests separately guards that
        // no wire type is reachable from it.
        Assert.That(((IExplorerPlugin)CreatePlugin()).DomainContract, Is.EqualTo(typeof(ITenancyDomain)));

    [Test]
    public void The_view_is_the_plugins_own_panel() =>
        Assert.That(CreatePlugin().ViewType, Is.EqualTo(typeof(MyTenantPanel)));

    [Test]
    public void The_gate_is_the_one_the_plugin_was_constructed_with()
    {
        var gate = new StubGate();

        Assert.That(new MyTenantAreaPlugin(gate).AccessGate, Is.SameAs(gate));
    }

    [Test]
    public void A_null_gate_is_rejected() =>
        Assert.That(() => new MyTenantAreaPlugin(null!), Throws.InstanceOf<ArgumentNullException>());

    [Test]
    public void The_descriptor_is_a_cached_instance_so_enumeration_allocates_nothing()
    {
        var plugin = CreatePlugin();

        Assert.That(plugin.Descriptor, Is.SameAs(plugin.Descriptor));
    }
}
