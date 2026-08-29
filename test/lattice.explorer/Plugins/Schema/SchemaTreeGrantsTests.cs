using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The Schema plugin's scope vocabulary and the read side of the scoped access
/// decisions it files: the per-tree, per-action grey-out expressed through the
/// keyed store rather than through a shared capability record.
/// </summary>
[TestFixture]
public sealed class SchemaTreeGrantsTests
{
    [Test]
    public void Every_capability_has_a_stable_distinct_action_name()
    {
        var names = Enum.GetValues<SchemaCapability>()
            .Select(SchemaPluginScopes.Action)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(names, Has.All.Not.Empty);
            Assert.That(names.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(names.Length));
        });
    }

    [Test]
    public void An_undeclared_capability_has_no_action_name()
    {
        Assert.That(
            () => SchemaPluginScopes.Action((SchemaCapability)999),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void A_scope_names_the_tree_then_the_action()
    {
        Assert.That(
            SchemaPluginScopes.For("orders", SchemaCapability.ManagePolicy),
            Is.EqualTo("orders/policy.manage"));
    }

    [Test]
    public void A_scope_rejects_a_null_tree()
    {
        Assert.That(
            () => SchemaPluginScopes.For(null!, SchemaCapability.ViewPolicy),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_scoped_key_is_filed_under_the_schema_plugin_id()
    {
        var key = SchemaTreeGrants.KeyFor("orders", SchemaCapability.ViewDeadLetters);

        Assert.Multiple(() =>
        {
            Assert.That(key.PluginId, Is.EqualTo(SchemaPluginKeys.PluginId));
            Assert.That(key.Scope, Is.EqualTo("orders/deadletters.view"));
        });
    }

    [Test]
    public void A_scoped_key_rejects_a_null_tree()
    {
        Assert.That(
            () => SchemaTreeGrants.KeyFor(null!, SchemaCapability.ViewPolicy),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_none_grants_deny_every_capability_and_name_no_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaTreeGrants.None.TreeId, Is.Null);
            foreach (var capability in SchemaTreeGrants.Capabilities)
            {
                Assert.That(
                    SchemaTreeGrants.None.IsAllowed(capability),
                    Is.False,
                    $"{capability} must be denied before a tree has been probed");
            }
        });
    }

    [Test]
    public void The_capability_list_covers_every_declared_member()
    {
        Assert.That(SchemaTreeGrants.Capabilities, Is.EquivalentTo(Enum.GetValues<SchemaCapability>()));
    }

    [Test]
    public void An_unprobed_scope_reads_denied()
    {
        var store = new ExplorerPluginAccessStore();
        var grants = SchemaTreeGrants.For(store, "orders");

        Assert.Multiple(() =>
        {
            Assert.That(grants.TreeId, Is.EqualTo("orders"));
            foreach (var capability in SchemaTreeGrants.Capabilities)
            {
                Assert.That(grants.IsAllowed(capability), Is.False);
            }
        });
    }

    [Test]
    public void A_scoped_decision_does_not_inherit_the_plugin_level_admission()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(SchemaPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

        var grants = SchemaTreeGrants.For(store, "orders");

        Assert.That(
            grants.IsAllowed(SchemaCapability.ManagePolicy),
            Is.False,
            "a coarse endpoint-reachable admission must never open a per-action control");
    }

    [Test]
    public void A_filed_scoped_decision_is_read_back_for_that_action_only()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(SchemaTreeGrants.KeyFor("orders", SchemaCapability.ViewPolicy), ExplorerPluginAccess.Allowed);

        var grants = SchemaTreeGrants.For(store, "orders");

        Assert.Multiple(() =>
        {
            Assert.That(grants.IsAllowed(SchemaCapability.ViewPolicy), Is.True);
            Assert.That(grants.IsAllowed(SchemaCapability.ManagePolicy), Is.False);
        });
    }

    [Test]
    public void One_trees_decision_never_answers_for_another()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(SchemaTreeGrants.KeyFor("orders", SchemaCapability.ManagePolicy), ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaTreeGrants.For(store, "orders").IsAllowed(SchemaCapability.ManagePolicy), Is.True);
            Assert.That(SchemaTreeGrants.For(store, "invoices").IsAllowed(SchemaCapability.ManagePolicy), Is.False);
        });
    }

    [Test]
    public void Grants_read_the_store_live_rather_than_a_snapshot()
    {
        var store = new ExplorerPluginAccessStore();
        var grants = SchemaTreeGrants.For(store, "orders");

        store.Set(SchemaTreeGrants.KeyFor("orders", SchemaCapability.ScanCompliance), ExplorerPluginAccess.Allowed);
        var opened = grants.IsAllowed(SchemaCapability.ScanCompliance);

        store.Set(SchemaTreeGrants.KeyFor("orders", SchemaCapability.ScanCompliance), ExplorerPluginAccess.Denied);
        var revoked = grants.IsAllowed(SchemaCapability.ScanCompliance);

        Assert.Multiple(() =>
        {
            Assert.That(opened, Is.True);
            Assert.That(revoked, Is.False, "a re-probe that shrinks a grant must revoke the control it opened");
        });
    }

    [Test]
    public void An_undeclared_capability_reads_denied_rather_than_throwing()
    {
        var grants = SchemaTreeGrants.For(new ExplorerPluginAccessStore(), "orders");

        Assert.That(grants.IsAllowed((SchemaCapability)999), Is.False);
    }

    [Test]
    public void Grants_reject_a_null_store_or_an_empty_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SchemaTreeGrants.For(null!, "orders"), Throws.ArgumentNullException);
            Assert.That(
                () => SchemaTreeGrants.For(new ExplorerPluginAccessStore(), string.Empty),
                Throws.ArgumentException);
        });
    }
}
