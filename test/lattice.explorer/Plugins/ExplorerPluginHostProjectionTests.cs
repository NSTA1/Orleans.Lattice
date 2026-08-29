using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginHostProjectionTests
{
    [Test]
    public void Selection_carries_id_label_and_kind()
    {
        var selection = new ExplorerPluginSelection
        {
            Id = "view-orders",
            Label = "orders",
            Kind = ExplorerPluginSelectionKind.View,
        };

        Assert.Multiple(() =>
        {
            Assert.That(selection.Id, Is.EqualTo("view-orders"));
            Assert.That(selection.Label, Is.EqualTo("orders"));
            Assert.That(selection.Kind, Is.EqualTo(ExplorerPluginSelectionKind.View));
        });
    }

    [Test]
    public void Selection_rejects_a_null_or_blank_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ExplorerPluginSelection
                {
                    Id = null!,
                    Label = "x",
                    Kind = ExplorerPluginSelectionKind.Tree,
                },
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginSelection
                {
                    Id = "  ",
                    Label = "x",
                    Kind = ExplorerPluginSelectionKind.Tree,
                },
                Throws.ArgumentException);
        });
    }

    [Test]
    public void Selection_rejects_a_null_or_blank_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ExplorerPluginSelection
                {
                    Id = "x",
                    Label = null!,
                    Kind = ExplorerPluginSelectionKind.Tree,
                },
                Throws.ArgumentNullException);
            Assert.That(
                () => new ExplorerPluginSelection
                {
                    Id = "x",
                    Label = "",
                    Kind = ExplorerPluginSelectionKind.Tree,
                },
                Throws.ArgumentException);
        });
    }

    [Test]
    public void Selections_with_the_same_values_are_equal()
    {
        var left = new ExplorerPluginSelection
        {
            Id = "a",
            Label = "A",
            Kind = ExplorerPluginSelectionKind.TagIndex,
        };

        Assert.That(left, Is.EqualTo(left with { }));
    }

    [Test]
    public void Selection_kinds_cover_the_catalog()
    {
        Assert.That(
            Enum.GetValues<ExplorerPluginSelectionKind>(),
            Is.EquivalentTo(new[]
            {
                ExplorerPluginSelectionKind.Tree,
                ExplorerPluginSelectionKind.View,
                ExplorerPluginSelectionKind.TagIndex,
            }));
    }

    [Test]
    public void Default_connection_status_is_disconnected_and_unusable()
    {
        ExplorerPluginConnectionStatus status = default;

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(ExplorerPluginConnectionStatus.Disconnected));
            Assert.That(status.State, Is.EqualTo(ExplorerPluginConnectionState.Disconnected));
            Assert.That(status.IsUsable, Is.False);
            Assert.That(status.IsDisconnected, Is.True);
            Assert.That(status.RequiresAuthentication, Is.False);
        });
    }

    [TestCase(ExplorerPluginConnectionState.Disconnected, false, true)]
    [TestCase(ExplorerPluginConnectionState.Connecting, false, false)]
    [TestCase(ExplorerPluginConnectionState.Connected, true, false)]
    [TestCase(ExplorerPluginConnectionState.Reconnecting, true, false)]
    [TestCase(ExplorerPluginConnectionState.Faulted, false, true)]
    public void Connection_status_classifies_each_state(
        ExplorerPluginConnectionState state,
        bool usable,
        bool disconnected)
    {
        var status = new ExplorerPluginConnectionStatus(state);

        Assert.Multiple(() =>
        {
            Assert.That(status.IsUsable, Is.EqualTo(usable));
            Assert.That(status.IsDisconnected, Is.EqualTo(disconnected));
        });
    }

    [Test]
    public void Connection_status_carries_the_authentication_hint()
    {
        var status = new ExplorerPluginConnectionStatus(
            ExplorerPluginConnectionState.Faulted,
            RequiresAuthentication: true);

        Assert.Multiple(() =>
        {
            Assert.That(status.RequiresAuthentication, Is.True);
            Assert.That(
                status,
                Is.EqualTo(new ExplorerPluginConnectionStatus(ExplorerPluginConnectionState.Faulted, true)));
        });
    }

    [Test]
    public void Default_tenant_scope_is_the_inactive_fail_closed_scope()
    {
        ExplorerPluginTenantScope scope = default;

        Assert.Multiple(() =>
        {
            Assert.That(scope, Is.EqualTo(ExplorerPluginTenantScope.Inactive));
            Assert.That(scope.IsActive, Is.False);
            Assert.That(scope.ActiveTenantId, Is.Null);
            Assert.That(scope.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.ActiveTenant));
        });
    }

    [Test]
    public void Tenant_scope_carries_the_active_tenant_and_resolved_visibility()
    {
        var scope = new ExplorerPluginTenantScope(
            IsActive: true,
            ActiveTenantId: "acme",
            ExplorerPluginTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsActive, Is.True);
            Assert.That(scope.ActiveTenantId, Is.EqualTo("acme"));
            Assert.That(scope.Visibility, Is.EqualTo(ExplorerPluginTenantVisibility.AllTenants));
        });
    }

    [Test]
    public void Active_tenant_is_the_zero_visibility_so_an_unset_scope_fails_closed()
    {
        Assert.That((int)ExplorerPluginTenantVisibility.ActiveTenant, Is.Zero);
    }

    [Test]
    public void Host_change_covers_every_ambient_fact()
    {
        Assert.That(
            Enum.GetValues<ExplorerPluginHostChange>(),
            Is.EquivalentTo(new[]
            {
                ExplorerPluginHostChange.Selection,
                ExplorerPluginHostChange.Connection,
                ExplorerPluginHostChange.Tenant,
            }));
    }
}
