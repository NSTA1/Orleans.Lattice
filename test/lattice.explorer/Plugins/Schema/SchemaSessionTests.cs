using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The Schema area's shared session: the one object the concern-scoped
/// components read and mutate, and the gating predicate every control's
/// disabled state is derived from.
/// </summary>
[TestFixture]
public sealed class SchemaSessionTests
{
    [Test]
    public void The_session_rejects_a_null_domain()
    {
        Assert.That(() => new SchemaSession(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void A_fresh_session_is_fail_closed_with_no_tree()
    {
        var session = new SchemaSession(new FakeSchemaPluginDomain());

        Assert.Multiple(() =>
        {
            Assert.That(session.IsAllowed, Is.False);
            Assert.That(session.TreeId, Is.Null);
            Assert.That(session.Grants, Is.SameAs(SchemaTreeGrants.None));
            Assert.That(session.IsBusy, Is.False);
            Assert.That(session.LastResult, Is.Null);
            Assert.That(session.HasProbedTree, Is.False);
            Assert.That(session.Can(SchemaCapability.ViewPolicy), Is.False);
        });
    }

    [Test]
    public void An_action_needs_the_gate_a_tree_a_scoped_grant_and_no_request_in_flight()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(SchemaTreeGrants.KeyFor("orders", SchemaCapability.ManagePolicy), ExplorerPluginAccess.Allowed);

        var session = new SchemaSession(new FakeSchemaPluginDomain())
        {
            IsAllowed = true,
            TreeId = "orders",
            Grants = SchemaTreeGrants.For(store, "orders"),
        };

        Assert.Multiple(() =>
        {
            Assert.That(session.Can(SchemaCapability.ManagePolicy), Is.True);
            Assert.That(session.HasProbedTree, Is.True);

            session.IsBusy = true;
            Assert.That(session.Can(SchemaCapability.ManagePolicy), Is.False, "a request in flight greys every control");
            session.IsBusy = false;

            session.IsAllowed = false;
            Assert.That(session.Can(SchemaCapability.ManagePolicy), Is.False, "the plugin-level gate still applies");
            session.IsAllowed = true;

            session.TreeId = "   ";
            Assert.That(session.Can(SchemaCapability.ManagePolicy), Is.False, "no subject, no action");
            session.TreeId = "orders";

            Assert.That(session.Can(SchemaCapability.ScanCompliance), Is.False, "an unprobed action stays denied");
        });
    }

    [Test]
    public async Task Running_an_operation_marks_the_area_busy_and_notifies_on_both_edges()
    {
        var session = new SchemaSession(new FakeSchemaPluginDomain());
        var notifications = 0;
        var busyDuringOperation = false;
        session.Changed += () => notifications++;

        await session.RunAsync(() =>
        {
            busyDuringOperation = session.IsBusy;
            return Task.CompletedTask;
        });

        Assert.Multiple(() =>
        {
            Assert.That(busyDuringOperation, Is.True);
            Assert.That(session.IsBusy, Is.False);
            Assert.That(notifications, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_faulting_operation_still_clears_the_busy_flag()
    {
        var session = new SchemaSession(new FakeSchemaPluginDomain());

        Assert.That(
            async () => await session.RunAsync(() => throw new InvalidOperationException("boom")),
            Throws.InvalidOperationException);
        Assert.That(session.IsBusy, Is.False, "a fault must not strand the area disabled");
    }

    [Test]
    public void Running_rejects_a_null_operation()
    {
        var session = new SchemaSession(new FakeSchemaPluginDomain());

        Assert.That(async () => await session.RunAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Notifying_with_no_subscriber_is_safe()
    {
        var session = new SchemaSession(new FakeSchemaPluginDomain());

        Assert.That(session.NotifyChanged, Throws.Nothing);
    }

    [Test]
    public void The_session_exposes_the_domain_it_was_built_over()
    {
        var domain = new FakeSchemaPluginDomain();

        Assert.That(new SchemaSession(domain).Domain, Is.SameAs(domain));
    }

    [Test]
    public void The_last_result_is_the_areas_single_status_banner()
    {
        var session = new SchemaSession(new FakeSchemaPluginDomain())
        {
            LastResult = SchemaOperationResult.Denied("not permitted"),
        };

        Assert.Multiple(() =>
        {
            Assert.That(session.LastResult!.Status, Is.EqualTo(SchemaOperationStatus.Denied));
            Assert.That(session.LastResult.Message, Is.EqualTo("not permitted"));
        });
    }

    [Test]
    public void A_dead_letter_page_carries_the_tree_it_was_read_for()
    {
        var view = new SchemaDeadLetterView { Status = SchemaOperationStatus.Succeeded, Count = 3 };
        var session = new SchemaSession(new FakeSchemaPluginDomain())
        {
            DeadLetters = new SchemaDeadLetterPage("orders", view),
        };

        Assert.Multiple(() =>
        {
            Assert.That(session.DeadLetters!.TreeId, Is.EqualTo("orders"));
            Assert.That(session.DeadLetters.View, Is.SameAs(view));
        });
    }

    [Test]
    public void A_fresh_session_holds_no_dead_letter_page()
    {
        Assert.That(new SchemaSession(new FakeSchemaPluginDomain()).DeadLetters, Is.Null);
    }
}
