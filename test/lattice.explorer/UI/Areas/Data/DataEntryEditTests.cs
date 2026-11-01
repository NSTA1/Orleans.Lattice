using System.Text;
using Bunit;
using NSubstitute;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// Creating, editing and deleting a single entry in the Data area (#5367): the
/// controls are drawn only where the data facade's probe grants them, the editor
/// validates JSON and previews the schema policy, warns when the entry changed
/// after it opened, and reports what the facade did; deletion is confirmed by
/// name; and the palette offers each action only while its control is drawn.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataEntryEditTests : DataTestContext
{
    private const string Tree = "orders";

    private LtToastService ToastService => Services.GetRequiredService<LtToastService>();

    [Test]
    public void A_caller_without_write_sees_no_write_control_and_no_command()
    {
        Client.WithTree(Tree, keys: 3);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-value").TextContent, Does.Contain("\"n\": 1").Or.Contain("\"n\":1"));
            Assert.That(Writer.Probes, Does.Contain(Tree).And.Contain(Tree + ":key/0001"));
        });

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[data-lt-command]").Select(e => e.GetAttribute("data-lt-command")), Has.None.StartsWith("data.").And.None.Contains("entry"));
            Assert.That(cut.FindAll("button").Select(b => b.TextContent.Trim()), Has.None.EqualTo("Edit").And.None.EqualTo("Delete...").And.None.EqualTo("New entry..."));
            Assert.That(Area.Commands.Select(command => command.Id), Is.EqualTo(new[] { DataArea.RefreshCommandId }));
        });
    }

    [Test]
    public void A_writer_sees_new_edit_and_delete_and_the_palette_offers_them()
    {
        Client.WithTree(Tree, keys: 3);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() =>
        {
            Assert.That(Button(cut, "New entry...").GetAttribute("data-lt-command"), Is.EqualTo(DataEntryCommands.NewEntryId));
            Assert.That(Button(cut, "Edit").GetAttribute("data-lt-command"), Is.EqualTo(DataEntryCommands.EditEntryId));
            Assert.That(Button(cut, "Delete...").GetAttribute("data-lt-command"), Is.EqualTo(DataEntryCommands.DeleteEntryId));
        });

        Assert.That(
            Area.Commands.Select(command => command.Id),
            Is.EquivalentTo(new[] { DataArea.RefreshCommandId, DataArea.ImportCommandId, DataEntryCommands.NewEntryId, DataEntryCommands.EditEntryId, DataEntryCommands.DeleteEntryId }));
    }

    [Test]
    public void A_key_the_rules_refuse_has_no_edit_even_where_the_tree_allows_writing()
    {
        Client.WithTree(Tree, keys: 3);
        Writer.TreeRights[Tree] = (true, true);
        Writer.RefusedKeys.Add("key/0002");
        var cut = RenderAt("data/orders?key=key%2F0002");
        cut.WaitUntil(() => Button(cut, "New entry..."));
        cut.WaitUntil(() => Assert.That(Writer.Probes, Does.Contain(Tree + ":key/0002")));

        Assert.That(cut.FindAll("button").Select(b => b.TextContent.Trim()), Has.None.EqualTo("Edit").And.None.EqualTo("Delete..."));
    }

    [Test]
    public void A_new_entry_is_created_as_json_and_opened()
    {
        Client.WithTree(Tree, keys: 1);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "New entry..."));
        Button(cut, "New entry...").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("New entry")));
        Control(cut, "Key").Input("order/9");
        cut.Find("textarea").Input("{\"total\":12}");
        cut.Find(".lt-data-editor").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentRelative, Is.EqualTo("/data/orders?key=order%2F9"));
            Assert.That(Encoding.UTF8.GetString(Client.Entries[Tree]["order/9"].ValuePreview), Is.EqualTo("{\"total\":12}"));
            Assert.That(ToastService.Toasts.Select(t => t.Message), Does.Contain("Entry order/9 created."));
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty);
        });
    }

    [Test]
    public void Invalid_json_is_refused_before_save_with_where_it_fails()
    {
        Client.WithTree(Tree, keys: 1);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "New entry..."));
        Button(cut, "New entry...").Click();
        cut.WaitUntil(() => cut.Find("textarea"));

        Control(cut, "Key").Input("order/9");
        cut.Find("textarea").Input("{\"total\":");
        cut.Find(".lt-data-editor").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("This is not valid JSON"));
            Assert.That(cut.Find("textarea").GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(Writer.Writes, Is.Empty);
        });
    }

    [Test]
    public void An_existing_key_is_named_when_a_new_entry_would_replace_it()
    {
        Client.WithTree(Tree, keys: 1);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "New entry..."));
        Button(cut, "New entry...").Click();
        cut.WaitUntil(() => cut.Find("textarea"));

        var key = Control(cut, "Key");
        key.Input("key/0000");
        key.Blur();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("A key named key/0000 already exists")));
    }

    [Test]
    public void Editing_opens_the_value_pretty_printed_and_saves_the_change()
    {
        Client.WithTree(Tree, keys: 2);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Button(cut, "Edit"));
        Button(cut, "Edit").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("textarea").GetAttribute("value"), Is.EqualTo("{\n  \"n\": 1\n}").Or.EqualTo("{\r\n  \"n\": 1\r\n}")));
        cut.Find("textarea").Input("{\"n\": 2}");
        cut.Find(".lt-data-editor").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Encoding.UTF8.GetString(Client.Entries[Tree]["key/0001"].ValuePreview), Is.EqualTo("{\"n\": 2}"));
            Assert.That(ToastService.Toasts.Select(t => t.Message), Does.Contain("Entry key/0001 saved."));
            Assert.That(cut.Find(".lt-data-value").TextContent, Does.Contain("2"));
        });
    }

    [Test]
    public void A_change_between_open_and_save_shows_the_newer_value_and_reload_takes_it()
    {
        Client.WithTree(Tree, keys: 2);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Button(cut, "Edit"));
        Button(cut, "Edit").Click();
        cut.WaitUntil(() => cut.Find("textarea"));

        Writer.WriteBehind(Tree, "key/0001", "{\"n\":99}");
        cut.Find("textarea").Input("{\"n\":2}");
        cut.Find(".lt-data-editor").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-editor__conflict").TextContent, Does.Contain("This entry changed since you opened the editor."));
            Assert.That(cut.Find(".lt-data-editor__conflict pre").TextContent, Does.Contain("99"));
        });
        Assert.That(Writer.Writes, Is.Empty, "nothing is written while the conflict is open");

        Button(cut, "Reload the newer value").Click();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-data-editor__conflict"), Is.Empty);
            Assert.That(cut.Find("textarea").GetAttribute("value"), Does.Contain("99"));
        });
    }

    [Test]
    public void Overwrite_after_a_conflict_writes_the_draft()
    {
        Client.WithTree(Tree, keys: 2);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Button(cut, "Edit"));
        Button(cut, "Edit").Click();
        cut.WaitUntil(() => cut.Find("textarea"));

        Writer.WriteBehind(Tree, "key/0001", "{\"n\":99}");
        cut.Find("textarea").Input("{\"n\":2}");
        cut.Find(".lt-data-editor").Submit();
        cut.WaitUntil(() => Button(cut, "Overwrite with my value"));
        Button(cut, "Overwrite with my value").Click();

        cut.WaitUntil(() => Assert.That(Encoding.UTF8.GetString(Client.Entries[Tree]["key/0001"].ValuePreview), Is.EqualTo("{\"n\":2}")));
    }

    [Test]
    public void The_schema_preview_lists_every_failing_rule_and_the_refusal_is_reported()
    {
        Client.WithTree(Tree, keys: 1);
        Writer.TreeRights[Tree] = (true, true);
        Policies[Tree] = new LatticeSchemaPolicy(
        [
            LatticeSchemaRule.Json(),
            LatticeSchemaRule.MaxLength(15),
        ]);
        Writer.Policy = value => new LatticeSchemaPolicyValidator(Policies[Tree]!).Validate(value);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "New entry..."));
        Button(cut, "New entry...").Click();
        cut.WaitUntil(() => cut.Find("textarea"));

        Control(cut, "Key").Input("order/1");
        cut.Find("textarea").Input("{\"status\":\"open\"}");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-editor__schema-fail").TextContent, Does.Contain("The cluster will refuse this value"));
            Assert.That(cut.FindAll(".lt-data-editor__violations li"), Has.Count.EqualTo(1));
        });

        cut.Find(".lt-data-editor").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-editor__error").TextContent, Does.Contain("rejected by this tree's schema policy")));

        cut.Find("textarea").Input("{\"total\":3}");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-editor__schema-ok").TextContent, Does.Contain("Passes this tree's schema policy (2 rules)")));
    }

    [Test]
    public void A_policy_that_cannot_be_read_says_so_and_the_value_is_still_saved()
    {
        Client.WithTree(Tree, keys: 1);
        Writer.TreeRights[Tree] = (true, true);
        Schema.GetPolicyAsync(Tree, Arg.Any<CancellationToken>()).Returns<Task<LatticeSchemaPolicy?>>(_ => throw new InvalidOperationException("no"));
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "New entry..."));
        Button(cut, "New entry...").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-editor__schema").TextContent, Does.Contain("could not be read")));
    }

    [Test]
    public void A_binary_value_opens_as_base64_and_is_replaced_whole()
    {
        Client.WithTree(Tree);
        Client.Entries[Tree]["blob"] = FakeStateClient.Entry("blob", [0xFF, 0x00, 0x10]);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=blob");
        cut.WaitUntil(() => Button(cut, "Edit"));
        Button(cut, "Edit").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("textarea").GetAttribute("value"), Is.EqualTo(Convert.ToBase64String([0xFF, 0x00, 0x10])));
            Assert.That(cut.Find(".lt-data-editor .lt-data-note").TextContent, Does.Contain("replaced whole"));
        });

        cut.Find("textarea").Input(Convert.ToBase64String([1, 2]));
        cut.Find(".lt-data-editor").Submit();
        cut.WaitUntil(() => Assert.That(Client.Entries[Tree]["blob"].ValuePreview, Is.EqualTo(new byte[] { 1, 2 })));
    }

    [Test]
    public void A_forged_write_the_facade_refuses_is_reported_and_nothing_lands()
    {
        Client.WithTree(Tree, keys: 2);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Button(cut, "Edit"));
        Button(cut, "Edit").Click();
        cut.WaitUntil(() => cut.Find("textarea"));

        Writer.TreeRights[Tree] = (false, false);
        cut.Find("textarea").Input("{\"n\":5}");
        cut.Find(".lt-data-editor").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-editor__error").TextContent, Is.EqualTo("You do not have permission to write this key.")));
        Assert.That(Writer.Writes, Is.Empty);
    }

    [Test]
    public void Delete_is_confirmed_by_typing_the_key_and_reports_it()
    {
        Client.WithTree(Tree, keys: 2);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Button(cut, "Delete..."));
        Button(cut, "Delete...").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo("key/0001")));
        Confirm(cut, "key/0001");

        cut.WaitUntil(() =>
        {
            Assert.That(Client.Entries[Tree].ContainsKey("key/0001"), Is.False);
            Assert.That(ToastService.Toasts.Select(t => t.Message), Does.Contain("Entry key/0001 deleted."));
            Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("No entry at this key"));
            Assert.That(Button(cut, "Write a value"), Is.Not.Null);
        });
    }

    [Test]
    public void A_crdt_value_is_not_edited_here()
    {
        Client.WithTree(Tree);
        Client.Entries[Tree]["counter"] = FakeStateClient.Entry("counter", "x") with { CrdtShape = "PnCounter" };
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=counter");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry").TextContent, Does.Contain("A CRDT value changes through its typed operations")));
        Assert.That(cut.FindAll("button").Select(b => b.TextContent.Trim()), Has.None.EqualTo("Edit"));
    }

    [Test]
    public void The_edit_command_opens_the_editor_from_the_palette()
    {
        Client.WithTree(Tree, keys: 2);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Button(cut, "Edit"));

        var edit = Area.Commands.Single(command => command.Id == DataEntryCommands.EditEntryId);
        cut.InvokeAsync(() => edit.InvokeAsync!(CancellationToken.None).AsTask());

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("Edit entry")));
    }

    [Test]
    public void A_view_offers_no_write_and_is_never_probed()
    {
        var gate = new DataWriteGate(Services);
        var view = new DataTreeEntry { LogicalId = "v", StateId = "v", Kind = DataTreeKind.View };

        var tree = gate.ForTreeAsync(view).Result;
        var key = gate.ForKeyAsync(view, "k").Result;

        Assert.Multiple(() =>
        {
            Assert.That(tree, Is.EqualTo(DataWriteRights.None));
            Assert.That(key, Is.EqualTo(DataWriteRights.None));
            Assert.That(Writer.Probes, Is.Empty);
        });
    }
}
