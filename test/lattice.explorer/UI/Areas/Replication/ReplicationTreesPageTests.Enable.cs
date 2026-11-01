using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>Enabling replication: from a row and for a new tree, confirmed in a dialog.</summary>
public sealed partial class ReplicationTreesPageTests
{
    [Test]
    public void Enabling_a_disabled_tree_keeps_its_fixed_merge_mode_and_is_confirmed()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "stock").QuerySelector("button")!.Click();
        var dialog = cut.Find(".lt-dialog");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector(".lt-dialog__title")!.TextContent, Is.EqualTo("Enable replication"));
            Assert.That(dialog.QuerySelector("input")!.GetAttribute("value"), Is.EqualTo("stock"));
            Assert.That(dialog.QuerySelector("input")!.HasAttribute("readonly"), Is.True);
            Assert.That(dialog.QuerySelector("select")!.HasAttribute("disabled"), Is.True, "the merge mode was fixed when first enabled");
            Assert.That(dialog.QuerySelector("select option[selected]")!.GetAttribute("value"), Is.EqualTo("OrSet"));
            Assert.That(Control.Enables, Is.Empty, "nothing happens until the dialog is confirmed");
        });

        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Control.Enables, Is.EqualTo(new[] { ("stock", LatticeMergeMode.OrSet, (string?)null) }));
            Assert.That(Cells(Row(cut, "stock"))[1], Is.EqualTo("Enabled"));
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication enabled for stock (OR-set)."));
            Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty);
        });
    }

    [Test]
    public void Enabling_a_new_tree_takes_its_id_merge_mode_and_optional_bootstrap_source()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        var submit = () => cut.Find("form.lt-replication-form button[type=\"submit\"]");
        var emptyDisabled = submit().HasAttribute("disabled");

        var inputs = cut.FindAll("form.lt-replication-form input");
        inputs[0].Input("inventory");
        cut.Find("form.lt-replication-form select").Change("PnCounter");
        cut.FindAll("form.lt-replication-form input")[1].Input("eu-north");
        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(emptyDisabled, Is.True, "a tree id is required");
            Assert.That(Control.Enables, Is.EqualTo(new[] { ("inventory", LatticeMergeMode.PnCounter, (string?)"eu-north") }));
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication enabled for inventory (PN-counter). A snapshot bootstrap was requested."));
            Assert.That(ToastService.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Success));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()), Does.Contain("inventory"));
        });
    }

    [Test]
    public void An_app_owned_tree_id_cannot_be_enabled_here()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        cut.FindAll("form.lt-replication-form input")[0].Input("a/crm/notes");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("form.lt-replication-form .lt-field__error").TextContent, Does.Contain("belongs to the app crm; its enrolment follows the app install."));
            Assert.That(cut.Find("form.lt-replication-form input").GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(cut.Find("form.lt-replication-form button[type=\"submit\"]").HasAttribute("disabled"), Is.True);
        });

        cut.Find("form.lt-replication-form").Submit();
        Assert.That(Control.Enables, Is.Empty);
    }

    [Test]
    public void Enabling_an_already_enabled_tree_reports_it_calmly()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        cut.FindAll("form.lt-replication-form input")[0].Input("orders");
        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication was already enabled for orders (LWW register)."));
            Assert.That(ToastService.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Info));
        });
    }

    [Test]
    public void While_a_change_is_in_flight_the_controls_are_held_and_cancel_closes_the_dialog()
    {
        UseTrees();
        Control.ChangeGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Row(cut, "stock").QuerySelector("button")!.Click();
        cut.Find("form.lt-replication-form").Submit();
        var submit = cut.Find("form.lt-replication-form button[type=\"submit\"]");

        Assert.Multiple(() =>
        {
            Assert.That(submit.TextContent.Trim(), Is.EqualTo("Enabling"));
            Assert.That(submit.HasAttribute("disabled"), Is.True, "a second submit cannot race the first");
            Assert.That(Row(cut, "orders").QuerySelector("button")!.HasAttribute("disabled"), Is.True);
        });

        cut.InvokeAsync(() => Control.ChangeGate.SetResult());
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty);
            Assert.That(ToastService.Toasts, Has.Count.EqualTo(1));
            Assert.That(Cells(Row(cut, "stock"))[1], Is.EqualTo("Enabled"));
        });

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-replication-form"), Has.Count.EqualTo(1)));
        cut.FindAll("form.lt-replication-form button").Single(button => button.TextContent.Trim() == "Cancel").Click();
        Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty);
    }

    [Test]
    public void Closing_the_dialog_mid_enable_leaves_the_enable_running_and_its_result_still_reported()
    {
        UseTrees();
        Control.ChangeGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        cut.FindAll("form.lt-replication-form input")[0].Input("inventory");
        cut.FindAll("form.lt-replication-form input")[1].Input("eu-north");
        cut.Find("form.lt-replication-form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-replication-form [role=\"status\"]"), Has.Count.EqualTo(1)));

        cut.FindAll("form.lt-replication-form button").Single(button => button.TextContent.Trim() == "Cancel").Click();
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty, "the dialog closes");
            Assert.That(ToastService.Toasts, Is.Empty, "the enable has not finished yet");
        });

        cut.InvokeAsync(() => Control.ChangeGate.SetResult());
        cut.WaitUntil(() =>
        {
            Assert.That(Control.Enables, Is.EqualTo(new[] { ("inventory", LatticeMergeMode.LwwRegister, (string?)"eu-north") }));
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication enabled for inventory (LWW register). A snapshot bootstrap was requested."));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()), Does.Contain("inventory"));
        });
    }

    [Test]
    public void While_a_bootstrapped_enable_is_in_flight_the_dialog_says_it_waits_for_the_source()
    {
        UseTrees();
        Control.ChangeGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        Assert.That(cut.FindAll("form.lt-replication-form [role=\"status\"]"), Is.Empty, "nothing is in flight yet");
        cut.FindAll("form.lt-replication-form input")[0].Input("inventory");
        cut.FindAll("form.lt-replication-form input")[1].Input("eu-north");
        cut.Find("form.lt-replication-form").Submit();

        cut.WaitUntil(() => Assert.That(
            cut.Find("form.lt-replication-form [role=\"status\"]").TextContent.Trim(),
            Is.EqualTo("Enabling replication for inventory and asking eu-north for a snapshot. "
                + "This waits for eu-north to answer, which can take a minute or more. "
                + "You can close this dialog; stay on this page and a message reports the result.")));

        cut.InvokeAsync(() => Control.ChangeGate.SetResult());
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty);
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("Replication enabled for inventory (LWW register). A snapshot bootstrap was requested."));
        });
    }

    [Test]
    public void At_the_compact_width_the_enable_flow_is_a_full_height_sheet()
    {
        UseTrees();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__open"), Has.Count.EqualTo(6)));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();

        Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end"));
    }
}
