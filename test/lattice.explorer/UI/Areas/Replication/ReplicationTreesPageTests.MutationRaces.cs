using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Areas.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>Submission ownership across asynchronous validation and departed pages.</summary>
public sealed partial class ReplicationTreesPageTests
{
    [Test]
    public async Task Editing_the_merge_mode_during_validation_requires_a_new_confirmation()
    {
        UseTrees();
        var client = Services.UseTreeCatalogue("inventory");
        var gate = client.CatalogGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        SuggestionFields.Box(cut, "Tree").Input("inventory");

        var submission = cut.Find("form.lt-replication-form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(client.Calls, Does.Contain("ListTreesAsync")));
        Assert.That(submission.IsCompleted, Is.False);
        cut.Find("form.lt-replication-form select").Change("PnCounter");
        gate.SetResult();
        await submission;

        Assert.Multiple(() =>
        {
            Assert.That(Control.Enables, Is.Empty, "the earlier confirmation must not fix a mode the user changed");
            Assert.That(cut.FindAll("form.lt-replication-form"), Has.Count.EqualTo(1));
        });

        await cut.Find("form.lt-replication-form").SubmitAsync();
        Assert.That(Control.Enables, Is.EqualTo(new[] { ("inventory", LatticeMergeMode.PnCounter, (string?)null) }));
    }

    [Test]
    public async Task A_second_submit_while_tree_validation_waits_does_not_start_another_enable()
    {
        UseTrees();
        var client = Services.UseTreeCatalogue("inventory");
        var gate = client.CatalogGate = new TaskCompletionSource();
        Control.ChangeGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        SuggestionFields.Box(cut, "Tree").Input("inventory");

        var form = cut.Find("form.lt-replication-form");
        var first = form.SubmitAsync();
        var second = form.SubmitAsync();
        cut.WaitUntil(() => Assert.That(client.Calls, Does.Contain("ListTreesAsync")));
        var heldWhileValidating = cut.Find("form.lt-replication-form button[type=\"submit\"]").HasAttribute("disabled");
        gate.SetResult();
        cut.WaitUntil(() => Assert.That(Control.Enables, Is.Not.Empty));
        Control.ChangeGate.SetResult();
        await Task.WhenAll(first, second);

        Assert.Multiple(() =>
        {
            Assert.That(heldWhileValidating, Is.True, "validation is already a pending submission");
            Assert.That(Control.Enables, Has.Count.EqualTo(1));
            Assert.That(ToastService.Toasts, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Refused_validation_releases_the_submission_for_a_corrected_tree()
    {
        UseTrees();
        Services.UseTreeCatalogue("inventory");
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        SuggestionFields.Box(cut, "Tree").Input("missing");

        await cut.Find("form.lt-replication-form").SubmitAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Control.Enables, Is.Empty);
            Assert.That(SuggestionFields.ErrorOf(cut, "Tree"), Does.Contain("No tree is named missing"));
            Assert.That(cut.Find("form.lt-replication-form button[type=\"submit\"]").HasAttribute("disabled"), Is.False);
        });

        SuggestionFields.Box(cut, "Tree").Input("inventory");
        await cut.Find("form.lt-replication-form").SubmitAsync();

        Assert.That(Control.Enables, Is.EqualTo(new[] { ("inventory", LatticeMergeMode.LwwRegister, (string?)null) }));
    }

    [Test]
    public async Task Cancelling_the_dialog_while_validation_waits_does_not_enable_the_tree()
    {
        UseTrees();
        var client = Services.UseTreeCatalogue("inventory");
        var gate = client.CatalogGate = new TaskCompletionSource();
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        SuggestionFields.Box(cut, "Tree").Input("inventory");

        var submission = cut.Find("form.lt-replication-form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(client.Calls, Does.Contain("ListTreesAsync")));
        cut.FindAll("form.lt-replication-form button").Single(button => button.TextContent.Trim() == "Cancel").Click();
        gate.SetResult();
        await submission;

        Assert.Multiple(() =>
        {
            Assert.That(Control.Enables, Is.Empty);
            Assert.That(cut.FindAll("form.lt-replication-form"), Is.Empty);
            Assert.That(ToastService.Toasts, Is.Empty);
        });

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Enable replication for a tree").Click();
        Assert.That(cut.FindAll("form.lt-replication-form"), Has.Count.EqualTo(1));
    }

    [TestCase(true, false)]
    [TestCase(true, true)]
    [TestCase(false, false)]
    [TestCase(false, true)]
    public async Task Mutation_replies_after_leaving_the_page_neither_notify_nor_reload(bool enable, bool fail)
    {
        UseTrees();
        var gate = Control.ChangeGate = new TaskCompletionSource();
        Control.ChangeGateIgnoresCancellation = true;
        Control.ChangeFailure = fail ? new TimeoutException() : null;
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        Task submission;
        if (enable)
        {
            Row(cut, "stock").QuerySelector("button")!.Click();
            submission = cut.Find("form.lt-replication-form").SubmitAsync();
            cut.WaitUntil(() => Assert.That(Control.Enables, Has.Count.EqualTo(1)));
        }
        else
        {
            Row(cut, "orders").QuerySelector("button")!.Click();
            cut.Find("form.lt-confirm input").Input("orders");
            submission = cut.Find("form.lt-confirm").SubmitAsync();
            cut.WaitUntil(() => Assert.That(Control.Disables, Has.Count.EqualTo(1)));
        }

        var reads = Control.Reads;
        await DisposeComponentsAsync();
        gate.SetResult();
        await submission;

        Assert.Multiple(() =>
        {
            Assert.That(ToastService.Toasts, Is.Empty, "the reply belongs to the page that has been left");
            Assert.That(Control.Reads, Is.EqualTo(reads), "a departed page must not start another load");
            Assert.That(LeftPage.Fault(Renderer), Is.Null);
        });
    }
}
