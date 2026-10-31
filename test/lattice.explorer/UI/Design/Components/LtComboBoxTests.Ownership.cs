using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

public sealed partial class LtComboBoxTests
{
    [Test]
    public async Task ConfirmAsync_refuses_a_value_edited_away_and_back_while_validation_runs()
    {
        var source = new FakeSuggestionSource("orders") { Gated = true, IgnoresCancellation = true };
        var cut = RenderBox(source, p => p.Add(x => x.Value, "orders"));
        var confirmation = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        Assert.That(source.Queries, Has.Count.EqualTo(1));

        cut.Render(p => p.Add(x => x.Value, "invoices"));
        cut.Render(p => p.Add(x => x.Value, "orders"));
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("orders", 8)));
        var accepted = await confirmation;

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(cut.FindAll("[role=option]"), Is.Empty, "the abandoned lookup cannot populate the new edit");
        });
    }

    [TestCase("disabled")]
    [TestCase("readonly")]
    [TestCase("source")]
    [TestCase("mode")]
    public async Task ConfirmAsync_refuses_a_round_trip_in_field_context(string change)
    {
        var source = new FakeSuggestionSource("orders") { Gated = true, IgnoresCancellation = true };
        var cut = RenderBox(source, p => p.Add(x => x.Value, "orders"));
        var confirmation = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        Assert.That(source.Queries, Has.Count.EqualTo(1));

        ChangeContext(cut, change, true);
        ChangeContext(cut, change, false, source);
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("orders", 8)));

        Assert.That(await confirmation, Is.False, "returning to the same parameters does not revive an abandoned submission");
    }

    [TestCase("value")]
    [TestCase("source")]
    [TestCase("disabled")]
    [TestCase("readonly")]
    [TestCase("disposed")]
    public async Task Choosing_a_suggestion_does_not_commit_after_its_callback_is_superseded(string change)
    {
        var callback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var commits = new List<string>();
        var source = new FakeSuggestionSource("orders");
        var cut = RenderBox(source, p => p
            .Add(x => x.OnChoose, async (LtSuggestion _) =>
            {
                entered.SetResult();
                await callback.Task;
            })
            .Add(x => x.OnCommit, (string value) => commits.Add(value)));
        cut.Find("input").Input("ord");
        var choice = cut.Find("[role=option]").ClickAsync(new Microsoft.AspNetCore.Components.Web.MouseEventArgs());
        await entered.Task;

        if (change == "disposed")
        {
            await cut.InvokeAsync(() => cut.Instance.DisposeAsync().AsTask());
        }
        else
        {
            ChangeContext(cut, change, true);
        }

        callback.SetResult();
        await choice;

        Assert.That(commits, Is.Empty, "a slow OnChoose must not commit into a replacement field");
    }

    [Test]
    public async Task Choosing_a_suggestion_stops_before_OnChoose_when_ValueChanged_replaces_the_value()
    {
        var callback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var chosen = new List<string>();
        var commits = new List<string>();
        var cut = RenderBox(new FakeSuggestionSource("orders"), p => p
            .Add(x => x.ValueChanged, async (string _) =>
            {
                entered.TrySetResult();
                await callback.Task;
            })
            .Add(x => x.OnChoose, (LtSuggestion item) => chosen.Add(item.Value))
            .Add(x => x.OnCommit, (string value) => commits.Add(value)));
        // Open the list without starting the ValueChanged callback.
        Key(cut, "ArrowDown");
        var choice = cut.Find("[role=option]").ClickAsync(new Microsoft.AspNetCore.Components.Web.MouseEventArgs());
        await entered.Task;
        cut.Render(p => p.Add(x => x.Value, "invoices"));
        callback.SetResult();
        await choice;

        Assert.Multiple(() =>
        {
            Assert.That(chosen, Is.Empty);
            Assert.That(commits, Is.Empty);
        });
    }

    private static void ChangeContext(IRenderedComponent<LtComboBox> cut, string change, bool changed,
        ILtSuggestionSource? original = null)
    {
        switch (change)
        {
            case "value":
                cut.Render(p => p.Add(x => x.Value, "invoices"));
                break;
            case "source":
                cut.Render(p => p.Add(x => x.Source, changed ? new FakeSuggestionSource("invoices") : original));
                break;
            case "mode":
                cut.Render(p => p.Add(x => x.Mode, changed ? LtComboBoxMode.Suggest : LtComboBoxMode.PickExisting));
                break;
            case "disabled":
                cut.Render(p => p.Add(x => x.Disabled, changed));
                break;
            case "readonly":
                cut.Render(p => p.Add(x => x.ReadOnly, changed));
                break;
        }
    }
}
