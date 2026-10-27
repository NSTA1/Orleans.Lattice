using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtNameInputCancellationTests : ShellDesignTestContext
{
    [Test]
    public async Task Input_external_value_change_discards_the_old_suggestion_delivery()
    {
        var source = new FakeSuggestionSource("old") { Gated = true, IgnoresCancellation = true };
        var cut = Render<LtNameInput>(p => p.Add(x => x.Label, "Tree").Add(x => x.Existing, source));
        var input = cut.Find("input").InputAsync(new Microsoft.AspNetCore.Components.ChangeEventArgs { Value = "old" });
        Assert.That(source.Queries, Has.Count.EqualTo(1));

        cut.Render(p => p.Add(x => x.Value, "new"));
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("old", 8)));
        await input.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("new"));
            Assert.That(cut.FindAll(".lt-field__error, .lt-name-input__note"), Is.Empty);
            Assert.That(cut.Find("[role=status]").TextContent, Is.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Confirm_cancelled_source_never_publishes_a_failure_note(bool faultsAfterCancellation)
    {
        var source = new FakeSuggestionSource { Gated = true, IgnoresCancellation = faultsAfterCancellation };
        var validated = 0;
        var cut = Render<LtNameInput>(p => p
            .Add(x => x.Label, "Tree")
            .Add(x => x.Value, "old")
            .Add(x => x.Existing, source)
            .Add(x => x.Validate, (_, _) =>
            {
                validated++;
                return Task.FromResult<string?>(null);
            }));

        var confirm = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        Assert.That(source.Queries, Has.Count.EqualTo(1));
        cut.Render(p => p.Add(x => x.Value, "new"));
        if (faultsAfterCancellation)
        {
            await cut.InvokeAsync(() => source.Queries[0].Gate.SetException(new InvalidOperationException("stale failure")));
        }

        var accepted = await confirm.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
            Assert.That(validated, Is.Zero);
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("new"));
            Assert.That(cut.FindAll(".lt-field__error, .lt-name-input__note"), Is.Empty);
        });
    }

    [Test]
    public async Task Confirm_validator_observing_cancellation_refuses_the_stale_submission()
    {
        var gate = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken checkedToken = default;
        var cut = Render<LtNameInput>(p => p
            .Add(x => x.Label, "Tree")
            .Add(x => x.Value, "old")
            .Add(x => x.Validate, (_, token) =>
            {
                checkedToken = token;
                token.Register(() => gate.TrySetCanceled(token));
                return gate.Task;
            }));

        var confirm = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        Assert.That(checkedToken.CanBeCanceled, Is.True);
        cut.Render(p => p.Add(x => x.Value, "new"));

        Assert.That(await confirm.WaitAsync(TimeSpan.FromSeconds(5)), Is.False);
        Assert.That(cut.FindAll(".lt-field__error, .lt-name-input__note"), Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Confirm_field_becoming_noneditable_abandons_an_inflight_source(bool readOnly)
    {
        var source = new FakeSuggestionSource("orders") { Gated = true, IgnoresCancellation = true };
        var cut = Render<LtNameInput>(p => p
            .Add(x => x.Label, "Tree").Add(x => x.Value, "orders").Add(x => x.Existing, source));
        var confirm = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Render(p => p.Add(x => x.ReadOnly, readOnly).Add(x => x.Disabled, !readOnly));
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("orders", 8)));

        Assert.That(await confirm.WaitAsync(TimeSpan.FromSeconds(5)), Is.False);
        Assert.That(cut.FindAll(".lt-field__error, .lt-name-input__note"), Is.Empty);
    }

    [TestCase("")]
    [TestCase("   ")]
    public async Task Input_empty_text_cancels_suggestions_and_blur_does_not_validate(string text)
    {
        var source = new FakeSuggestionSource { Gated = true, IgnoresCancellation = true };
        var validated = 0;
        var cut = Render<LtNameInput>(p => p.Add(x => x.Label, "Tree")
            .Add(x => x.Existing, source).Add(x => x.Validate, (_, _) =>
            {
                validated++;
                return Task.FromResult<string?>(null);
            }));
        var input = cut.Find("input").InputAsync(new Microsoft.AspNetCore.Components.ChangeEventArgs { Value = "old" });
        await cut.Find("input").InputAsync(new Microsoft.AspNetCore.Components.ChangeEventArgs { Value = text });
        await cut.Find("input").BlurAsync(new Microsoft.AspNetCore.Components.Web.FocusEventArgs());
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("old", 8)));
        await input.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries, Has.Count.EqualTo(1));
            Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
            Assert.That(validated, Is.Zero);
            Assert.That(cut.FindAll(".lt-field__error, .lt-name-input__note"), Is.Empty);
        });
    }
}
