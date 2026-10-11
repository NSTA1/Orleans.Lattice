using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class NameCheckRaceTests : ShellDesignTestContext
{
    [Test]
    public async Task Confirm_equivalent_method_group_on_rerender_keeps_its_verdict()
    {
        var validator = new GatedValidator();
        var cut = Render<LtNameInput>(p => p
            .Add(x => x.Label, "Tree")
            .Add(x => x.Value, "orders")
            .Add(x => x.Validate, validator.CheckAsync));

        var confirm = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Render(p => p.Add(x => x.Validate, validator.CheckAsync));
        await cut.InvokeAsync(() => validator.Gate.SetResult("Not a group"));

        Assert.That(await confirm, Is.False);
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Not a group"));
    }

    [Test]
    public async Task Confirm_source_replaced_while_checking_abandons_the_old_verdict()
    {
        var source = new FakeSuggestionSource("orders") { Gated = true, IgnoresCancellation = true };
        var cut = Render<LtNameInput>(p => p
            .Add(x => x.Label, "Tree")
            .Add(x => x.Value, "orders")
            .Add(x => x.Existing, source));

        var confirm = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.WaitUntil(() => Assert.That(source.Queries, Has.Count.EqualTo(1)));
        cut.Render(p => p.Add(x => x.Existing, new FakeSuggestionSource()));
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("orders", 8)));

        Assert.That(await confirm, Is.False);
        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Confirm_validator_replaced_while_checking_abandons_its_reply(bool throws)
    {
        var gate = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cut = Render<LtNameInput>(p => p
            .Add(x => x.Label, "Tree")
            .Add(x => x.Value, "orders")
            .Add(x => x.Validate, (_, _) => gate.Task));

        var confirm = cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Render(p => p.Add(x => x.Validate, (_, _) => Task.FromResult<string?>(null)));
        await cut.InvokeAsync(() =>
        {
            if (throws)
            {
                gate.SetException(new InvalidOperationException("Old check failed"));
            }
            else
            {
                gate.SetResult("Old tree is refused");
            }
        });

        Assert.That(await confirm, Is.False);
        Assert.That(cut.FindAll(".lt-field__error, .lt-name-input__note"), Is.Empty);
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    private sealed class GatedValidator
    {
        public TaskCompletionSource<string?> Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<string?> CheckAsync(string text, CancellationToken token) => Gate.Task;
    }
}
