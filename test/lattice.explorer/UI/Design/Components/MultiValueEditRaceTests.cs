using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class MultiValueEditRaceTests : ShellDesignTestContext
{
    [TestCase("values")]
    [TestCase("source")]
    [TestCase("text")]
    [TestCase("removal")]
    [TestCase("dispose")]
    public async Task Add_replaced_while_checking_does_not_raise_old_values(string replacement)
    {
        var source = new FakeSuggestionSource("us-east") { Gated = true, IgnoresCancellation = true };
        var changes = new List<IReadOnlyList<string>>();
        var cut = Render<LtMultiComboBox>(p => p
            .Add(x => x.Label, "Regions")
            .Add(x => x.Values, new[] { "eu-west" })
            .Add(x => x.Source, source)
            .Add(x => x.ValuesChanged, values => changes.Add(values)));

        var edit = cut.Find("input").InputAsync(new ChangeEventArgs { Value = "us-east," });
        cut.WaitUntil(() => Assert.That(source.Queries, Has.Count.EqualTo(1)));
        switch (replacement)
        {
            case "values":
                cut.Render(p => p.Add(x => x.Values, new[] { "eu-north" }));
                break;
            case "source":
                cut.Render(p => p.Add(x => x.Source, new FakeSuggestionSource()));
                break;
            case "text":
                source.Gated = false;
                cut.Find("input").Input("new text");
                break;
            case "removal":
                cut.Find(".lt-combobox__remove").Click();
                break;
            case "dispose":
                await DisposeComponentsAsync();
                break;
        }

        changes.Clear();
        source.Gated = false;
        await cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("us-east", 8)));
        await edit;

        Assert.That(changes, Is.Empty, "an old edit must not overwrite a new selection");
        if (replacement == "text")
        {
            Assert.That(cut.Find("input").GetAttribute("value"), Is.EqualTo("new text"));
        }
    }
}
