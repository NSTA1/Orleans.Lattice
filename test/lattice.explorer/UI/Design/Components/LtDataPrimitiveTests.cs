using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The data primitives outside the table: the definition list and the mono
/// cell, whose copy button writes the value to the clipboard and announces the
/// outcome in words.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtDataPrimitiveTests : ShellDesignTestContext
{
    [Test]
    public void A_definition_list_renders_term_and_value_rows()
    {
        RenderFragment rows = builder =>
        {
            builder.OpenComponent<LtDefinition>(0);
            builder.AddAttribute(1, nameof(LtDefinition.Term), "Shards");
            builder.AddAttribute(2, nameof(LtDefinition.ChildContent), (RenderFragment)(b => b.AddContent(0, "16")));
            builder.CloseComponent();
            builder.OpenComponent<LtDefinition>(3);
            builder.AddAttribute(4, nameof(LtDefinition.Term), "Physical id");
            builder.AddAttribute(5, nameof(LtDefinition.Mono), true);
            builder.AddAttribute(6, nameof(LtDefinition.ChildContent), (RenderFragment)(b => b.AddContent(0, "t/acme/a/crm/orders")));
            builder.CloseComponent();
        };

        var cut = Render<LtDefinitionList>(p => p.AddChildContent(rows));
        var terms = cut.FindAll("dl > div > dt");
        var values = cut.FindAll("dl > div > dd");

        Assert.Multiple(() =>
        {
            Assert.That(terms.Select(term => term.TextContent), Is.EqualTo(new[] { "Shards", "Physical id" }));
            Assert.That(values.Select(value => value.TextContent), Is.EqualTo(new[] { "16", "t/acme/a/crm/orders" }));
            Assert.That(values[0].ClassName, Is.EqualTo("lt-dl__value"));
            Assert.That(values[1].ClassName, Is.EqualTo("lt-dl__value lt-dl__value--mono"));
        });
    }

    [Test]
    public void A_mono_cell_shows_its_value_and_a_copy_button_described_by_it()
    {
        var cut = Render<LtMonoCell>(p => p.Add(x => x.Value, "orders/2026/0042"));
        var code = cut.Find("code");
        var button = cut.Find("button");

        Assert.Multiple(() =>
        {
            Assert.That(code.TextContent, Is.EqualTo("orders/2026/0042"));
            Assert.That(code.GetAttribute("title"), Is.EqualTo("orders/2026/0042"), "a truncated value keeps its full text in reach");
            Assert.That(button.GetAttribute("type"), Is.EqualTo("button"));
            Assert.That(button.TextContent, Is.EqualTo("Copy"));
            Assert.That(button.GetAttribute("aria-describedby"), Is.EqualTo(code.Id));
            Assert.That(cut.Find("[role=status]").TextContent, Is.Empty);
        });
    }

    [Test]
    public void Copying_writes_the_value_to_the_clipboard_and_says_so()
    {
        JSInterop.Mode = JSRuntimeMode.Strict;
        JSInterop.SetupVoid(LtMonoCell.ClipboardWriteFunction, "orders/2026/0042").SetVoidResult();
        var cut = Render<LtMonoCell>(p => p.Add(x => x.Value, "orders/2026/0042"));

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            JSInterop.VerifyInvoke(LtMonoCell.ClipboardWriteFunction);
            Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo("Copied"));
        });
    }

    [Test]
    public void A_refused_copy_says_it_failed()
    {
        JSInterop.Mode = JSRuntimeMode.Strict;
        JSInterop.SetupVoid(LtMonoCell.ClipboardWriteFunction, "k").SetException(new JSException("denied"));
        var cut = Render<LtMonoCell>(p => p.Add(x => x.Value, "k"));

        cut.Find("button").Click();

        Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo("Copy failed"));
    }

    [Test]
    public void An_untruncated_cell_has_no_tooltip_and_a_custom_label()
    {
        var cut = Render<LtMonoCell>(p => p.Add(x => x.Value, "k").Add(x => x.Truncate, false).Add(x => x.CopyLabel, "Copy key"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-mono-cell").ClassName, Is.EqualTo("lt-mono-cell"));
            Assert.That(cut.Find("code").HasAttribute("title"), Is.False);
            Assert.That(cut.Find("button").TextContent, Is.EqualTo("Copy key"));
        });
    }
}
