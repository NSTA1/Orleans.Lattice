using AngleSharp.Dom;
using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The rule builder's harness: opens a tree's policy editor on the Policy tab and
/// drives the builder the way an operator does - pick a member, pick a card,
/// fill its details, add, save - by the controls' labels, never by position.
/// </summary>
public abstract class SchemaRuleBuilderTestBase : SchemaTestContext
{
    internal IRenderedComponent<SchemaTreePage> OpenEditor(string tree = "orders", LtBreakpoint? band = null)
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}", band);
        cut.WaitUntil(() => Assert.That(Buttons(cut).Count(button => Text(button) is "Edit policy" or "Set a policy"), Is.EqualTo(1)));
        Buttons(cut).Single(button => Text(button) is "Edit policy" or "Set a policy").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-rulebuilder"), Has.Count.EqualTo(1)));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-rulebuilder__aside [aria-busy], .lt-schema-rulebuilder__aside .lt-skeleton"), Is.Empty));
        return cut;
    }

    internal static IReadOnlyList<IElement> Buttons(IRenderedComponent<SchemaTreePage> cut) => cut.FindAll("button");

    internal static string Text(IElement element) => element.TextContent.Trim();

    internal static void Click(IRenderedComponent<SchemaTreePage> cut, string text) =>
        Buttons(cut).Single(button => Text(button) == text).Click();

    internal static void ClickLabelled(IRenderedComponent<SchemaTreePage> cut, string label) =>
        cut.Find($"button[aria-label='{label}']").Click();

    internal static void StartRule(IRenderedComponent<SchemaTreePage> cut)
    {
        Click(cut, "Add a rule");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-composer"), Has.Count.EqualTo(1)));
    }

    internal static IElement Field(IRenderedComponent<SchemaTreePage> cut, string label, int occurrence = 0)
    {
        var labels = cut.FindAll("label").Where(candidate => Text(candidate) == label).ToArray();
        Assert.That(labels, Has.Length.GreaterThan(occurrence), $"no field labelled '{label}'");
        return cut.Find("#" + labels[occurrence].GetAttribute("for"));
    }

    internal static void Type(IRenderedComponent<SchemaTreePage> cut, string label, string value, int occurrence = 0) =>
        Field(cut, label, occurrence).Input(value);

    internal static void Choose(IRenderedComponent<SchemaTreePage> cut, string label, string value, int occurrence = 0) =>
        Field(cut, label, occurrence).Change(value);

    internal static void Tick(IRenderedComponent<SchemaTreePage> cut, string label, int occurrence = 0) =>
        cut.FindAll("[role=switch]").Where(control => control.QuerySelector(".lt-switch__label")?.TextContent.Trim() == label).ElementAt(occurrence).Click();

    internal static void Kind(IRenderedComponent<SchemaTreePage> cut, SchemaCardKind kind, int gallery = 0)
    {
        var group = cut.FindAll("[role=radiogroup]")[gallery];
        group.QuerySelectorAll("input[type=radio]").Single(radio => radio.GetAttribute("value") == kind.ToString()).Change(kind.ToString());
    }

    internal static void Path(IRenderedComponent<SchemaTreePage> cut, string path) => Type(cut, "Member path", path);

    internal static void Commit(IRenderedComponent<SchemaTreePage> cut)
    {
        Buttons(cut).Single(button => Text(button) is "Add rule" or "Update rule" or "Add alternative").Click();
    }

    internal static IReadOnlyList<string> Sentences(IRenderedComponent<SchemaTreePage> cut) =>
        [.. cut.FindAll(".lt-schema-ruleset__rule > .lt-schema-ruleset__sentence").Select(sentence => Collapse(sentence.TextContent))];

    internal static string Collapse(string text) => string.Join(' ', text.Split((char[])[' ', '\n', '\r', '\t'], StringSplitOptions.RemoveEmptyEntries));

    internal void Save(IRenderedComponent<SchemaTreePage> cut)
    {
        var before = Schema.CountOf("SetPolicy");
        Click(cut, "Save policy");
        cut.WaitUntil(() => Assert.That(Schema.CountOf("SetPolicy"), Is.EqualTo(before + 1), cut.FindAll(".lt-schema-error").Select(error => error.TextContent).FirstOrDefault() ?? "no save"));
    }
}
