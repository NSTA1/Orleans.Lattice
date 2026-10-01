using AngleSharp.Dom;
using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// Issue #4120: every field primitive renders one structure, so every field lines up with
/// every other in a toolbar or a form - a <c>.lt-field</c> whose first child is a visible
/// <c>label.lt-field__label</c> (the label row), followed by the control box, which is the
/// labelled <c>.lt-input</c> or <c>.lt-select</c> itself or a <c>*__control</c> frame that
/// holds it. The sweep also fails when a new component in the design system draws a text
/// input or select without being added to it, so no field primitive can skip the label row.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class FieldPrimitiveStructureTests : ShellDesignTestContext
{
    private const string ComponentsRoot = "src/lattice.explorer/UI/Design/Components";

    /// <summary>
    /// The design-system components that draw a form control with its label beside it rather
    /// than above it, and so are not label-row fields. A checkbox is a small box with its
    /// label to the right; a switch is one button whose label is its own text.
    /// </summary>
    private static readonly string[] InlineLabelPrimitives = ["LtCheckbox", "LtSwitch"];

    /// <summary>Every label-row field primitive, by component name.</summary>
    public static IEnumerable<TestCaseData> Fields()
    {
        yield return new TestCaseData(nameof(LtTextInput)).SetArgDisplayNames(nameof(LtTextInput));
        yield return new TestCaseData(nameof(LtNameInput)).SetArgDisplayNames(nameof(LtNameInput));
        yield return new TestCaseData(nameof(LtComboBox)).SetArgDisplayNames(nameof(LtComboBox));
        yield return new TestCaseData(nameof(LtMultiComboBox)).SetArgDisplayNames(nameof(LtMultiComboBox));
        yield return new TestCaseData(nameof(LtSelect)).SetArgDisplayNames(nameof(LtSelect));
        yield return new TestCaseData(nameof(LtSearchInput)).SetArgDisplayNames(nameof(LtSearchInput));
        yield return new TestCaseData(nameof(LtDateTimeInput)).SetArgDisplayNames(nameof(LtDateTimeInput));
        yield return new TestCaseData(nameof(LtDurationInput)).SetArgDisplayNames(nameof(LtDurationInput));
    }

    [TestCaseSource(nameof(Fields))]
    public void Every_field_primitive_draws_a_visible_label_row_over_its_control_box(string primitive)
    {
        var root = RenderField(primitive);

        Assert.That(root.ClassList, Does.Contain("lt-field"), $"{primitive}'s root is not a .lt-field");
        var label = root.FirstElementChild;
        Assert.That(label, Is.Not.Null, $"{primitive} renders nothing inside its field");
        Assert.Multiple(() =>
        {
            Assert.That(label!.LocalName, Is.EqualTo("label"), $"{primitive}'s first child is not its label row");
            Assert.That(label.ClassList, Is.EquivalentTo(new[] { "lt-field__label" }),
                $"{primitive}'s label row must carry exactly lt-field__label - never lt-visually-hidden, never a class that moves or hides it");
            Assert.That(label.TextContent, Is.EqualTo("The field"), $"{primitive}'s label row shows the label");
        });

        var box = label!.NextElementSibling;
        Assert.That(box, Is.Not.Null, $"{primitive} has no control box under its label row");
        var control = root.QuerySelector("#" + label.GetAttribute("for"));
        Assert.That(control, Is.Not.Null, $"{primitive}'s label is bound to no control");

        Assert.Multiple(() =>
        {
            Assert.That(control!.ClassList.Contains("lt-input") || control.ClassList.Contains("lt-select"), Is.True,
                $"{primitive}'s labelled control is not drawn as .lt-input or .lt-select, so it does not take the one control height");
            var boxIsControl = ReferenceEquals(box, control);
            var boxIsFrame = box!.ClassList.Any(name => name.EndsWith("__control", StringComparison.Ordinal)) && box.Contains(control);
            Assert.That(boxIsControl || boxIsFrame, Is.True,
                $"{primitive}'s control box (the element after its label row) is <{box.LocalName} class=\"{box.ClassName}\">, neither the labelled control nor a *__control frame holding it");
        });
    }

    [Test]
    public void Every_design_system_component_that_draws_a_text_field_or_select_is_swept()
    {
        var swept = Fields().Select(data => (string)data.Arguments[0]!).ToHashSet(StringComparer.Ordinal);
        var root = Path.Combine(HygieneRepository.FindRepoRoot(), ComponentsRoot.Replace('/', Path.DirectorySeparatorChar));
        var drawing = HygieneRepository.EnumerateFiles(root, "*.razor")
            .Where(file => File.ReadAllText(file) is var markup
                && (markup.Contains("<input", StringComparison.Ordinal)
                    || markup.Contains("<select", StringComparison.Ordinal)
                    || markup.Contains("<textarea", StringComparison.Ordinal)
                    || markup.Contains("<LtComboBox", StringComparison.Ordinal)))
            .Select(Path.GetFileNameWithoutExtension)
            .ToArray();

        Assert.That(drawing, Has.Length.GreaterThanOrEqualTo(6), "the scan must reach the field primitives");
        var unswept = drawing.Where(name => !swept.Contains(name!) && !InlineLabelPrimitives.Contains(name, StringComparer.Ordinal)).ToArray();

        Assert.That(unswept, Is.Empty,
            "These components draw a form control but are not in the label-row sweep. Add them to Fields() (a label-row field) "
            + "or to InlineLabelPrimitives (a control whose label is beside it):" + Environment.NewLine + string.Join(Environment.NewLine, unswept));
    }

    private IElement RenderField(string primitive)
    {
        var source = new FakeSuggestionSource(["a/crm/orders"]);
        return primitive switch
        {
            nameof(LtTextInput) => Root(Render<LtTextInput>(p => p.Add(x => x.Label, "The field").Add(x => x.Placeholder, "a hint"))),
            nameof(LtNameInput) => Root(Render<LtNameInput>(p => p.Add(x => x.Label, "The field").Add(x => x.Existing, source).Add(x => x.Noun, "tree"))),
            nameof(LtComboBox) => Root(Render<LtComboBox>(p => p.Add(x => x.Label, "The field").Add(x => x.Source, source).Add(x => x.Noun, "tree"))),
            nameof(LtMultiComboBox) => Root(Render<LtMultiComboBox>(p => p.Add(x => x.Label, "The field").Add(x => x.Source, source).Add(x => x.Noun, "tree"))),
            nameof(LtSelect) => Root(Render<LtSelect>(p => p.Add(x => x.Label, "The field").Add(x => x.Options, [new LtSelectOption("a", "A")]))),
            nameof(LtSearchInput) => Root(Render<LtSearchInput>(p => p.Add(x => x.Label, "The field"))),
            nameof(LtDateTimeInput) => Root(Render<LtDateTimeInput>(p => p.Add(x => x.Label, "The field").Add(x => x.EmptyText, "Latest"))),
            nameof(LtDurationInput) => Root(Render<LtDurationInput>(p => p.Add(x => x.Label, "The field"))),
            _ => throw new ArgumentOutOfRangeException(nameof(primitive), primitive, "not a field primitive"),
        };
    }

    private static IElement Root<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        cut.Nodes.OfType<IElement>().Single();
}
