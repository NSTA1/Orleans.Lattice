namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Negative controls for the unmodelled-grain census, over a synthetic source
/// tree, notes and register. Each failure the gate can report is shown red here
/// against a clean baseline that is shown green, so a red result is
/// attributable to the one change each control makes.
/// </summary>
public sealed partial class RefinementSurfaceCensusTests
{
    private const string SyntheticRegisterLabel = "synthetic/UNMODELLED.md";

    private static readonly (string RelativePath, string Text)[] SyntheticTree =
    [
        ("alpha/Grains/PlainGrain.cs", "namespace A;\ninternal sealed class PlainGrain : Grain, IPlainGrain\n{\n}\n"),
        ("alpha/Grains/StatefulGrain.cs",
            "namespace A;\ninternal sealed class StatefulGrain(\n    IPersistentState<S> state)\n    : Orleans.Grain<S>,\n      IStatefulGrain\n{\n}\n"),
        ("alpha/Grains/RawGrain.cs", "namespace A;\ninternal sealed class RawGrain : IGrainBase, IRawGrain\n{\n    public IGrainContext GrainContext => null!;\n}\n"),
        ("alpha/Contracts/IPlainGrain.cs", "namespace A;\npublic interface IPlainGrain : IGrainWithStringKey\n{\n}\n"),
        ("beta/Base/IntermediateBase.cs", "namespace B;\npublic abstract class IntermediateBase : Grain\n{\n}\n"),
        ("beta/DerivedWorker.cs", "namespace B;\ninternal sealed partial class DerivedWorker : IntermediateBase, IWorker\n{\n}\n"),
        ("beta/DerivedWorker.Part.cs", "namespace B;\ninternal sealed partial class DerivedWorker\n{\n}\n"),
        ("beta/Helper.cs", "namespace B;\n// class Pretend : Grain\ninternal sealed class Helper : IDisposable\n{\n    public void Dispose() { }\n}\n"),
    ];

    private static readonly (string Label, string Text)[] SyntheticNotes =
    [
        ("spec/a/Refinement.md", "| `Step` | `PlainGrain.StepAsync` | Yes |\n\nThe `StatefulGrain` state is the model's `log`.\n"),
        ("spec/b/Refinement.md", "## Deliberate abstraction gaps\n\n- **Base.** IntermediateBase carries no protocol of its own.\n"),
    ];

    private static readonly string[] SyntheticRows =
    [
        "| `RawGrain` | `alpha` | A diagnostic probe with no protocol. |",
        "| `DerivedWorker` | `beta` | A scheduler whose protocol is tracked as a follow-up. |",
    ];

    private static IReadOnlyList<RefinementSurfaceCensusGrain> SyntheticGrains(params (string, string)[] extra) =>
        RefinementSurfaceCensus.FindGrains([.. SyntheticTree, .. extra]);

    private static string SyntheticRegister(params string[] rows) =>
        "# Unmodelled grains\n\nProse before the table.\n\n## Register\n\n| Grain | Package | Reason |\n| --- | --- | --- |\n"
        + string.Join("\n", rows) + "\n\n## After\n\nProse after the table.\n";

    private static IReadOnlyList<string> SyntheticProblems(IReadOnlyList<RefinementSurfaceCensusGrain> grains, params string[] rows) =>
        RefinementSurfaceCensus.Problems(
            grains,
            SyntheticNotes,
            RefinementSurfaceCensus.ReadRegister(SyntheticRegister(rows), SyntheticRegisterLabel),
            SyntheticRegisterLabel);

    [Test]
    public void Control_the_scan_finds_every_grain_form_and_nothing_else()
    {
        var grains = SyntheticGrains();

        Assert.Multiple(() =>
        {
            Assert.That(grains.Select(g => $"{g.Package}:{g.Name}"), Is.EqualTo(new[]
            {
                "alpha:PlainGrain", "alpha:RawGrain", "alpha:StatefulGrain", "beta:DerivedWorker", "beta:IntermediateBase",
            }), "Grain, a qualified multi-line Grain<T>, IGrainBase, an abstract base and its partial subclass; no interface, no helper");
            Assert.That(grains.Single(g => g.Name == "IntermediateBase").IsAbstract, Is.True);
            Assert.That(grains.Single(g => g.Name == "DerivedWorker").SourcePath, Is.EqualTo("beta/DerivedWorker.cs"));
        });
    }

    [Test]
    public void Control_the_clean_synthetic_tree_reports_no_problems()
    {
        Assert.That(SyntheticProblems(SyntheticGrains(), SyntheticRows), Is.Empty);
    }

    [Test]
    public void Negative_control_an_unlisted_grain_no_note_names_fails()
    {
        var grains = SyntheticGrains(("gamma/NewGrain.cs", "namespace G;\ninternal sealed class NewGrain : Grain, INewGrain\n{\n}\n"));

        Assert.That(SyntheticProblems(grains, SyntheticRows),
            Has.Count.EqualTo(1).And.Some.Contains("`NewGrain`").And.Some.Contains("does not list"));
    }

    [Test]
    public void Negative_control_a_row_for_a_grain_that_does_not_exist_fails()
    {
        Assert.That(SyntheticProblems(SyntheticGrains(), [.. SyntheticRows, "| `GoneGrain` | `beta` | Removed long ago. |"]),
            Has.Count.EqualTo(1).And.Some.Contains("`GoneGrain`").And.Some.Contains("is not a grain declared"));
    }

    [Test]
    public void Negative_control_a_row_for_a_grain_a_note_now_names_fails()
    {
        Assert.That(SyntheticProblems(SyntheticGrains(), ["| `PlainGrain` | `alpha` | Not modelled. |", .. SyntheticRows]),
            Has.Count.EqualTo(1).And.Some.Contains("`PlainGrain` is named by spec/a/Refinement.md"));
    }

    [Test]
    public void Negative_control_a_row_in_the_wrong_package_fails()
    {
        var problems = SyntheticProblems(SyntheticGrains(), SyntheticRows[1], "| `RawGrain` | `beta` | A diagnostic probe. |");

        Assert.Multiple(() =>
        {
            Assert.That(problems, Has.Some.Contains("`RawGrain` is listed in package `beta`").And.Contains("`alpha`"));
            Assert.That(problems, Has.Some.Contains("src/alpha/Grains/RawGrain.cs declares the grain `RawGrain`"));
        });
    }

    [Test]
    public void Negative_control_a_repeated_or_unsorted_row_fails()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SyntheticProblems(SyntheticGrains(), [.. SyntheticRows, SyntheticRows[1]]),
                Has.Some.Contains("lists `DerivedWorker` (`beta`) on lines"));
            Assert.That(SyntheticProblems(SyntheticGrains(), SyntheticRows[1], SyntheticRows[0]),
                Has.Some.Contains("`RawGrain` (`alpha`) is out of order"));
        });
    }

    [Test]
    public void Control_a_note_naming_the_grains_own_contract_interface_names_the_grain()
    {
        var notes = SyntheticNotes.Append(("spec/c/Refinement.md", "| `Probe` | `IRawGrain.ProbeAsync` | Yes |\n")).ToList();
        var register = RefinementSurfaceCensus.ReadRegister(SyntheticRegister(SyntheticRows[1]), SyntheticRegisterLabel);
        var stale = RefinementSurfaceCensus.ReadRegister(SyntheticRegister(SyntheticRows), SyntheticRegisterLabel);

        Assert.Multiple(() =>
        {
            Assert.That(SyntheticGrains().Single(g => g.Name == "RawGrain").Contract, Is.EqualTo("IRawGrain"));
            Assert.That(SyntheticGrains().Single(g => g.Name == "DerivedWorker").Contract, Is.Null,
                "DerivedWorker implements IWorker, not IDerivedWorker");
            Assert.That(RefinementSurfaceCensus.Problems(SyntheticGrains(), notes, register, SyntheticRegisterLabel), Is.Empty);
            Assert.That(RefinementSurfaceCensus.Problems(SyntheticGrains(), notes, stale, SyntheticRegisterLabel),
                Has.Some.Contains("`RawGrain` is named by spec/c/Refinement.md"));
        });
    }

    [Test]
    public void Negative_control_a_contract_the_grain_does_not_implement_does_not_name_it()
    {
        var notes = SyntheticNotes.Append(("spec/c/Refinement.md", "`IDerivedWorker` and `IWorker.RunAsync`.\n")).ToList();
        var register = RefinementSurfaceCensus.ReadRegister(SyntheticRegister(SyntheticRows[0]), SyntheticRegisterLabel);

        Assert.That(RefinementSurfaceCensus.Problems(SyntheticGrains(), notes, register, SyntheticRegisterLabel),
            Has.Count.EqualTo(1).And.Some.Contains("`DerivedWorker`").And.Some.Contains("does not list"));
    }

    [Test]
    public void Negative_control_a_name_inside_a_longer_identifier_does_not_name_the_grain()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RefinementSurfaceCensus.Names("`IRawGrain` and RawGrainState and RawGrain2", "RawGrain"), Is.False);
            Assert.That(RefinementSurfaceCensus.Names("see `RawGrain.ProbeAsync`.", "RawGrain"), Is.True);
            Assert.That(RefinementSurfaceCensus.Names("(RawGrain)", "RawGrain"), Is.True);
        });
    }

    [TestCase("# Title\n\nNo register section.\n", "has no '## Register' section")]
    [TestCase("## Register\n\nProse but no table.\n", "section has no table")]
    [TestCase("## Register\n\n| Grain | Reason |\n| --- | --- |\n", "the register header is")]
    [TestCase("## Register\n\n| Grain | Package | Reason |\n| --- | --- | --- |\n| `RawGrain` | `alpha` | TODO |\n", "needs a real reason")]
    [TestCase("## Register\n\n| Grain | Package | Reason |\n| --- | --- | --- |\n| RawGrain | `alpha` | A probe. |\n", "must be one backticked class name")]
    [TestCase("## Register\n\n| Grain | Package | Reason |\n| --- | --- | --- |\n| `RawGrain` | alpha | A probe. |\n", "must be one backticked src/ directory name")]
    [TestCase("## Register\n\n| Grain | Package | Reason |\n| --- | --- | --- |\n| `RawGrain` | `alpha` |\n", "has 2 cells")]
    [TestCase("## Register\n\n| Grain | Package | Reason |\n| --- | --- | --- |\n\n| Grain | Package | Reason |\n", "a second table")]
    public void Negative_control_a_register_the_gate_cannot_read_is_rejected(string markdown, string expected)
    {
        Assert.That(() => RefinementSurfaceCensus.ReadRegister(markdown, SyntheticRegisterLabel),
            Throws.InvalidOperationException.With.Message.Contains(expected));
    }
}
