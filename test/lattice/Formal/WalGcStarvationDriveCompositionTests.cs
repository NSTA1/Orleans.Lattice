using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Mechanises the parametric half of the refinement argument between
/// <c>spec/scheduling/WalGcStarvationDriveWitness.tla</c> and the CI core
/// <c>spec/scheduling/WalGcStarvationDrive.tla</c>.
/// <para>
/// TLC checks the witness against the core only at one leaf. That check alone
/// cannot justify a multi-leaf composition, so the per-leaf lemmas it confirms
/// are lifted to any <c>Leaves</c> by the syntactic facts pinned here: every
/// definition the two modules share is byte-identical after comments are
/// removed, apart from a fixed set whose only difference is a named erasure of
/// the witness-only <c>stall</c> and <c>call</c> fields; and those fields, the
/// stall bound, the jitter, and the initial-state marker are read or written
/// nowhere else. Every witness action that touches them does so on its own
/// leaf <c>l</c>, so no cross-leaf interaction can depend on them, and every
/// other action projects through <c>AbsLeaf</c> onto the identical core action
/// for every leaf set. The full argument is in
/// <c>WalGcStarvationDriveWitnessRefinement.md</c>.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalGcStarvationDriveCompositionTests
{
    private static readonly Regex DefinitionHeader =
        new(@"^(?<name>[A-Za-z_]\w*)(\([^)]*\))?\s*==", RegexOptions.CultureInvariant);

    private static readonly Regex WitnessOnlyIdentifier =
        new(@"\b(stall|call|StallTicks|jitter|fresh|CallArrive|CallServe)\b", RegexOptions.CultureInvariant);

    private static readonly string[] CoreOnly = ["TurnReleased", "Fairness"];

    private static readonly string[] WitnessOnly =
    [
        "wvars", "TurnNotHeldAcrossDrive", "CallArrive", "CallServe", "MarkedInit", "MarkedStep",
        "AbsLeaf", "Core", "CoreInitRefined", "CoreStepRefined", "CoreFairnessRefined",
    ];

    /// <summary>
    /// Shared definitions that differ, each with the erasure that maps the
    /// witness text onto the core text. Edits are applied to whitespace-collapsed
    /// text and every <c>Find</c> must occur exactly once.
    /// </summary>
    private static readonly Dictionary<string, (string Find, string Replace)[]> Erasures = new()
    {
        ["TypeOK"] = [("stall : (-2)..StallTicks, ", ""), ("call : {\"new\", \"queued\", \"done\"}, ", "")],
        ["Init"] = [("stall |-> 0, ", ""), ("call |-> \"new\", ", "")],
        ["TimerTick"] =
        [
            ("IF leaf[l].stall < StallTicks THEN leaf' = [leaf EXCEPT ![l].stall = @ + 1] ELSE leaf'", "leaf'"),
        ],
        ["ReArm"] =
        [
            ("\\E jitter \\in 0..2 : leaf' = [leaf EXCEPT ![l].stall = IF leaf[l].round = 0 THEN -jitter ELSE 0, ",
                "leaf' = [leaf EXCEPT "),
        ],
        ["Next"] = [("\\/ \\E l \\in Leaves : CallArrive(l) \\/ \\E l \\in Leaves : CallServe(l) ", "")],
    };

    private static string SchedulingFile(string name) =>
        File.ReadAllText(Path.Combine(SpecModuleCatalogue.RepositorySpecRoot, "scheduling", name));

    [Test]
    public void Witness_and_core_share_every_definition_apart_from_the_named_witness_and_core_additions()
    {
        var core = ReadDefinitions(SchedulingFile("WalGcStarvationDrive.tla"));
        var witness = ReadDefinitions(SchedulingFile("WalGcStarvationDriveWitness.tla"));

        Assert.Multiple(() =>
        {
            Assert.That(core.Keys.Except(witness.Keys), Is.EquivalentTo(CoreOnly));
            Assert.That(witness.Keys.Except(core.Keys), Is.EquivalentTo(WitnessOnly));
        });
    }

    [Test]
    public void Every_shared_definition_is_identical_or_differs_only_by_its_named_erasure()
    {
        var core = ReadDefinitions(SchedulingFile("WalGcStarvationDrive.tla"));
        var witness = ReadDefinitions(SchedulingFile("WalGcStarvationDriveWitness.tla"));
        var shared = core.Keys.Intersect(witness.Keys).Where(name => name != "Spec").ToList();

        Assert.That(shared, Has.Count.GreaterThan(30), "the definition reader matched too little to be meaningful");
        Assert.Multiple(() =>
        {
            foreach (var name in shared)
            {
                if (!Erasures.TryGetValue(name, out var edits))
                {
                    Assert.That(witness[name], Is.EqualTo(core[name]), $"{name} must be byte-identical (comments aside)");
                    continue;
                }

                Assert.That(Collapse(witness[name]), Is.Not.EqualTo(Collapse(core[name])),
                    $"{name} is listed as differing but is identical; remove its erasure");
                Assert.That(Erase(name, Collapse(witness[name]), edits), Is.EqualTo(Collapse(core[name])),
                    $"{name} differs from the core by more than its named erasure");
            }
        });
    }

    [Test]
    public void Witness_specification_is_the_core_fairness_plus_call_service_over_the_marked_relation()
    {
        var core = ReadDefinitions(SchedulingFile("WalGcStarvationDrive.tla"));
        var witness = ReadDefinitions(SchedulingFile("WalGcStarvationDriveWitness.tla"));

        var expected = "/\\ MarkedInit /\\ [][MarkedStep]_wvars "
            + Collapse(core["Fairness"]).Replace("WF_vars(ReArm(l))", "WF_vars(ReArm(l)) /\\ WF_vars(CallServe(l))");

        Assert.Multiple(() =>
        {
            Assert.That(Collapse(core["Spec"]), Is.EqualTo("Init /\\ [][Next]_vars /\\ Fairness"));
            Assert.That(Collapse(witness["Spec"]), Is.EqualTo(expected));
            Assert.That(Collapse(witness["MarkedInit"]), Is.EqualTo("Init /\\ fresh = TRUE"));
            Assert.That(Collapse(witness["MarkedStep"]), Is.EqualTo("Next /\\ fresh' = FALSE"));
            Assert.That(Collapse(witness["CoreInitRefined"]), Is.EqualTo("fresh => Core!Init"));
            Assert.That(witness["CoreInitRefined"], Does.Not.Contain("TLCGet"),
                "the initial-state obligation must read the modelled marker, not TLC's search depth");
        });
    }

    [Test]
    public void Witness_only_fields_are_touched_only_by_their_own_leaf_actions_and_the_mapping()
    {
        var witness = ReadDefinitions(SchedulingFile("WalGcStarvationDriveWitness.tla"));
        string[] allowed =
        [
            "wvars", "TypeOK", "Init", "TimerTick", "ReArm", "CallArrive", "CallServe", "Next",
            "TurnNotHeldAcrossDrive", "MarkedInit", "MarkedStep", "Spec", "CoreInitRefined",
        ];

        Assert.Multiple(() =>
        {
            foreach (var (name, body) in witness)
            {
                var touches = WitnessOnlyIdentifier.IsMatch(body);
                if (!allowed.Contains(name))
                    Assert.That(touches, Is.False, $"{name} reads or writes a witness-only field");
            }

            foreach (var action in new[] { "TimerTick", "ReArm", "CallArrive", "CallServe" })
            {
                var body = witness[action];
                Assert.That(Regex.Matches(body, @"leaf\[(?<i>[^\]]+)\]").Select(m => m.Groups["i"].Value).Distinct(),
                    Is.EqualTo(new[] { "l" }), $"{action} must read only its own leaf");
                Assert.That(Regex.Matches(body, @"!\[(?<i>[^\]]+)\]").Select(m => m.Groups["i"].Value).Distinct(),
                    Is.EqualTo(new[] { "l" }), $"{action} must write only its own leaf");
            }

            foreach (var action in new[] { "CallArrive", "CallServe" })
            {
                Assert.That(Regex.Matches(witness[action], @"!\[l\]\.(?<f>\w+)").Select(m => m.Groups["f"].Value),
                    Is.SubsetOf(new[] { "stall", "call" }).And.Not.Empty,
                    $"{action} must write only witness-only fields, so it projects to a core stutter");
                Assert.That(witness[action], Does.Contain(
                    "UNCHANGED <<free, cold, hold, head, appended, deleted, timely, admitted, leadDone, rank>>"),
                    $"{action} must leave every non-leaf variable unchanged");
            }

            Assert.That(witness["AbsLeaf"], Does.Not.Match(@"\b(stall|call)\b"),
                "the refinement mapping must forget the witness-only fields");
        });
    }

    [Test]
    public void Definition_reader_and_erasure_fail_loudly_on_text_they_do_not_recognise()
    {
        const string spec = "---- MODULE M ----\nVARIABLE x\nA == x' = 1\n\\* comment\nB(l) ==\n    /\\ x = l\n====\n";
        var definitions = ReadDefinitions(spec);

        Assert.Multiple(() =>
        {
            Assert.That(definitions.Keys, Is.EqualTo(new[] { "A", "B" }));
            Assert.That(definitions["B"], Is.EqualTo("B(l) ==\n    /\\ x = l"));
            Assert.Throws<AssertionException>(() => Erase("B", "B(l) == x = l", [("absent", "")]));
            Assert.Throws<AssertionException>(() => Erase("B", "a a", [("a", "")]));
        });
    }

    private static Dictionary<string, string> ReadDefinitions(string tla)
    {
        var definitions = new Dictionary<string, string>(StringComparer.Ordinal);
        string? current = null;
        var lines = new List<string>();

        void Flush()
        {
            if (current is not null)
                definitions.Add(current, string.Join("\n", lines));
            current = null;
            lines.Clear();
        }

        foreach (var line in SpecActions.StripComments(tla).Split('\n'))
        {
            var header = DefinitionHeader.Match(line);
            if (header.Success)
            {
                Flush();
                current = header.Groups["name"].Value;
                lines.Add(line);
            }
            else if (line.StartsWith("----", StringComparison.Ordinal) || line.StartsWith("====", StringComparison.Ordinal)
                || line.StartsWith("EXTENDS", StringComparison.Ordinal) || line.StartsWith("CONSTANT", StringComparison.Ordinal)
                || line.StartsWith("VARIABLE", StringComparison.Ordinal))
            {
                Flush();
            }
            else if (current is not null)
            {
                lines.Add(line);
            }
        }

        Flush();
        return definitions;
    }

    private static string Collapse(string text)
    {
        var body = text[(text.IndexOf("==", StringComparison.Ordinal) + 2)..];
        return Regex.Replace(body, @"\s+", " ").Trim();
    }

    private static string Erase(string name, string collapsed, (string Find, string Replace)[] edits)
    {
        var text = collapsed + " ";
        foreach (var (find, replace) in edits)
        {
            var count = Regex.Matches(text, Regex.Escape(find)).Count;
            Assert.That(count, Is.EqualTo(1), $"{name}: erasure '{find}' must occur exactly once");
            text = text.Replace(find, replace, StringComparison.Ordinal);
        }

        return text.Trim();
    }
}
