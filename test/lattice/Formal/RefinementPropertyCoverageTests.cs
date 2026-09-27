using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Every property TLC checks must have a refinement mapping or a reasoned
/// exclusion. Checks coverage, not the truth of the mapping or its detectors.
/// </summary>
[TestFixture]
public sealed class RefinementPropertyCoverageTests
{
    private const string ExclusionSection = "Excluded properties";

    private static string ReadConfig() =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(RefinementNote.NotePath)!, "AtomicCommit.cfg"));

    [Test]
    public void Every_checked_property_is_mapped_or_explicitly_excluded()
    {
        AssertCoverage(ReadConfig(), RefinementNote.ReadText());
    }

    [TestCase("\n")]
    [TestCase("\r\n")]
    public void Coverage_accepts_grouped_mapping_rows_and_reasoned_exclusions(string lineEnding)
    {
        AssertCoverage(
            ReadConfig().ReplaceLineEndings(lineEnding),
            RefinementNote.ReadText().ReplaceLineEndings(lineEnding));
    }

    [TestCase("CommitIntegrity")]
    [TestCase("TypeOK")]
    public void Coverage_rejects_a_removed_mapping_or_exclusion(string property)
    {
        var note = RefinementNote.ReadText();
        var row = RefinementNote.ParseTables(note).Values
            .SelectMany(t => t.Rows)
            .Single(r => r.Label == $"`{property}`");
        var lines = note.ReplaceLineEndings("\n").Split('\n').ToList();
        lines.RemoveAt(row.LineNumber - 1);

        var error = Assert.Throws<AssertionException>(
            () => AssertCoverage(ReadConfig(), string.Join("\n", lines)));

        Assert.That(error!.Message, Does.Contain(property));
    }

    [TestCase("INVARIANTS")]
    [TestCase("PROPERTIES")]
    [TestCase("INVARIANT")]
    [TestCase("PROPERTY")]
    public void Coverage_rejects_a_new_checked_property_even_if_prose_mentions_it(string directive)
    {
        const string property = "UnmappedProbe";
        var config = ReadConfig() + $"\n{directive}\n    {property}\n";
        var note = RefinementNote.ReadText() + $"\nProse mentions `{property}` without mapping it.\n";

        var error = Assert.Throws<AssertionException>(() => AssertCoverage(config, note));

        Assert.That(error!.Message, Does.Contain(property));
    }

    [TestCase("")]
    [TestCase("SPECIFICATION Spec\nCONSTANTS\n    t1 = t1")]
    [TestCase("\\* INVARIANTS\n\\* TypeOK\n\\* PROPERTIES\n\\* Termination")]
    [TestCase("INVARIANTS\nPROPERTIES\n")]
    public void Coverage_rejects_a_cfg_parse_with_zero_checked_properties(string config)
    {
        var error = Assert.Throws<AssertionException>(
            () => AssertCoverage(config, RefinementNote.ReadText()));

        Assert.That(error!.Message, Does.Contain("zero checked properties"));
    }

    [Test]
    public void Coverage_rejects_an_exclusion_without_a_reason()
    {
        var note = RefinementNote.ReadText();
        var row = RefinementNote.ParseTables(note)[ExclusionSection].Rows.Single();
        var lines = note.ReplaceLineEndings("\n").Split('\n');
        lines[row.LineNumber - 1] = $"| {row.Label} | |";

        var error = Assert.Throws<AssertionException>(
            () => AssertCoverage(ReadConfig(), string.Join("\n", lines)));

        Assert.That(error!.Message, Does.Contain("TypeOK").And.Contain("reason"));
    }

    [Test]
    public void Coverage_rejects_a_property_both_mapped_and_excluded()
    {
        var note = RefinementNote.ReadText().Replace(
            "| `TypeOK` |",
            "| `CommitIntegrity` |",
            StringComparison.Ordinal);

        var error = Assert.Throws<AssertionException>(() => AssertCoverage(ReadConfig(), note));

        Assert.That(error!.Message, Does.Contain("CommitIntegrity").And.Contain("both mapped and excluded"));
    }

    [Test]
    public void Cfg_reader_recognises_singular_plural_inline_and_commented_checks()
    {
        const string config = """
            SPECIFICATION Spec
            INVARIANT First \* an inline comment
            INVARIANTS Second Third
                Fourth
            PROPERTY Fifth
            PROPERTIES
                Sixth \* another inline comment
            CONSTANTS
                t1 = t1
            """;

        var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(config);

        Assert.Multiple(() =>
        {
            Assert.That(blocks[SpecMutationCatalogue.InvariantsBlock],
                Is.EqualTo(new[] { "First", "Second", "Third", "Fourth" }));
            Assert.That(blocks[SpecMutationCatalogue.PropertiesBlock],
                Is.EqualTo(new[] { "Fifth", "Sixth" }));
        });
    }

    private static void AssertCoverage(string config, string note)
    {
        var checkedProperties = SpecMutationCatalogue.ReadCheckedProperties(config);
        Assert.That(checkedProperties, Is.Not.Empty,
            "spec/AtomicCommit.cfg parsed to zero checked properties. Restore the INVARIANT(S) / "
            + "PROPERTY / PROPERTIES declarations or repair the parser; empty coverage is not success.");

        var tables = RefinementNote.ParseTables(note);
        var mapped = tables[RefinementNote.PropertySection].Rows
            .SelectMany(r => ReadNames(r.Label))
            .ToHashSet(StringComparer.Ordinal);
        var excluded = new HashSet<string>(StringComparer.Ordinal);

        if (tables.TryGetValue(ExclusionSection, out var exclusions))
        {
            Assert.That(exclusions.Headers, Is.EqualTo(new[] { "Spec property", "Reason" }),
                $"spec/Refinement.md's '{ExclusionSection}' table must name each property and its reason.");
            foreach (var row in exclusions.Rows)
            {
                Assert.That(row.Cells.Count == 2 && !string.IsNullOrWhiteSpace(row.Cells[1]), Is.True,
                    $"Excluded property {row.Label} at line {row.LineNumber} needs a non-empty reason.");
                excluded.UnionWith(ReadNames(row.Label));
            }
        }

        var overlap = mapped.Intersect(excluded).ToArray();
        Assert.That(overlap, Is.Empty,
            $"Properties cannot be both mapped and excluded: {string.Join(", ", overlap)}.");

        var missing = checkedProperties.Except(mapped).Except(excluded).ToArray();
        Assert.That(missing, Is.Empty,
            "spec/AtomicCommit.cfg checks properties with no mapping or reasoned exclusion in "
            + $"spec/Refinement.md: {string.Join(", ", missing)}.");
    }

    private static IEnumerable<string> ReadNames(string label)
    {
        Assert.That(Regex.IsMatch(label, @"^`[A-Za-z][A-Za-z0-9_]*`(\s*/\s*`[A-Za-z][A-Za-z0-9_]*`)*$"),
            Is.True, $"Invalid property label '{label}'; use backticked names separated by '/'.");
        return Regex.Matches(label, @"`([A-Za-z][A-Za-z0-9_]*)`")
            .Select(match => match.Groups[1].Value);
    }
}
