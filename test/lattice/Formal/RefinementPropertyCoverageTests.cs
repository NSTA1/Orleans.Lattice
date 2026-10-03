using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Every property TLC checks must have a refinement mapping or a reasoned
/// exclusion, in every module. Checks coverage, not the truth of the mapping
/// or its detectors.
/// </summary>
[TestFixture]
public sealed class RefinementPropertyCoverageTests
{
    /// <summary>Heading of the refinement note's optional table of excluded properties.</summary>
    public const string ExclusionSection = "Excluded properties";

    private const string RemovedLabel = "RemovedCoverageProbe";

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_checked_property_is_mapped_or_explicitly_excluded(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        AssertCoverage(module, module.ReadConfig(), module.ReadRefinementNote());
    }

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Coverage_accepts_grouped_mapping_rows_and_reasoned_exclusions(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        foreach (var lineEnding in new[] { "\n", "\r\n" })
        {
            AssertCoverage(
                module,
                module.ReadConfig().ReplaceLineEndings(lineEnding),
                module.ReadRefinementNote().ReplaceLineEndings(lineEnding));
        }
    }

    /// <summary>
    /// Every single-name coverage row, mapped or excluded, is load-bearing:
    /// taking its property out of the note fails the gate and names the
    /// property. The row is relabelled rather than deleted, so a table holding
    /// only that row still parses and the failure is the coverage gate's own.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.CoveredProperties))]
    public void Coverage_rejects_a_removed_mapping_or_exclusion(SpecModule module, string property)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentException.ThrowIfNullOrEmpty(property);

        var note = module.ReadRefinementNote();
        var row = RefinementNote.ParseTables(note).Values
            .SelectMany(t => t.Rows)
            .Single(r => r.Label == $"`{property}`");
        var lines = note.ReplaceLineEndings("\n").Split('\n');
        lines[row.LineNumber - 1] = new Regex(Regex.Escape($"`{property}`")).Replace(lines[row.LineNumber - 1], $"`{RemovedLabel}`", 1);

        var error = Assert.Throws<AssertionException>(
            () => AssertCoverage(module, module.ReadConfig(), string.Join("\n", lines)));

        Assert.That(error!.Message, Does.Contain(property));
    }

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Coverage_rejects_a_new_checked_property_even_if_prose_mentions_it(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        const string property = "UnmappedProbe";
        var note = module.ReadRefinementNote() + $"\nProse mentions `{property}` without mapping it.\n";

        Assert.Multiple(() =>
        {
            foreach (var directive in new[] { "INVARIANTS", "PROPERTIES", "INVARIANT", "PROPERTY" })
            {
                var config = module.ReadConfig() + $"\n{directive}\n    {property}\n";
                var error = Assert.Throws<AssertionException>(() => AssertCoverage(module, config, note));
                Assert.That(error!.Message, Does.Contain(property), $"under {directive}");
            }
        });
    }

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Coverage_rejects_a_cfg_parse_with_zero_checked_properties(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        string[] configs =
        [
            "",
            "SPECIFICATION Spec\nCONSTANTS\n    t1 = t1",
            "\\* INVARIANTS\n\\* TypeOK\n\\* PROPERTIES\n\\* Termination",
            "INVARIANTS\nPROPERTIES\n",
        ];

        Assert.Multiple(() =>
        {
            foreach (var config in configs)
            {
                var error = Assert.Throws<AssertionException>(
                    () => AssertCoverage(module, config, module.ReadRefinementNote()));
                Assert.That(error!.Message, Does.Contain("zero checked properties"), $"for cfg '{config}'");
            }
        });
    }

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Coverage_rejects_an_exclusion_without_a_reason(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var property = FirstMappedProperty(module);
        var note = WithExclusion(module.ReadRefinementNote(), property, string.Empty);

        var error = Assert.Throws<AssertionException>(() => AssertCoverage(module, module.ReadConfig(), note));

        Assert.That(error!.Message, Does.Contain(property).And.Contain("reason"));
    }

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Coverage_rejects_a_property_both_mapped_and_excluded(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var property = FirstMappedProperty(module);
        var note = WithExclusion(module.ReadRefinementNote(), property, "a control's reason.");

        var error = Assert.Throws<AssertionException>(() => AssertCoverage(module, module.ReadConfig(), note));

        Assert.That(error!.Message, Does.Contain(property).And.Contain("both mapped and excluded"));
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

    private static string FirstMappedProperty(SpecModule module) =>
        module.ReadRefinementTables()[RefinementNote.PropertySection].Rows
            .Select(r => r.Label)
            .Where(l => Regex.IsMatch(l, @"^`[A-Za-z][A-Za-z0-9_]*`$"))
            .Select(l => l.Trim('`'))
            .FirstOrDefault()
        ?? throw new InvalidOperationException(
            $"{module.Describe(module.RefinementNotePath)} maps no property in a single-name row, so the "
            + "exclusion controls have no property to exclude.");

    /// <summary>
    /// Adds an exclusion row to <paramref name="note"/>: into its exclusion
    /// table when it has one, otherwise as a new section at its end.
    /// </summary>
    private static string WithExclusion(string note, string property, string reason)
    {
        var row = $"| `{property}` | {reason} |";
        var tables = RefinementNote.ParseTables(note);
        var lines = note.ReplaceLineEndings("\n").Split('\n').ToList();

        if (tables.TryGetValue(ExclusionSection, out var exclusions))
        {
            lines.Insert(exclusions.Rows[^1].LineNumber, row);
            return string.Join("\n", lines);
        }

        return string.Join("\n", lines)
            + $"\n\n## {ExclusionSection}\n\n| Spec property | Reason |\n|---------------|--------|\n{row}\n";
    }

    private static void AssertCoverage(SpecModule module, string config, string note)
    {
        var cfg = module.Describe(module.ConfigPath);
        var noteName = module.Describe(module.RefinementNotePath);

        var checkedProperties = SpecMutationCatalogue.ReadCheckedProperties(config);
        Assert.That(checkedProperties, Is.Not.Empty,
            $"{cfg} parsed to zero checked properties. Restore the INVARIANT(S) / "
            + "PROPERTY / PROPERTIES declarations or repair the parser; empty coverage is not success.");

        var tables = RefinementNote.ParseTables(note, noteName);
        var mapped = tables[RefinementNote.PropertySection].Rows
            .SelectMany(r => ReadNames(r.Label))
            .ToHashSet(StringComparer.Ordinal);
        var excluded = new HashSet<string>(StringComparer.Ordinal);

        if (tables.TryGetValue(ExclusionSection, out var exclusions))
        {
            Assert.That(exclusions.Headers, Is.EqualTo(new[] { "Spec property", "Reason" }),
                $"{noteName}'s '{ExclusionSection}' table must name each property and its reason.");
            foreach (var row in exclusions.Rows)
            {
                Assert.That(row.Cells.Count == 2 && !string.IsNullOrWhiteSpace(row.Cells[1]), Is.True,
                    $"Excluded property {row.Label} at {noteName} line {row.LineNumber} needs a non-empty reason.");
                excluded.UnionWith(ReadNames(row.Label));
            }
        }

        var overlap = mapped.Intersect(excluded).ToArray();
        Assert.That(overlap, Is.Empty,
            $"Properties cannot be both mapped and excluded: {string.Join(", ", overlap)}.");

        var missing = checkedProperties.Except(mapped).Except(excluded).ToArray();
        Assert.That(missing, Is.Empty,
            $"{cfg} checks properties with no mapping or reasoned exclusion in "
            + $"{noteName}: {string.Join(", ", missing)}.");
    }

    private static IEnumerable<string> ReadNames(string label)
    {
        Assert.That(Regex.IsMatch(label, @"^`[A-Za-z][A-Za-z0-9_]*`(\s*/\s*`[A-Za-z][A-Za-z0-9_]*`)*$"),
            Is.True, $"Invalid property label '{label}'; use backticked names separated by '/'.");
        return Regex.Matches(label, @"`([A-Za-z][A-Za-z0-9_]*)`")
            .Select(match => match.Groups[1].Value);
    }
}
