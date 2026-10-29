using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Pins the refinement index (contract C6 of #5148, item #5155) and writes it
/// to the file <see cref="RefinementIndex.EnvironmentVariable"/> names. The
/// content-gates job sets that variable and uploads the file as the
/// <c>refinement-index</c> artifact, which the formal-impact report (#5162)
/// reads without .NET.
/// <para>
/// These tests deliberately take no <see cref="SpecModule"/> parameter. A test
/// that does is a per-module gate (see <see cref="SpecModuleGates"/>) and is
/// enrolled in the discovery controls; the index is one artefact over every
/// module, so its vacuity guard checks the population itself instead.
/// </para>
/// </summary>
[TestFixture]
public sealed class RefinementIndexTests
{
    private const string ConsolidationGrain = "TreeShardConsolidationGrain";

    private static readonly Regex NamesConsolidationGrain = new(
        "`" + ConsolidationGrain + @"(?:\.[A-Za-z_][A-Za-z0-9_]*)?`",
        RegexOptions.CultureInvariant);

    private static readonly Lazy<RefinementSymbolResolver> Resolver = new(RefinementSymbolResolver.ForRepository);

    private static readonly Lazy<IReadOnlyList<RefinementIndexModule>> RepositoryIndex =
        new(() => RefinementIndex.Build(SpecModuleCatalogue.Repository(), Resolver.Value));

    private string _scratch = string.Empty;

    [SetUp]
    public void CreateScratch()
    {
        _scratch = Path.Combine(Path.GetTempPath(), "lattice-refinement-index-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_scratch);
    }

    [TearDown]
    public void DeleteScratch()
    {
        if (Directory.Exists(_scratch))
        {
            Directory.Delete(_scratch, recursive: true);
        }
    }

    [Test]
    public void Export_writes_the_index_to_the_file_the_environment_variable_names()
    {
        var json = RefinementIndex.ToJson(RepositoryIndex.Value);
        Assert.That(RefinementIndex.SchemaProblems(json), Is.Empty);

        var written = RefinementIndex.WriteIfRequested(json);
        var silent = RepositoryIndex.Value
            .SelectMany(module => module.Gaps.Where(gap => gap.Symbols.Count == 0).Select(gap => $"{module.Note}: {gap.Title}"))
            .ToArray();

        TestContext.Out.WriteLine(
            $"Refinement index: {RepositoryIndex.Value.Count} modules, "
            + $"{RepositoryIndex.Value.Sum(m => m.Rows.Count)} rows, {RepositoryIndex.Value.Sum(m => m.Gaps.Count)} gaps; "
            + (written is null ? $"{RefinementIndex.EnvironmentVariable} is unset, so nothing was written." : $"written to {written}."));
        TestContext.Out.WriteLine($"{silent.Length} abstraction gap(s) name no production symbol:");
        foreach (var gap in silent)
        {
            TestContext.Out.WriteLine("  " + gap);
        }

        if (written is not null)
        {
            Assert.That(File.ReadAllText(written), Is.EqualTo(json));
        }
    }

    [Test]
    public void Write_targets_the_named_file_and_writes_nothing_when_no_target_is_named()
    {
        var target = Path.Combine(_scratch, "nested", "refinement-index.json");

        Assert.Multiple(() =>
        {
            Assert.That(RefinementIndex.WriteTo("{}\n", null), Is.Null);
            Assert.That(RefinementIndex.WriteTo("{}\n", "  "), Is.Null);
            Assert.That(Directory.EnumerateFileSystemEntries(_scratch), Is.Empty, "an unset target writes nothing");

            Assert.That(RefinementIndex.WriteTo("{}\n", target), Is.EqualTo(Path.GetFullPath(target)));
            Assert.That(File.ReadAllBytes(target), Is.EqualTo("{}\n"u8.ToArray()), "UTF-8 without a byte-order mark");
        });
    }

    [Test]
    public void Index_covers_every_discovered_module_and_every_module_has_rows()
    {
        var discovered = SpecModuleCatalogue.Repository();

        Assert.That(discovered, Is.Not.Empty);
        Assert.That(VacuityProblems(discovered.Select(m => m.Name).ToArray(), RepositoryIndex.Value), Is.Empty);
    }

    [Test]
    public void Vacuity_guard_rejects_a_missing_module_a_module_without_rows_and_an_empty_index()
    {
        var discovered = SpecModuleCatalogue.Repository().Select(m => m.Name).ToArray();
        var index = RepositoryIndex.Value;
        var first = index[0];

        Assert.Multiple(() =>
        {
            Assert.That(VacuityProblems(discovered, index), Is.Empty, "control: the real index passes");
            Assert.That(VacuityProblems(discovered, index.Skip(1).ToArray()), Has.Some.Contains(first.Module));
            Assert.That(
                VacuityProblems(discovered, [first with { Rows = [] }, .. index.Skip(1)]),
                Has.Some.Contains(first.Module).And.Some.Contains("no rows"));
            Assert.That(VacuityProblems(discovered, []), Has.Some.Contains("no modules"));
            Assert.That(VacuityProblems([], []), Has.Some.Contains("no modules"));
        });
    }

    [Test]
    public void Index_serialises_to_the_contract_schema_and_records_every_mapping_section()
    {
        var index = RepositoryIndex.Value;
        var json = RefinementIndex.ToJson(index);

        Assert.Multiple(() =>
        {
            Assert.That(RefinementIndex.SchemaProblems(json), Is.Empty);
            foreach (var section in RefinementNote.MappingSections)
            {
                Assert.That(index.All(m => m.Rows.Any(r => r.Section == section)), Is.True, $"every module has '{section}' rows");
            }

            Assert.That(index.SelectMany(m => m.Rows).Any(r => r.Section == RefinementPropertyCoverageTests.ExclusionSection), Is.True,
                "some note excludes a property, so the exclusion section is exported");
            Assert.That(index.SelectMany(m => m.Rows).Any(r => r.Detectors.Count > 0), Is.True, "detectors are exported");
            Assert.That(index.SelectMany(m => m.Gaps).Any(g => g.Symbols.Count > 0), Is.True, "gap symbols are exported");
            Assert.That(index.SelectMany(m => m.Rows).SelectMany(r => r.Production).Intersect(
                index.SelectMany(m => m.Rows).SelectMany(r => r.Detectors)), Is.Empty,
                "no detector is reported as a production symbol");
        });
    }

    [Test]
    public void Index_is_byte_identical_across_independent_builds()
    {
        var again = RefinementIndex.Build(
            SpecModuleCatalogue.Repository().Reverse(),
            RefinementSymbolResolver.ForRepository());

        Assert.That(RefinementIndex.ToJson(again), Is.EqualTo(RefinementIndex.ToJson(RepositoryIndex.Value)));
    }

    /// <summary>
    /// The acceptance case of #5155: the shard-ownership notes name the fold
    /// coordinator, and every row and gap that names it in backticks lists it.
    /// </summary>
    [Test]
    public void Shard_ownership_index_lists_the_consolidation_grain_under_every_row_and_gap_that_names_it()
    {
        var modules = SpecModuleCatalogue.Repository()
            .Where(m => m.Describe(m.RefinementNotePath).StartsWith("spec/shard-ownership/", StringComparison.Ordinal))
            .ToArray();
        Assume.That(modules.Select(m => m.Describe(m.RefinementNotePath)), Does.Contain("spec/shard-ownership/Refinement.md"));

        var rowHits = 0;
        Assert.Multiple(() =>
        {
            foreach (var module in modules)
            {
                var entry = RepositoryIndex.Value.Single(e => e.Module == module.Name);
                var tables = module.ReadRefinementTables();

                var expectedRows = RefinementIndex.RowSections
                    .Where(tables.ContainsKey)
                    .SelectMany(section => tables[section].Rows.Select(row => (Row: row, Table: tables[section])))
                    .Where(x => NamesInProductionCells(x.Table, x.Row))
                    .Select(x => $"{x.Row.Section}|{x.Row.Label}")
                    .Order(StringComparer.Ordinal)
                    .ToArray();
                var listedRows = entry.Rows
                    .Where(r => r.Production.Any(IsConsolidationGrain))
                    .Select(r => $"{r.Section}|{r.Label}")
                    .Order(StringComparer.Ordinal)
                    .ToArray();
                Assert.That(listedRows, Is.EqualTo(expectedRows), module.Name + " rows");
                rowHits += listedRows.Length;

                var expectedGaps = RefinementIndex.ParseGapBlocks(module.ReadRefinementNote())
                    .Where(b => NamesConsolidationGrain.IsMatch(b.Text))
                    .Select(b => b.Title)
                    .Order(StringComparer.Ordinal)
                    .ToArray();
                var listedGaps = entry.Gaps
                    .Where(g => g.Symbols.Any(IsConsolidationGrain))
                    .Select(g => g.Title)
                    .Order(StringComparer.Ordinal)
                    .ToArray();
                Assert.That(listedGaps, Is.EqualTo(expectedGaps), module.Name + " gaps");
            }
        });

        var ownership = RepositoryIndex.Value.Single(e => e.Note == "spec/shard-ownership/Refinement.md");
        Assert.That(ownership.Rows.Where(r => r.Production.Any(IsConsolidationGrain)).Select(r => r.Label),
            Does.Contain("`FoldBegin`"), "the fold's opening action names the consolidation grain");
        Assert.That(rowHits, Is.GreaterThan(0));
    }

    [Test]
    public void Rows_carry_production_symbols_and_detectors_separately()
    {
        var resolver = ScratchResolver();
        var entry = RefinementIndex.BuildModule("Probe", "spec/probe/Refinement.md", ProbeNote(gaps: string.Empty), resolver);

        var step = entry.Rows.Single(r => r.Label == "`Step`");
        var excluded = entry.Rows.Single(r => r.Section == RefinementPropertyCoverageTests.ExclusionSection);

        Assert.Multiple(() =>
        {
            Assert.That(entry.Rows.Select(r => r.Section).Distinct(), Is.EqualTo(RefinementIndex.RowSections));
            Assert.That(step.Section, Is.EqualTo(RefinementNote.ActionSection));
            Assert.That(step.Production, Is.EqualTo(new[] { "FooGrain", "FooGrain.RunAsync", "ZedGrain.StopAsync" }),
                "dotted forms as the existing extractor reads them, plus a bare type that resolves; "
                + "not the label, not an unresolvable bare name");
            Assert.That(step.Detectors, Is.EqualTo(new[] { "FooGrainTests.Run_runs", "FooGrainTests.run_lower" }));
            Assert.That(excluded.Label, Is.EqualTo("`TypeOK`"));
            Assert.That(excluded.Detectors, Is.Empty);
            Assert.That(entry.Rows.Where(r => r.Section == RefinementNote.ActionSection).Select(r => r.Label),
                Is.EqualTo(new[] { "`Another`", "`Step`" }), "rows are ordered by label within a section");
        });
    }

    [Test]
    public void Gaps_take_bold_titles_resolvable_symbols_and_report_a_gap_that_names_none()
    {
        const string gaps = """
            ## Deliberate abstraction gaps

            What the module does not cover:

            - **Second gap.** Calls `FooGrain.RunAsync` and cites
              `FooGrainTests.Run_runs`, which is a test, not production.
            - **First gap.** Uses `FooGrain` and `BarCore`, but `Phase` is
            a spec name.

              A second paragraph of the same bullet names `ZedGrain.StopAsync`.
            - **Time.** No timers.

            A closing remark naming `FooGrain` is framing, not a gap.

            ## Next section

            - **Not a gap.** `FooGrain`.
            """;

        foreach (var lineEnding in new[] { "\n", "\r\n" })
        {
            var entry = RefinementIndex.BuildModule(
                "Probe", "spec/probe/Refinement.md", ProbeNote(gaps.ReplaceLineEndings(lineEnding)), ScratchResolver());

            Assert.That(entry.Gaps, Is.EqualTo(new[]
            {
                new RefinementIndexGap("First gap", ["BarCore", "FooGrain", "ZedGrain.StopAsync"]),
                new RefinementIndexGap("Second gap", ["FooGrain.RunAsync"]),
                new RefinementIndexGap("Time", []),
            }).Using<RefinementIndexGap>(GapEquals), lineEnding == "\n" ? "LF" : "CRLF");
        }
    }

    [Test]
    public void Gaps_written_as_paragraphs_are_one_entry_each_and_a_note_without_the_section_has_none()
    {
        const string paragraphs = """
            ## Deliberate abstraction gaps

            Finite cohort and sample-unit clocks. No `FooGrain` roll-up.
            Resolved callers are canonicalised.

            The model does not claim `BarCore` is partition local.
            """;

        var withProse = RefinementIndex.BuildModule("Probe", "spec/probe/Refinement.md", ProbeNote(paragraphs), ScratchResolver());
        var without = RefinementIndex.BuildModule("Probe", "spec/probe/Refinement.md", ProbeNote(string.Empty), ScratchResolver());

        Assert.Multiple(() =>
        {
            Assert.That(withProse.Gaps, Is.EqualTo(new[]
            {
                new RefinementIndexGap("Finite cohort and sample-unit clocks", ["FooGrain"]),
                new RefinementIndexGap("The model does not claim `BarCore` is partition local", ["BarCore"]),
            }).Using<RefinementIndexGap>(GapEquals));
            Assert.That(without.Gaps, Is.Empty);
        });
    }

    [Test]
    public void Schema_check_accepts_a_minimal_conforming_document()
    {
        Assert.That(RefinementIndex.SchemaProblems(MinimalDocument().ToJsonString()), Is.Empty);
    }

    [TestCase("not json", "not JSON")]
    [TestCase("[]", "$ must be an object")]
    [TestCase("schema=2", "$.schema")]
    [TestCase("schema=\"1\"", "$.schema")]
    [TestCase("drop=modules", "missing modules")]
    [TestCase("extra=$", "does not define: extra")]
    [TestCase("drop=module", "missing module")]
    [TestCase("drop=gaps", "missing gaps")]
    [TestCase("drop=detectors", "missing detectors")]
    [TestCase("drop=symbols", "missing symbols")]
    [TestCase("extra=row", "does not define: extra")]
    [TestCase("section=Coyote models of the cores", "is not one of")]
    [TestCase("production=unsorted", "production must be sorted")]
    [TestCase("detectors=duplicate", "detectors must be sorted ordinally without duplicates")]
    [TestCase("symbols=number", "symbols must hold only non-empty strings")]
    [TestCase("modules=unsorted", "$.modules[*].module must be sorted")]
    [TestCase("modules=duplicate", "$.modules[*].module must be sorted ordinally without duplicates")]
    [TestCase("rows=unsorted", "(by section, then label) must be sorted")]
    [TestCase("gaps=unsorted", "(by title) must be sorted")]
    [TestCase("label=empty", ".label must be a non-empty string")]
    public void Negative_control_schema_check_rejects_a_malformed_document(string defect, string expected)
    {
        Assert.That(RefinementIndex.SchemaProblems(Malformed(defect)), Has.Some.Contains(expected));
    }

    private static bool IsConsolidationGrain(string symbol) =>
        symbol == ConsolidationGrain || symbol.StartsWith(ConsolidationGrain + ".", StringComparison.Ordinal);

    private static bool NamesInProductionCells(RefinementTable table, RefinementRow row)
    {
        for (var column = 1; column < row.Cells.Count; column++)
        {
            var header = column < table.Headers.Count ? table.Headers[column] : string.Empty;
            if (!string.Equals(header, RefinementCodeSymbols.DetectorHeader, StringComparison.OrdinalIgnoreCase)
                && NamesConsolidationGrain.IsMatch(row.Cells[column]))
            {
                return true;
            }
        }

        return false;
    }

    private static bool GapEquals(RefinementIndexGap left, RefinementIndexGap right) =>
        left.Title == right.Title && left.Symbols.SequenceEqual(right.Symbols, StringComparer.Ordinal);

    /// <summary>
    /// Problems that would make the index check nothing: no modules, a
    /// discovered module the index omits, a module the index has that
    /// discovery does not, or a module with no rows.
    /// </summary>
    private static List<string> VacuityProblems(IReadOnlyList<string> discovered, IReadOnlyList<RefinementIndexModule> index)
    {
        var problems = new List<string>();
        if (discovered.Count == 0 || index.Count == 0)
        {
            problems.Add("the index or the discovered population has no modules, so the index checks nothing");
        }

        var indexed = index.Select(m => m.Module).ToHashSet(StringComparer.Ordinal);
        problems.AddRange(discovered.Where(name => !indexed.Contains(name))
            .Select(name => $"discovered module {name} is missing from the index"));
        problems.AddRange(indexed.Except(discovered, StringComparer.Ordinal)
            .Select(name => $"indexed module {name} is not a discovered module"));
        problems.AddRange(index.Where(m => m.Rows.Count == 0)
            .Select(m => $"module {m.Module} has no rows in the index"));
        return problems;
    }

    private RefinementSymbolResolver ScratchResolver()
    {
        File.WriteAllText(Path.Combine(_scratch, "FooGrain.cs"),
            "namespace P;\ninternal sealed class FooGrain\n{\n    public Task RunAsync() => Task.CompletedTask;\n}\n");
        File.WriteAllText(Path.Combine(_scratch, "ZedGrain.cs"),
            "namespace P;\ninternal sealed class ZedGrain\n{\n    public Task StopAsync() => Task.CompletedTask;\n}\n");
        File.WriteAllText(Path.Combine(_scratch, "Cores.cs"),
            "namespace P;\ninternal static class BarCore\n{\n}\n");
        return new RefinementSymbolResolver(_scratch);
    }

    private static string ProbeNote(string gaps) => $"""
        # Probe refinement

        ## Variable mapping

        | Spec variable | Code |
        |---------------|------|
        | `phase` | `FooGrain.RunAsync` state. |

        ## Action mapping

        | Spec action | Code | Detector |
        |-------------|------|----------|
        | `Step` | `FooGrain.RunAsync` on `FooGrain`, then `ZedGrain.StopAsync`; `Phase` is not a type. | Yes: `FooGrainTests.Run_runs`, `FooGrainTests.run_lower`. |
        | `Another` | Prose only. | None. |

        ## Property mapping

        | Spec property | Code-level property | Detector |
        |---------------|---------------------|----------|
        | `Safe` | Holds. | Yes: `FooGrainTests.Run_runs`. |

        ## Excluded properties

        | Spec property | Reason |
        |---------------|--------|
        | `TypeOK` | A domain constraint. |

        {gaps}
        """;

    private static JsonObject MinimalDocument() => new()
    {
        ["schema"] = 1,
        ["modules"] = new JsonArray(
            Module("Alpha", Row(RefinementNote.ActionSection, "`A`"), Row(RefinementNote.ActionSection, "`B`")),
            Module("Beta", Row(RefinementNote.VariableSection, "`v`"))),
    };

    private static JsonObject Module(string name, params JsonObject[] rows) => new()
    {
        ["module"] = name,
        ["note"] = $"spec/{name.ToLowerInvariant()}/Refinement.md",
        ["rows"] = new JsonArray(rows),
        ["gaps"] = new JsonArray(
            new JsonObject { ["title"] = "A gap", ["symbols"] = new JsonArray("FooGrain", "FooGrain.RunAsync") },
            new JsonObject { ["title"] = "B gap", ["symbols"] = new JsonArray() }),
    };

    private static JsonObject Row(string section, string label) => new()
    {
        ["section"] = section,
        ["label"] = label,
        ["production"] = new JsonArray("FooGrain", "FooGrain.RunAsync"),
        ["detectors"] = new JsonArray("FooGrainTests.Run_runs"),
    };

    private static string Malformed(string defect)
    {
        var document = MinimalDocument();
        var module = document["modules"]![0]!.AsObject();
        var row = module["rows"]![0]!.AsObject();
        var gap = module["gaps"]![0]!.AsObject();

        switch (defect)
        {
            case "not json": return "{ not json";
            case "[]": return "[]";
            case "schema=2": document["schema"] = 2; break;
            case "schema=\"1\"": document["schema"] = "1"; break;
            case "drop=modules": document.Remove("modules"); break;
            case "extra=$": document["extra"] = true; break;
            case "drop=module": module.Remove("module"); break;
            case "drop=gaps": module.Remove("gaps"); break;
            case "drop=detectors": row.Remove("detectors"); break;
            case "drop=symbols": gap.Remove("symbols"); break;
            case "extra=row": row["extra"] = 1; break;
            case "section=Coyote models of the cores": row["section"] = "Coyote models of the cores"; break;
            case "production=unsorted": row["production"] = new JsonArray("FooGrain.RunAsync", "FooGrain"); break;
            case "detectors=duplicate": row["detectors"] = new JsonArray("T.a", "T.a"); break;
            case "symbols=number": gap["symbols"] = new JsonArray(1); break;
            case "modules=unsorted": module["module"] = "Zulu"; break;
            case "modules=duplicate": module["module"] = "Beta"; break;
            case "rows=unsorted": row["label"] = "`Z`"; break;
            case "gaps=unsorted": gap["title"] = "Z gap"; break;
            case "label=empty": row["label"] = string.Empty; break;
            default: throw new ArgumentOutOfRangeException(nameof(defect), defect, "unknown defect");
        }

        return document.ToJsonString();
    }
}
