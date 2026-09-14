using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts that the repository-wide gate table in
/// <c>.github/instructions/testing.instructions.md</c> enumerates exactly the fixtures
/// that are actually repository-wide, so that the instruction a contributor follows
/// before raising a pull request cannot drift out from under the code it describes
/// (issue #2991).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this gate exists at all.</b> The table is the one place that tells a
/// contributor which gates a per-package pre-PR run is structurally blind to. It was
/// hand-authored, nothing executed it, and it was already wrong: it claimed five
/// fixtures when the mechanical population was larger, and it omitted two gates living
/// outside <c>test/lattice/</c> entirely. A stale list here is worse than no list,
/// because it reads as a complete inventory and is relied on as one.
/// </para>
/// <para>
/// <b>The claim being made executable is the count, not only the membership.</b> The
/// prose above the table states how many fixtures it lists and how many of those are
/// source scanners. Both numbers are parsed out of the prose and compared against the
/// table, so a row added without touching the sentence reddens, and a sentence edited
/// without touching the table reddens too. That is the specific failure this issue was
/// filed about: an English sentence asserting a population that nothing counts.
/// </para>
/// <para>
/// <b>There are two mechanisms for "repository-wide", and conflating them is what made
/// the original sentence false.</b> Most of these gates resolve the repository root and
/// enumerate all of <c>src/</c>. <see cref="RecordedNonScanningDocumentedGates"/> names
/// the exception: <c>DashboardJsonTests</c> is repository-wide by <i>reflection</i> over
/// the live meters and contains no <c>src</c> path at all. A gate that computed only
/// source scanners and compared that set to the table would report it as missing on
/// every run, forever. It is therefore recorded rather than detected, and the record is
/// itself checked below.
/// </para>
/// <para>
/// <b>Why a recorded set is not the defect this issue is about.</b> A hand-authored list
/// is only a hazard when nothing checks it. Every recorded set here is asserted against
/// the population it partitions: a name that stops being a source scanner, or a
/// documented row that becomes one, fails
/// <see cref="Recorded_exclusions_are_all_still_live_members_of_the_population"/>. The
/// lists shrink and grow with the tree instead of decaying quietly, which is exactly the
/// property the prose sentence lacked.
/// </para>
/// <para>
/// <b>Why the detector is deliberately broader than "instrument gates".</b> The table
/// scopes itself to the instrument concern, but "is this fixture about instruments" is
/// not computable from source. Narrowing the detector would require a hand-authored
/// exclusion list that nothing could check, which would reproduce the defect inside its
/// own remedy. So the detector finds <i>every</i> whole-<c>src/</c> scanning fixture and
/// the instrument partition is recorded in
/// <see cref="RecordedNonInstrumentScanners"/> with a reason per entry.
/// </para>
/// <para>
/// <b>Vacuity is the failure mode most likely to hide in a fixture like this.</b> A
/// parser that silently matches nothing, or a scan whose regex quietly stops matching,
/// passes green while asserting nothing at all - the same shape of unexecuted claim the
/// gate exists to prevent. <see cref="Neither_population_is_vacuous"/> therefore asserts
/// every intermediate population is non-empty, including the raw regex hit count, rather
/// than only the two sets being compared.
/// </para>
/// <para>
/// <b>The detector excludes its own source, and that exclusion is itself checked.</b> A
/// detector necessarily contains the pattern it detects: the remarks above quote the
/// canonical call shape and the failure messages repeat it, so this fixture matches its
/// own regex while scanning <c>test/</c> rather than <c>src/</c>. The exclusion is one
/// file wide and <see cref="Neither_population_is_vacuous"/> asserts it still fires, so a
/// rename or a pattern change cannot leave a live carve-out that no longer describes
/// anything.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepositoryWideGateEnrolmentTests
{
    private const string InstructionsPath = ".github/instructions/testing.instructions.md";

    /// <summary>
    /// Matches <c>Path.Combine(x, "src")</c> where <c>"src"</c> is the <b>terminal</b>
    /// segment, which is what distinguishes scanning the whole source tree from reaching
    /// into a single file beneath it.
    /// </summary>
    private static readonly Regex TerminalSrcCombine =
        new(@"Path\.Combine\(\s*[^,]+?\s*,\s*""src""\s*\)", RegexOptions.Compiled);

    /// <summary>
    /// Matches a <c>Directory.Exists(</c> / <c>File.Exists(</c> immediately preceding a
    /// combine, which uses <c>src</c> to <i>locate</i> the repository root rather than to
    /// scan it. <c>HygieneRepository.FindRepoRoot</c> does exactly this, and counting it
    /// would make every caller of that helper a false positive.
    /// </summary>
    private static readonly Regex ExistsGuard = new(@"Exists\(\s*$", RegexOptions.Compiled);

    /// <summary>
    /// Whole-<c>src/</c> scanning fixtures that are not part of the instrument concern the
    /// table scopes itself to. Each is excluded deliberately and with a reason; the
    /// exclusion is checked, so a name that stops scanning <c>src/</c> fails rather than
    /// lingering.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> RecordedNonInstrumentScanners =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["BPlusLeafGrainTests"] =
                "grain fixture; two replay-slice partials read option defaults out of src, not instruments",
            ["LatticeOptionsResolverPropagationGuardTests"] =
                "asserts options-resolver propagation across packages, not instrument declarations",
            ["PackageReleasePlumbingTests"] =
                "asserts per-package release plumbing (csproj/nuspec), not instrument declarations",
            ["ProcessorCountPoolSizingGuardTests"] =
                "asserts no raw ProcessorCount pool sizing across packages, not instrument declarations",
            ["SecurityInstructionsCoverageTests"] =
                "asserts the security instructions cover the packages they claim, not instruments",
            ["UnslicedPackageHygieneCoverageTests"] =
                "asserts every package is enrolled in slice hygiene, not instrument declarations",
        };

    /// <summary>
    /// Documented gates that are repository-wide by a mechanism other than scanning
    /// <c>src/</c>, and so are invisible to the detector by construction rather than by
    /// omission.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> RecordedNonScanningDocumentedGates =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["DashboardJsonTests"] =
                "repository-wide by reflection over the live meters; contains no src path at all",
        };

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    /// <summary>
    /// Derives the owning type name from the file path rather than by parsing the source.
    /// Every fixture in this repository lives in a file named after its type, and a
    /// partial carries the type name before the first dot. Parsing for an enclosing
    /// <c>class</c> token is unreliable here because the word also occurs in comments and
    /// strings, which silently attributes a scan to a word from prose.
    /// </summary>
    private static string TypeNameForPath(string path)
    {
        var fileName = Path.GetFileNameWithoutExtension(path);
        var firstDot = fileName.IndexOf('.');
        return firstDot < 0 ? fileName : fileName[..firstDot];
    }

    private static bool ScansWholeSourceTree(string text)
    {
        foreach (Match match in TerminalSrcCombine.Matches(text))
        {
            var lookBehindStart = Math.Max(0, match.Index - 24);
            var lookBehind = text[lookBehindStart..match.Index];
            if (!ExistsGuard.IsMatch(lookBehind))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<string> TestSourceFiles() =>
        HygieneRepository
            .EnumerateFiles(Path.Combine(RepoRoot, "test"), "*.cs")
            .Where(static p => !HygieneRepository.HasExcludedSegment(p))
            .ToList();

    /// <summary>
    /// All source files declaring the named fixture type, including partials. Resolution
    /// is by file name via <see cref="TypeNameForPath"/>, the same rule the population
    /// detector uses, so a fixture cannot be visible to one and invisible to the other.
    /// </summary>
    private static IReadOnlyList<string> SourceFilesForType(string typeName) =>
        TestSourceFiles()
            .Where(p => string.Equals(TypeNameForPath(p), typeName, StringComparison.Ordinal))
            .ToList();

    private sealed record Population(
        IReadOnlySet<string> ScanningFixtures,
        IReadOnlySet<string> NonFixtureScanners,
        int RawSiteCount,
        bool SelfExclusionFired);

    /// <summary>
    /// Computes the whole-<c>src/</c> scanning fixture population, closed over one hop of
    /// helper delegation. A helper is a type that scans <c>src/</c> from a root passed in
    /// as a parameter - <c>MetricEmissionScanner</c> is the live instance, and without the
    /// closure its caller <c>InstrumentEmissionCoverageTests</c> is invisible despite
    /// being one of the gates this table exists to name.
    /// </summary>
    private static Population ComputePopulation()
    {
        var files = TestSourceFiles();
        var textByPath = files.ToDictionary(static p => p, File.ReadAllText, StringComparer.Ordinal);

        var rawSiteCount = 0;
        var selfExclusionFired = false;
        var fixtureTypes = new HashSet<string>(StringComparer.Ordinal);
        var directScanners = new HashSet<string>(StringComparer.Ordinal);
        var helperScanners = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (path, text) in textByPath)
        {
            var typeName = TypeNameForPath(path);
            if (text.Contains("[TestFixture", StringComparison.Ordinal))
            {
                fixtureTypes.Add(typeName);
            }

            rawSiteCount += TerminalSrcCombine.Matches(text).Count;

            if (!ScansWholeSourceTree(text))
            {
                continue;
            }

            // A detector necessarily contains the pattern it detects: this fixture's own
            // remarks quote the canonical call shape, and its failure messages repeat it.
            // Excluding it is not a carve-out for an inconvenient result - it scans test/,
            // not src/, so every hit in this file is the detector reading its own prose.
            // The exclusion is asserted to still fire in Neither_population_is_vacuous, so
            // it cannot quietly become a no-op.
            if (string.Equals(typeName, nameof(RepositoryWideGateEnrolmentTests), StringComparison.Ordinal))
            {
                selfExclusionFired = true;
                continue;
            }

            // A file resolving the root itself scans the real tree; one taking the root as
            // a parameter is a helper, and may equally be a fixture driving a synthetic
            // workspace under a temporary directory.
            if (text.Contains("FindRepoRoot()", StringComparison.Ordinal))
            {
                directScanners.Add(typeName);
            }
            else
            {
                helperScanners.Add(typeName);
            }
        }

        var delegated = new HashSet<string>(StringComparer.Ordinal);
        var nonFixtureHelpers = helperScanners
            .Concat(directScanners)
            .Where(name => !fixtureTypes.Contains(name))
            .ToList();

        foreach (var (path, text) in textByPath)
        {
            var typeName = TypeNameForPath(path);
            if (directScanners.Contains(typeName) || !fixtureTypes.Contains(typeName))
            {
                continue;
            }

            if (!text.Contains("FindRepoRoot()", StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var helper in nonFixtureHelpers)
            {
                if (Regex.IsMatch(text, @"\b" + Regex.Escape(helper) + @"\s*\."))
                {
                    delegated.Add(typeName);
                    break;
                }
            }
        }

        var scanningFixtures = directScanners
            .Concat(delegated)
            .Where(fixtureTypes.Contains)
            .ToHashSet(StringComparer.Ordinal);

        return new Population(
            scanningFixtures,
            nonFixtureHelpers.ToHashSet(StringComparer.Ordinal),
            rawSiteCount,
            selfExclusionFired);
    }

    private sealed record DocumentedTable(
        IReadOnlyDictionary<string, string> Rows,
        IReadOnlyDictionary<string, string> Enrolments,
        int StatedTotal,
        int StatedScannerCount,
        int StatedRunCount);

    /// <summary>
    /// Matches a backtick-delimited span in the enrolment column. Backticks in that column
    /// are the contributor's declaration that the enclosed text is a code symbol rather
    /// than prose, which is what makes the cell's claim checkable at all.
    /// </summary>
    private static readonly Regex BacktickedSpan = new(@"`([^`]+)`", RegexOptions.Compiled);

    /// <summary>
    /// Extracts the <b>code anchors</b> of an enrolment cell: the backticked tokens that
    /// name a C# symbol. Bracketed attribute syntax and generic arity are stripped, so
    /// <c>[InstrumentedEnum]</c> and <c>Histogram&lt;T&gt;</c> reduce to the bare
    /// identifier. Anything that is not a single PascalCase word is deliberately ignored:
    /// paths (<c>src/</c>), meter names (<c>orleans.lattice</c>) and PromQL functions
    /// (<c>histogram_quantile</c>) are backticked in this column too and are not symbols
    /// in the fixture.
    /// </summary>
    private static IReadOnlyList<string> EnrolmentAnchors(string cell)
    {
        var anchors = new List<string>();
        foreach (Match match in BacktickedSpan.Matches(cell))
        {
            var token = match.Groups[1].Value.Trim().Trim('[', ']').Trim();
            var generic = token.IndexOf('<');
            if (generic >= 0)
            {
                token = token[..generic];
            }

            if (Regex.IsMatch(token, @"^[A-Z][A-Za-z0-9]*$") && !anchors.Contains(token, StringComparer.Ordinal))
            {
                anchors.Add(token);
            }
        }

        return anchors;
    }

    private static readonly IReadOnlyDictionary<string, int> NumberWords =
        new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["one"] = 1,
            ["two"] = 2,
            ["three"] = 3,
            ["four"] = 4,
            ["five"] = 5,
            ["six"] = 6,
            ["seven"] = 7,
            ["eight"] = 8,
            ["nine"] = 9,
            ["ten"] = 10,
            ["eleven"] = 11,
            ["twelve"] = 12,
            ["thirteen"] = 13,
            ["fourteen"] = 14,
            ["fifteen"] = 15,
            ["sixteen"] = 16,
            ["seventeen"] = 17,
            ["eighteen"] = 18,
            ["nineteen"] = 19,
            ["twenty"] = 20,
        };

    private static int ParseStatedNumber(string text, string pattern, string what)
    {
        var match = Regex.Match(text, pattern, RegexOptions.IgnoreCase);
        Assert.That(
            match.Success,
            Is.True,
            $"{InstructionsPath} no longer contains the sentence stating {what}. "
                + $"The gate cannot check a claim it cannot find, so the sentence must be "
                + $"restored or this pattern updated: /{pattern}/");

        var word = match.Groups["n"].Value;
        Assert.That(
            NumberWords.ContainsKey(word),
            Is.True,
            $"{InstructionsPath} states {what} as '{word}', which is not a number word this "
                + "gate recognises. Use a word between one and twenty.");

        return NumberWords[word];
    }

    private static DocumentedTable ReadDocumentedTable()
    {
        var path = Path.Combine(RepoRoot, InstructionsPath.Replace('/', Path.DirectorySeparatorChar));
        Assert.That(File.Exists(path), Is.True, $"{InstructionsPath} is missing.");

        var lines = File.ReadAllLines(path);
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        var enrolments = new Dictionary<string, string>(StringComparer.Ordinal);

        var headerIndex = Array.FindIndex(
            lines,
            static l => l.StartsWith("| fixture | project |", StringComparison.Ordinal));
        Assert.That(
            headerIndex,
            Is.GreaterThanOrEqualTo(0),
            $"{InstructionsPath} no longer contains the repository-wide gate table header "
                + "'| fixture | project |'. Without it this gate parses nothing and would "
                + "pass vacuously, so the header is treated as load-bearing.");

        for (var i = headerIndex + 1; i < lines.Length; i++)
        {
            var line = lines[i];
            if (!line.StartsWith("|", StringComparison.Ordinal))
            {
                break;
            }

            var cells = line.Split('|', StringSplitOptions.None);
            if (cells.Length < 3)
            {
                continue;
            }

            var fixture = cells[1].Trim().Trim('`').Trim();
            var project = cells[2].Trim().Trim('`').Trim().TrimEnd('/');
            if (fixture.Length == 0 || fixture.All(static c => c == '-'))
            {
                continue;
            }

            rows[fixture] = project;
            enrolments[fixture] = cells.Length > 3 ? cells[3].Trim() : string.Empty;
        }

        var text = File.ReadAllText(path);

        return new DocumentedTable(
            rows,
            enrolments,
            ParseStatedNumber(
                text,
                @"(?<n>\w+) fixtures below are repository-wide",
                "the total number of repository-wide gates"),
            ParseStatedNumber(
                text,
                @"(?<n>\w+) resolve the repository root and scan",
                "how many of those gates are source scanners"),
            ParseStatedNumber(
                text,
                @"must also run these (?<n>\w+)",
                "how many gates an instrument change must run"));
    }

    [Test]
    public void Neither_population_is_vacuous()
    {
        var population = ComputePopulation();
        var table = ReadDocumentedTable();

        Assert.Multiple(() =>
        {
            Assert.That(
                TestSourceFiles(),
                Is.Not.Empty,
                "No test sources were enumerated at all, so every assertion below would "
                    + "pass while checking nothing.");
            Assert.That(
                population.RawSiteCount,
                Is.GreaterThan(0),
                "The terminal Path.Combine(x, \"src\") pattern matched nothing anywhere "
                    + "under test/. Either the detector regex has stopped matching the "
                    + "source, or no gate scans src any more. Both invalidate this fixture, "
                    + "and a silent zero here is exactly the unexecuted-claim defect this "
                    + "gate was written to prevent.");
            Assert.That(
                population.ScanningFixtures,
                Is.Not.Empty,
                "No whole-src scanning fixture was detected, so the comparison below would "
                    + "be between two sets neither of which was computed.");
            Assert.That(
                population.NonFixtureScanners,
                Is.Not.Empty,
                "No non-fixture scanning helper was detected, so the one-hop delegation "
                    + "closure is inert and InstrumentEmissionCoverageTests would be "
                    + "silently absent from the population.");
            Assert.That(
                table.Rows,
                Is.Not.Empty,
                $"The gate table in {InstructionsPath} parsed to zero rows. A parse that "
                    + "matches nothing passes every set comparison, which is the defect "
                    + "this gate exists to prevent.");
            Assert.That(
                population.SelfExclusionFired,
                Is.True,                "This fixture's own source no longer matches the detector pattern, so the "
                    + "self-exclusion in ComputePopulation is now a no-op. That is not "
                    + "harmless: the exclusion exists because a detector necessarily quotes "
                    + "the shape it detects, and its going quiet means either the pattern "
                    + "stopped matching the canonical call shape written in the remarks, or "
                    + "this fixture was renamed and the exclusion now names nothing. Both "
                    + "leave a live carve-out that no longer describes anything.");
            Assert.That(
                table.Enrolments.Values.Count(static v => v.Length > 0),
                Is.EqualTo(table.Rows.Count),
                $"The third column of the {InstructionsPath} gate table did not parse for "
                    + "every row. The enrolment assertions all iterate that column, so a "
                    + "partial parse silently narrows them instead of failing.");
            Assert.That(
                table.Enrolments.Values.Count(static v => EnrolmentAnchors(v).Count > 0),
                Is.GreaterThan(0),
                "No enrolment cell yielded a code anchor, so "
                    + nameof(Enrolment_column_code_anchors_resolve_in_the_fixture_they_describe)
                    + " is now checking nothing. Either every backticked symbol was removed "
                    + "from the column, or the anchor extraction stopped matching the "
                    + "table's markup. A gate that cannot fail is worse here than no gate, "
                    + "because the surrounding enforcement lends the unchecked column "
                    + "credibility it has not earned.");
            Assert.That(
                table.Enrolments.Values.Count(static v => v.Contains("src/", StringComparison.Ordinal)),
                Is.GreaterThan(0),
                "No enrolment cell claims its gate reads src/, so "
                    + nameof(Enrolment_column_source_claims_match_the_computed_scanner_population)
                    + " now iterates an empty set and passes without comparing anything.");
        });
    }

    [Test]
    public void Documented_gate_table_matches_the_computed_repository_wide_population()
    {
        var population = ComputePopulation();
        var table = ReadDocumentedTable();

        var computed = population
            .ScanningFixtures
            .Where(static name => !RecordedNonInstrumentScanners.ContainsKey(name))
            .ToHashSet(StringComparer.Ordinal);

        var documented = table
            .Rows
            .Keys
            .Where(static name => !RecordedNonScanningDocumentedGates.ContainsKey(name))
            .ToHashSet(StringComparer.Ordinal);

        var undocumented = computed.Except(documented).OrderBy(static n => n, StringComparer.Ordinal).ToList();
        var phantom = documented.Except(computed).OrderBy(static n => n, StringComparer.Ordinal).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                undocumented,
                Is.Empty,
                "These fixtures scan all of src/ but are absent from the repository-wide "
                    + $"gate table in {InstructionsPath}, so a contributor following that "
                    + "table will not run them and will not know they exist. Add a row, or "
                    + "record the fixture in RecordedNonInstrumentScanners with a reason if "
                    + "it is genuinely outside the instrument concern: "
                    + string.Join(", ", undocumented));
            Assert.That(
                phantom,
                Is.Empty,
                $"These fixtures are listed in the {InstructionsPath} gate table but no "
                    + "longer scan all of src/. A row that names a gate which is not "
                    + "repository-wide inflates the apparent coverage of the table. Remove "
                    + "the row, or record it in RecordedNonScanningDocumentedGates if it is "
                    + "repository-wide by another mechanism: "
                    + string.Join(", ", phantom));
        });
    }

    [Test]
    public void Recorded_exclusions_are_all_still_live_members_of_the_population()
    {
        var population = ComputePopulation();
        var table = ReadDocumentedTable();

        var staleScannerRecords = RecordedNonInstrumentScanners
            .Keys
            .Where(name => !population.ScanningFixtures.Contains(name))
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToList();

        var staleDocumentedRecords = RecordedNonScanningDocumentedGates
            .Keys
            .Where(name => !table.Rows.ContainsKey(name))
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToList();

        var contradictoryRecords = RecordedNonScanningDocumentedGates
            .Keys
            .Where(population.ScanningFixtures.Contains)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                staleScannerRecords,
                Is.Empty,
                "These names are recorded as non-instrument src scanners but no longer scan "
                    + "all of src/. A recorded exclusion that outlives the thing it excludes "
                    + "is the same decayed-list defect this gate exists to prevent, so "
                    + "remove them from RecordedNonInstrumentScanners: "
                    + string.Join(", ", staleScannerRecords));
            Assert.That(
                staleDocumentedRecords,
                Is.Empty,
                "These names are recorded as documented-but-not-source-scanning yet are no "
                    + $"longer rows in the {InstructionsPath} table. Remove them from "
                    + "RecordedNonScanningDocumentedGates: "
                    + string.Join(", ", staleDocumentedRecords));
            Assert.That(
                contradictoryRecords,
                Is.Empty,
                "These names are recorded as repository-wide by a mechanism other than "
                    + "scanning src/, but the detector now finds them scanning src/. The "
                    + "record is contradicted by the code and must be removed so the gate "
                    + "checks them normally: "
                    + string.Join(", ", contradictoryRecords));
        });
    }

    [Test]
    public void Stated_gate_counts_match_the_table()
    {
        var table = ReadDocumentedTable();

        var total = table.Rows.Count;
        var scanners = table
            .Rows
            .Keys
            .Count(static name => !RecordedNonScanningDocumentedGates.ContainsKey(name));

        Assert.Multiple(() =>
        {
            Assert.That(
                table.StatedTotal,
                Is.EqualTo(total),
                $"{InstructionsPath} states the table lists {table.StatedTotal} "
                    + $"repository-wide fixtures, but it has {total} rows. This is the exact "
                    + "defect issue #2991 was filed about: a hand-written count above a "
                    + "hand-written list, with nothing comparing the two.");
            Assert.That(
                table.StatedScannerCount,
                Is.EqualTo(scanners),
                $"{InstructionsPath} states {table.StatedScannerCount} of the listed gates "
                    + $"resolve the repository root and scan all of src/, but {scanners} rows "
                    + "are source scanners.");
            Assert.That(
                table.StatedRunCount,
                Is.EqualTo(total),
                $"{InstructionsPath} tells a contributor to run {table.StatedRunCount} gates "
                    + $"after an instrument change, but the table lists {total}. The two "
                    + "numbers describe the same set and must agree.");
        });
    }

    [Test]
    public void Documented_project_column_matches_where_each_fixture_actually_lives()
    {
        var table = ReadDocumentedTable();
        var byTypeName = TestSourceFiles()
            .GroupBy(TypeNameForPath, StringComparer.Ordinal)
            .ToDictionary(static g => g.Key, static g => g.First(), StringComparer.Ordinal);

        var missing = new List<string>();
        var misfiled = new List<string>();

        foreach (var (fixture, project) in table.Rows.OrderBy(static r => r.Key, StringComparer.Ordinal))
        {
            if (!byTypeName.TryGetValue(fixture, out var path))
            {
                missing.Add(fixture);
                continue;
            }

            var relative = Path
                .GetRelativePath(RepoRoot, path)
                .Replace(Path.DirectorySeparatorChar, '/');
            var actualProject = string.Join('/', relative.Split('/').Take(2));
            if (!string.Equals(actualProject, project, StringComparison.Ordinal))
            {
                misfiled.Add($"{fixture} (documented {project}, actually {actualProject})");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                missing,
                Is.Empty,
                $"These {InstructionsPath} gate table rows name a fixture with no source "
                    + "file under test/. A row naming a fixture that does not exist tells a "
                    + "contributor to run something they cannot run: "
                    + string.Join(", ", missing));
            Assert.That(
                misfiled,
                Is.Empty,
                "These gate table rows name the wrong test project. The project column is "
                    + "what a contributor passes to dotnet test, so a wrong value produces a "
                    + "vacuous green rather than an error: "
                    + string.Join(", ", misfiled));
        });
    }

    /// <summary>
    /// The enrolment column's <b>code anchors</b> must name symbols that exist in the
    /// fixture the row describes.
    /// </summary>
    /// <remarks>
    /// The cell as a whole is prose and is not derivable from source - "what does this
    /// gate enrol" has no mechanical definition, which is precisely why the column was
    /// left unchecked when the other two were made executable (issue #3003). What <i>is</i>
    /// derivable is the part of the cell that rots silently: the symbols it names. A cell
    /// stating that a gate keys on <c>PlatformSentinelInstruments</c> is making a claim
    /// that a rename can falsify without touching the table, and nothing noticed.
    /// <para>
    /// This is deliberately narrower than the column's full meaning, and the gap is stated
    /// rather than papered over: a cell carrying no backticked symbol is not checked by
    /// this assertion at all. Claiming otherwise would be the same false credibility the
    /// gate exists to remove.
    /// </para>
    /// </remarks>
    [Test]
    public void Enrolment_column_code_anchors_resolve_in_the_fixture_they_describe()
    {
        var table = ReadDocumentedTable();
        var unresolved = new List<string>();

        foreach (var (fixture, cell) in table.Enrolments.OrderBy(static r => r.Key, StringComparer.Ordinal))
        {
            var anchors = EnrolmentAnchors(cell);
            if (anchors.Count == 0)
            {
                continue;
            }

            var sources = SourceFilesForType(fixture);
            if (sources.Count == 0)
            {
                unresolved.Add($"{fixture} (no source file, so no anchor can resolve)");
                continue;
            }

            var text = string.Join("\n", sources.Select(File.ReadAllText));
            foreach (var anchor in anchors)
            {
                if (!Regex.IsMatch(text, @"\b" + Regex.Escape(anchor) + @"\b"))
                {
                    unresolved.Add($"{fixture} -> `{anchor}`");
                }
            }
        }

        Assert.That(
            unresolved,
            Is.Empty,
            "These enrolment-column cells name a code symbol that does not appear in the "
                + "fixture they describe. Backticks in that column declare the enclosed "
                + "text to be a symbol of that fixture, so an unresolved anchor means the "
                + "description has drifted from the code - the row still reads as an "
                + "accurate account of what the gate enrols while naming something that no "
                + "longer exists. Either update the cell to the current name, or unbacktick "
                + "the word if it was meant as prose: "
                + string.Join(", ", unresolved));
    }

    /// <summary>
    /// A cell claiming its gate reads <c>src/</c> must name a fixture the detector
    /// actually found scanning the whole source tree.
    /// </summary>
    /// <remarks>
    /// Asserted in one direction only, and the asymmetry is real rather than an oversight:
    /// most rows are source scanners without saying so, so requiring the converse would
    /// fail on seven correct rows today. The direction that is checkable is the one that
    /// misleads - describing a gate as reading <c>src/</c> when it does not. That is
    /// exactly the confusion that made the original prose false about
    /// <c>DashboardJsonTests</c>, which is repository-wide by reflection over the live
    /// meters and reads no source at all.
    /// </remarks>
    [Test]
    public void Enrolment_column_source_claims_match_the_computed_scanner_population()
    {
        var population = ComputePopulation();
        var table = ReadDocumentedTable();

        var falseClaims = table
            .Enrolments
            .Where(static r => r.Value.Contains("src/", StringComparison.Ordinal))
            .Where(r => !population.ScanningFixtures.Contains(r.Key))
            .Select(static r => r.Key)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToList();

        Assert.That(
            falseClaims,
            Is.Empty,
            "These enrolment-column cells say the gate reads src/, but the detector does "
                + "not find them scanning the source tree. A row that describes a "
                + "reflection-based or data-driven gate as a source scanner sends a "
                + "contributor looking for the wrong kind of breakage: "
                + string.Join(", ", falseClaims));
    }

    /// <summary>
    /// Every row carries a distinct, non-empty enrolment description.
    /// </summary>
    /// <remarks>
    /// The weakest of the three checks and the only one covering all rows. It catches the
    /// realistic authoring error the other two cannot: a row added by copying an adjacent
    /// one and editing only the fixture name, which yields a cell that is confidently
    /// wrong and whose anchors, being the neighbour's, all resolve.
    /// </remarks>
    [Test]
    public void Enrolment_column_descriptions_are_present_and_distinct()
    {
        var table = ReadDocumentedTable();

        var empty = table
            .Enrolments
            .Where(static r => r.Value.Length == 0)
            .Select(static r => r.Key)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToList();

        var duplicated = table
            .Enrolments
            .GroupBy(static r => r.Value, StringComparer.Ordinal)
            .Where(static g => g.Count() > 1)
            .Select(static g => string.Join(" == ", g.Select(static r => r.Key).OrderBy(static n => n, StringComparer.Ordinal)))
            .OrderBy(static s => s, StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                empty,
                Is.Empty,
                "These gate table rows have an empty third cell. The column is the only "
                    + "statement of what the gate actually enrols, and a blank one leaves "
                    + "the row naming a fixture a contributor has no reason to run: "
                    + string.Join(", ", empty));
            Assert.That(
                duplicated,
                Is.Empty,
                "These gate table rows share an identical enrolment description, which "
                    + "means at least one of them describes a gate other than its own: "
                    + string.Join("; ", duplicated));
        });
    }
}
