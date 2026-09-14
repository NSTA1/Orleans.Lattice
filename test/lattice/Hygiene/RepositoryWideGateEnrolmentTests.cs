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
    /// Whether a type name is this fixture's own, which must be excluded from every arm of
    /// the detector rather than from whichever arm was written first.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A detector necessarily contains the shape it detects, so this fixture matches itself
    /// on both the direct-scan arm (its remarks quote the canonical call) and the
    /// delegation arm (its remarks name the helper, and <c>MetricEmissionScanner.</c> in an
    /// English sentence satisfies a regex looking for a member access on that helper). The
    /// exclusion therefore lives behind one predicate used by both arms, because the arm
    /// that is added later is the one that will not get its own copy.
    /// </para>
    /// <para>
    /// That is not hypothetical: the delegation arm went uncovered when the first version
    /// of this fixture was written, and the defect surfaced only when a later edit happened
    /// to mention the helper in prose. A symbol named in a comment changing what a scanner
    /// concludes is the same class of fault this epic has been chasing elsewhere.
    /// </para>
    /// </remarks>
    private static bool IsSelfSource(string typeName) =>
        string.Equals(typeName, nameof(RepositoryWideGateEnrolmentTests), StringComparison.Ordinal);

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

    private sealed record Population(
        IReadOnlySet<string> ScanningFixtures,
        IReadOnlySet<string> NonFixtureScanners,
        int RawSiteCount,
        bool SelfExclusionFired,
        bool SelfExclusionFiredOnDelegationArm);

    /// <summary>
    /// Fixtures read by hand and confirmed to scan the whole of <c>src/</c>, each chosen
    /// because it is the <b>only</b> witness to one of the detector's rules. A rule that
    /// silently stops firing removes its anchor and nothing else, so the failure names the
    /// rule rather than a count.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the guard the non-emptiness assertions cannot be: the detector's first
    /// regex excluded parentheses from the first <c>Path.Combine</c> argument, which
    /// rejected every nested call and silently dropped two real scanners. The population
    /// it produced was clean, plausible, and <b>not empty</b> - it was quietly short, which
    /// is the one shape a vacuity guard passes by construction. Anchoring each rule to a
    /// named fixture converts that silence into a specific, readable failure.
    /// </para>
    /// </remarks>
    private static readonly IReadOnlyDictionary<string, string> KnownPositiveAnchors =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [nameof(SecurityInstructionsCoverageTests)] =
                "Its scan reads Path.Combine(HygieneRepository.FindRepoRoot(), \"src\"), so "
                    + "the first argument is a nested call. It is the witness for a detector "
                    + "pattern that admits a call expression there, and it is precisely the "
                    + "fixture an over-restrictive character class drops without trace.",
            [nameof(MeterFieldDeclarationOrderTests)] =
                "Its scan reads Path.Combine(root, \"src\") from a plain identifier, and it "
                    + "is the witness for the simple shape. It also sits outside the Hygiene "
                    + "namespace, so it anchors that the enumeration walks test/ rather than "
                    + "one namespace.",
            [nameof(InstrumentEmissionCoverageTests)] =
                "It contains no \"src\" literal at all and is reachable only through the "
                    + "one-hop delegation closure via MetricEmissionScanner. It is the sole "
                    + "witness that the closure runs; without it the closure could be inert "
                    + "and every set comparison would still pass.",
            ["BPlusLeafGrainTests"] =
                "Its scan lives in two partial files that carry no [TestFixture] attribute, "
                    + "while the attribute sits in eight other partials of the same type. It "
                    + "is the witness for keying the population by type rather than by file: "
                    + "file-keyed identity attributes the scan to a file that declares no "
                    + "fixture and drops it silently.",
            ["DashboardHistogramQuantileTests"] =
                "It lives in test/lattice.dashboards/, so it is the witness that enumeration "
                    + "is not confined to test/lattice/. That project is outside the "
                    + "Formal|Hygiene|Docs content-gate filter, which is the reason its rows "
                    + "belong in the table at all. Named as a string because it is in another "
                    + "assembly this project does not reference; that is safe here because an "
                    + "anchor is asserted present, so a rename fails this test loudly rather "
                    + "than unanchoring the rule in silence.",
        };

    /// <summary>
    /// Types that combine a path with <c>"src"</c> but must <b>not</b> be admitted, each
    /// chosen because it is the most plausible false positive for one exclusion rule.
    /// </summary>
    /// <remarks>
    /// An over-broad population is the failure mode that looks like thoroughness, so the
    /// exclusions need witnesses exactly as the inclusions do.
    /// </remarks>
    private static readonly IReadOnlyDictionary<string, string> KnownNegativeAnchors =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["HygieneRepository"] =
                "Its only \"src\" use is Directory.Exists(Path.Combine(dir.FullName, "
                    + "\"src\")), which locates the repository root rather than enumerating "
                    + "it. Admitting it would be the worst single false positive available: "
                    + "it lives in test/shared/ and every fixture that calls FindRepoRoot "
                    + "would follow it in.",
            [nameof(MetricEmissionScannerTests)] =
                "It creates a synthetic src/ directory under a temporary root and never "
                    + "calls FindRepoRoot. It is the witness for the real-root rule.",
            ["RepoContextGraphToolTests"] =
                "Its repoRoot is Path.Combine(workspace, repoName) over a temporary "
                    + "workspace. A mechanical scan for the combine shape reports it, so it "
                    + "is the false positive most likely to be admitted by a detector that "
                    + "drops the real-root rule.",
        };

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
            // Both arms of the exclusion are asserted to fire in Neither_population_is_vacuous,
            // so neither can quietly become a no-op.
            if (IsSelfSource(typeName))
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
        var selfExclusionFiredOnDelegationArm = false;
        var nonFixtureHelpers = helperScanners
            .Concat(directScanners)
            .Where(name => !fixtureTypes.Contains(name))
            .ToList();

        foreach (var (path, text) in textByPath)
        {
            var typeName = TypeNameForPath(path);

            // The sibling of the exclusion above. This fixture names the delegation helper
            // in its own remarks, and a member access on that name is what the closure
            // looks for, so without this arm the detector admits itself by reading its own
            // documentation of the rule it implements.
            if (IsSelfSource(typeName))
            {
                selfExclusionFiredOnDelegationArm = true;
                continue;
            }

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
            selfExclusionFired,
            selfExclusionFiredOnDelegationArm);
    }

    private sealed record DocumentedTable(
        IReadOnlyDictionary<string, string> Rows,
        int StatedTotal,
        int StatedScannerCount,
        int StatedRunCount);

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
        }

        var text = File.ReadAllText(path);

        return new DocumentedTable(
            rows,
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
                Is.True,
                "This fixture's own source no longer matches the detector pattern, so the "
                    + "self-exclusion in ComputePopulation is now a no-op. That is not "
                    + "harmless: the exclusion exists because a detector necessarily quotes "
                    + "the shape it detects, and its going quiet means either the pattern "
                    + "stopped matching the canonical call shape written in the remarks, or "
                    + "this fixture was renamed and the exclusion now names nothing. Both "
                    + "leave a live carve-out that no longer describes anything.");
            Assert.That(
                population.SelfExclusionFiredOnDelegationArm,
                Is.True,
                "The delegation arm of the self-exclusion did not fire. It is asserted "
                    + "separately from the direct-scan arm on purpose: the two are siblings, "
                    + "and this fixture admitted itself through the delegation arm the first "
                    + "time its remarks happened to name the helper, because only the "
                    + "direct-scan arm had an exclusion. One assertion covering 'the "
                    + "exclusion' would have stayed green through exactly that defect, which "
                    + "is why each arm is measured rather than counted.");
            Assert.That(
                KnownPositiveAnchors,
                Is.Not.Empty,
                "The known-positive anchor set is empty, so the anchor test passes without "
                    + "measuring a single detector rule. An anchor test with no anchors is "
                    + "the defect it was written to catch.");
            Assert.That(
                KnownNegativeAnchors,
                Is.Not.Empty,
                "The known-negative anchor set is empty, so no exclusion rule has a witness "
                    + "and an over-broad population would pass unremarked.");
        });
    }

    /// <summary>
    /// Measures the detector against results already known by hand: every rule has a named
    /// witness that must be admitted, and every exclusion a named witness that must not be.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The assertions above check that no population is <i>empty</i>. This one checks that
    /// each is <i>right</i>, and the difference is the whole reason it exists. A detector
    /// fault does not usually empty a population - it shortens one, quietly, while every
    /// count stays plausible and every set comparison still passes. Non-emptiness cannot
    /// see that. A known answer can.
    /// </para>
    /// <para>
    /// Each anchor therefore carries the rule it witnesses rather than merely a name, so a
    /// failure reports which rule stopped firing instead of leaving the reader to diff two
    /// lists of fixture names and infer it.
    /// </para>
    /// </remarks>
    [Test]
    public void Detector_reproduces_its_known_positive_and_negative_anchors()
    {
        var population = ComputePopulation();
        var admitted = new HashSet<string>(population.ScanningFixtures, StringComparer.Ordinal);
        admitted.UnionWith(population.NonFixtureScanners);

        Assert.Multiple(() =>
        {
            foreach (var (anchor, rule) in KnownPositiveAnchors.OrderBy(static a => a.Key, StringComparer.Ordinal))
            {
                Assert.That(
                    admitted,
                    Does.Contain(anchor),
                    $"Known-positive anchor '{anchor}' is no longer detected as a "
                        + "whole-src scanner, so the detector rule it witnesses has stopped "
                        + $"firing. {rule} Either the rule regressed and the computed "
                        + "population is now quietly short, or this fixture genuinely "
                        + "stopped scanning src and a replacement witness for that rule must "
                        + "be chosen here. Do not delete the anchor to go green: an anchor "
                        + "removed is a rule left unmeasured.");
            }

            foreach (var (anchor, rule) in KnownNegativeAnchors.OrderBy(static a => a.Key, StringComparer.Ordinal))
            {
                Assert.That(
                    admitted,
                    Does.Not.Contain(anchor),
                    $"Known-negative anchor '{anchor}' was admitted to the scanning "
                        + "population, so an exclusion rule has stopped firing and the "
                        + $"population is now over-broad. {rule} An over-broad population "
                        + "reads as thoroughness and inflates the table with fixtures that "
                        + "do not scan the repository, which is the same unexecuted-claim "
                        + "defect in the opposite direction.");
            }
        });
    }

    [Test]
    public void Documented_gate_table_matches_the_computed_repository_wide_population()    {
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
}
