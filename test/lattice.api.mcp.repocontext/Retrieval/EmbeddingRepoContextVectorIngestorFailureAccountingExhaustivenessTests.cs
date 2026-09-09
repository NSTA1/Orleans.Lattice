using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the vector-ingest failure accounting against a failure path that is
/// counted in the total but attributed to no stage (issue #2403).
/// <para>
/// <b>The invariant.</b> <c>EmbedAndStoreReportingLandedAsync</c> keeps a total,
/// <c>failedBatches</c>, and three stage components - <c>embedFailedBatches</c>,
/// <c>storeFailedBatches</c>, <c>recordFailedBatches</c> - and publishes a pass
/// census whose success figure is computed as
/// <c>attemptedBatches - failedBatches</c>. The census is therefore internally
/// consistent only while the total equals the sum of the components. The three
/// components are what an operator uses to attribute a loss to a stage; if they
/// stop summing to the total the attribution is wrong INVISIBLY, because the
/// total still looks right and each component still looks plausible on its own.
/// </para>
/// <para>
/// <b>Why this fixture is structural rather than behavioural.</b> The companion
/// fixture
/// <see cref="EmbeddingRepoContextVectorIngestorFailureAccountingInvariantTests"/>
/// drives every failure path that exists today and asserts the identity holds.
/// That is necessary and it is not sufficient: the risk this item was filed over
/// is a FOURTH failure path added later that increments the total and no
/// component. No test written today exercises a path that does not exist yet, so
/// a purely behavioural guard passes forever and catches exactly nothing - a
/// vacuous guard. This fixture reads the source instead, so a new unattributed
/// failure path fails the build whether or not any test drives it.
/// </para>
/// <para>
/// The idiom is the repository's own: <c>MeterFieldDeclarationOrderTests</c>
/// scans <c>src/</c> for a load-bearing declaration-order invariant, fails loudly
/// when its own scan matches nothing, and carries synthetic self-tests proving
/// its detector detects. The same three parts are here.
/// </para>
/// <para>
/// <b>What is and is not caught, stated plainly.</b> A total incremented with no
/// component is caught COMPLETELY and positionally, wherever the new site is
/// inserted, because the rule looks forward from each total to the next one. The
/// converse - a component incremented with no total - is caught by an ADJACENCY
/// CONVENTION (a component increment must sit within a few lines of the total it
/// belongs to) rather than positionally, and behaviourally by the companion
/// fixture whenever the path is exercised. The first direction is the one the
/// item was filed over and it is the one held exactly.
/// </para>
/// </summary>
/// <remarks>
/// A pure source scan: no silo, no harness, so it stays in the fast unit loop and
/// carries no <c>Integration</c> category.
/// </remarks>
[TestFixture]
public sealed class EmbeddingRepoContextVectorIngestorFailureAccountingExhaustivenessTests
{
    /// <summary>
    /// The source file carrying the accounting. Relative to the repository root so
    /// the scan names the thing to fix when the file moves.
    /// </summary>
    private const string IngestorRelativePath =
        "src/lattice.api.mcp.repocontext/Retrieval/EmbeddingRepoContextVectorIngestor.cs";

    private const string MethodSignatureToken = "EmbedAndStoreReportingLandedAsync(";

    private const string CensusToken = "arm pass census";

    /// <summary>
    /// How many lines a component increment may sit below the total increment it
    /// belongs to. The two real sites use 1 line (the embed path) and 7 lines (the
    /// store/record path, whose components sit in the arms of an if/else), so the
    /// window is loose enough for the shapes the code actually takes and tight
    /// enough that a component accounted somewhere unrelated is flagged.
    /// </summary>
    private const int ComponentAdjacencyWindowLines = 12;

    /// <summary>The total counter's increment: <c>failedBatches++</c> and nothing else.</summary>
    /// <remarks>
    /// Case sensitivity is load-bearing. The components are spelled
    /// <c>embedFailedBatches</c> with a capital F, so this pattern cannot match one
    /// of them, and <c>consecutiveBatchFailures++</c> shares no substring with it.
    /// </remarks>
    private static readonly Regex TotalIncrement = new(
        @"(?<![\w.])failedBatches\+\+", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A stage component's increment, for example <c>embedFailedBatches++</c>.</summary>
    private static readonly Regex ComponentIncrement = new(
        @"(?<![\w.])([A-Za-z_]\w*FailedBatches)\+\+", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A stage component's declaration, for example <c>var embedFailedBatches = 0;</c>.</summary>
    private static readonly Regex ComponentDeclaration = new(
        @"(?<![\w.])var\s+([A-Za-z_]\w*FailedBatches)\s*=\s*0\s*;",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The total counter's declaration.</summary>
    private static readonly Regex TotalDeclaration = new(
        @"(?<![\w.])var\s+failedBatches\s*=\s*0\s*;",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A per-stage placeholder in the census template, for example <c>{EmbedFailed}</c>.</summary>
    private static readonly Regex CensusFailurePlaceholder = new(
        @"\{\w*Failed\}", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Reads the accounting method's body from the real source file.
    /// </summary>
    private static IReadOnlyList<string> ReadMethodBody()
    {
        var root = HygieneRepository.FindRepoRoot();
        var path = Path.Combine(root, IngestorRelativePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True,
            $"The vector ingestor is no longer at {IngestorRelativePath}, so this guard scanned nothing. "
            + "Point it at the file that now carries the failure accounting rather than deleting it.");

        return ExtractMethodBody(File.ReadAllLines(path));
    }

    /// <summary>
    /// Slices the accounting method out of a file's lines, from its signature to
    /// its closing brace.
    /// </summary>
    /// <remarks>
    /// The method's closing brace is the first line that is exactly four spaces and
    /// a brace: members sit at that indentation and every block inside the method -
    /// including its local function - closes deeper. Brace counting is deliberately
    /// avoided because the method is dense with logging templates whose
    /// placeholders are braces inside string literals. The callers assert that the
    /// slice contains the declarations and the census, so a mis-slice fails loudly
    /// instead of silently scanning an empty region.
    /// </remarks>
    private static IReadOnlyList<string> ExtractMethodBody(IReadOnlyList<string> lines)
    {
        var start = -1;
        for (var i = 0; i < lines.Count; i++)
        {
            if (lines[i].Contains(MethodSignatureToken, StringComparison.Ordinal)
                && lines[i].Contains("private", StringComparison.Ordinal))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return Array.Empty<string>();
        }

        for (var i = start + 1; i < lines.Count; i++)
        {
            if (lines[i] == "    }")
            {
                return lines.Skip(start).Take(i - start + 1).ToList();
            }
        }

        return lines.Skip(start).ToList();
    }

    /// <summary>
    /// Returns the 1-based body line of every total increment that is not followed,
    /// before the next total increment, by at least one stage component increment.
    /// </summary>
    /// <remarks>
    /// The window looks FORWARD only, and that is the whole point rather than an
    /// implementation detail. A window that also looked backward would let the
    /// PREVIOUS path's component increment - which sits just below its own total -
    /// satisfy a newly inserted unattributed total, which is a false negative on
    /// precisely the change this guard exists to catch.
    /// </remarks>
    internal static IReadOnlyList<int> FindTotalsWithNoComponent(IReadOnlyList<string> body)
    {
        var totals = LinesMatching(body, TotalIncrement);
        var components = LinesMatching(body, ComponentIncrement);
        var unpaired = new List<int>();

        for (var i = 0; i < totals.Count; i++)
        {
            var from = totals[i];
            var to = i + 1 < totals.Count ? totals[i + 1] : int.MaxValue;
            if (!components.Any(c => c > from && c < to))
            {
                unpaired.Add(from);
            }
        }

        return unpaired;
    }

    /// <summary>
    /// Returns the 1-based body line of every stage component increment that has no
    /// total increment within <see cref="ComponentAdjacencyWindowLines"/> lines
    /// above it, so a stage counted without the total being counted is visible.
    /// </summary>
    internal static IReadOnlyList<int> FindComponentsWithNoTotal(IReadOnlyList<string> body)
    {
        var totals = LinesMatching(body, TotalIncrement);
        var orphans = new List<int>();

        foreach (var component in LinesMatching(body, ComponentIncrement))
        {
            if (!totals.Any(t => t <= component && component - t <= ComponentAdjacencyWindowLines))
            {
                orphans.Add(component);
            }
        }

        return orphans;
    }

    private static List<int> LinesMatching(IReadOnlyList<string> body, Regex pattern)
    {
        var lines = new List<int>();
        for (var i = 0; i < body.Count; i++)
        {
            if (pattern.IsMatch(body[i]))
            {
                lines.Add(i + 1);
            }
        }

        return lines;
    }

    private static List<string> DeclaredComponents(IReadOnlyList<string> body)
        => body
            .SelectMany(line => ComponentDeclaration.Matches(line).Select(m => m.Groups[1].Value))
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// The census logging call, from its template line to the end of its argument
    /// list, joined into one string.
    /// </summary>
    private static string CensusInvocation(IReadOnlyList<string> body)
    {
        var start = -1;
        for (var i = 0; i < body.Count; i++)
        {
            if (body[i].Contains(CensusToken, StringComparison.Ordinal))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return string.Empty;
        }

        for (var i = start; i < body.Count; i++)
        {
            if (body[i].TrimEnd().EndsWith(");", StringComparison.Ordinal))
            {
                return string.Join("\n", body.Skip(start).Take(i - start + 1));
            }
        }

        return string.Join("\n", body.Skip(start));
    }

    /// <summary>
    /// The scan's own denominator. Everything below asserts about a slice of source;
    /// if the slice is empty or has drifted away from the shapes these patterns
    /// recognise, every other assertion here passes for the wrong reason.
    /// </summary>
    [Test]
    public void The_failure_accounting_scan_finds_the_counters_it_claims_to_check()
    {
        var body = ReadMethodBody();
        var totals = LinesMatching(body, TotalIncrement);
        var components = DeclaredComponents(body);

        HygieneDenominator.RequireExamined(
            body.Count,
            "vector-ingest failure accounting exhaustiveness",
            "source lines",
            IngestorRelativePath + " :: EmbedAndStoreReportingLandedAsync");

        Assert.Multiple(() =>
        {
            Assert.That(body.Any(l => TotalDeclaration.IsMatch(l)), Is.True,
                "The scan did not find the 'var failedBatches = 0;' declaration. Either the method was "
                + "sliced wrongly or the total counter was renamed; in both cases every other assertion "
                + "in this fixture is now vacuous. Fix the patterns, do not delete the guard.");

            Assert.That(totals, Is.Not.Empty,
                "The scan found no 'failedBatches++' site at all, so the pairing rule below would report "
                + "a clean result no matter what the source said.");

            Assert.That(components, Has.Count.GreaterThanOrEqualTo(3),
                "The accounting is documented to attribute a batch loss to one of three stages - embed, "
                + "store, and record. The scan found: "
                + (components.Count == 0 ? "(none)" : string.Join(", ", components))
                + ". Fewer than three means either a stage counter was removed, which loses an "
                + "operator's ability to attribute a failure, or the declaration pattern has drifted.");

            Assert.That(CensusInvocation(body), Is.Not.Empty,
                "The scan did not find the pass census logging call, which is the operator-visible "
                + "surface the whole invariant exists to keep self-consistent.");
        });
    }

    /// <summary>
    /// The guard the item was filed for: a failure path counted in the total and
    /// attributed to no stage. This is what fails when a FOURTH path is added later,
    /// without that path needing to be exercised by any test.
    /// </summary>
    [Test]
    public void Every_failure_accounting_total_increment_also_attributes_the_loss_to_a_stage()
    {
        var body = ReadMethodBody();

        var unpaired = FindTotalsWithNoComponent(body);

        Assert.That(unpaired, Is.Empty,
            "A 'failedBatches++' is not followed by any stage component increment before the next one, "
            + "at body line(s) " + string.Join(", ", unpaired) + " of EmbedAndStoreReportingLandedAsync.\n\n"
            + "That is a batch loss counted in the total and attributed to NO stage. The pass census "
            + "reports success as 'attemptedBatches - failedBatches', so the total stays right and each "
            + "stage component stays plausible while their sum no longer reconciles - an operator "
            + "attributing the loss to a stage is then silently wrong, with no symptom to notice. This "
            + "is exactly the drift issue #2403 was filed over.\n\n"
            + "Increment the stage component that owns the new path directly below the total, and add "
            + "the stage to the census line so the loss reaches an operator. If the new path genuinely "
            + "belongs to no existing stage, add a fourth component rather than leaving the loss "
            + "unattributed.");
    }

    /// <summary>
    /// The converse drift: a stage counted while the total is not, which makes the
    /// census over-report success. Held by adjacency rather than positionally, so
    /// the convention it enforces is that a stage is accounted next to its total.
    /// </summary>
    [Test]
    public void Every_failure_accounting_stage_increment_belongs_to_a_total_increment()
    {
        var body = ReadMethodBody();

        var orphans = FindComponentsWithNoTotal(body);

        Assert.That(orphans, Is.Empty,
            "A stage component increment at body line(s) " + string.Join(", ", orphans)
            + " has no 'failedBatches++' within " + ComponentAdjacencyWindowLines
            + " lines above it.\n\nA stage counted while the total is not makes the census OVER-report "
            + "success, because success is computed as 'attemptedBatches - failedBatches'. Account the "
            + "stage directly below the total it belongs to, which is the convention both existing "
            + "failure paths follow.");
    }

    /// <summary>
    /// The other half of attribution: a stage that is counted but never reported is
    /// invisible to the operator who needs it, and a census that reports fewer
    /// stages than exist silently loses one. This is what catches a fourth stage
    /// that IS accounted internally but never reaches the log line.
    /// </summary>
    [Test]
    public void Every_failure_accounting_stage_reaches_the_operator_visible_census()
    {
        var body = ReadMethodBody();
        var components = DeclaredComponents(body);
        var census = CensusInvocation(body);

        var unreported = components
            .Where(c => !Regex.IsMatch(census, @"(?<![\w.])" + Regex.Escape(c) + @"(?![\w])"))
            .ToList();

        var placeholders = CensusFailurePlaceholder.Matches(census).Count;

        Assert.Multiple(() =>
        {
            Assert.That(unreported, Is.Empty,
                "Stage counter(s) " + string.Join(", ", unreported) + " are maintained but never passed "
                + "to the pass census, so the loss they count never reaches an operator. The census is "
                + "the only place these figures are published; a counter that does not appear in it is "
                + "accounting nobody can read.");

            Assert.That(placeholders, Is.EqualTo(components.Count),
                $"The census template carries {placeholders} per-stage placeholder(s) but the method "
                + $"maintains {components.Count} stage counter(s) ({string.Join(", ", components)}). "
                + "A stage without a placeholder is dropped from the operator's view even though it is "
                + "passed as an argument, and the surplus arguments silently shift the remaining "
                + "placeholders onto the wrong values.");
        });
    }

    /// <summary>
    /// Proves the forward-looking pairing rule actually flags an unattributed total,
    /// on synthetic source. Without this the rule could be broken - a pattern that
    /// matches nothing reports no violations - and every arm above would pass while
    /// guarding nothing.
    /// </summary>
    [Test]
    public void The_failure_accounting_pairing_rule_flags_an_unattributed_total_wherever_it_is_added()
    {
        var attributed = new[]
        {
            "        var failedBatches = 0;",
            "        var embedFailedBatches = 0;",
            "                failedBatches++;",
            "                embedFailedBatches++;",
            "                failedBatches++;",
            "                storeFailedBatches++;",
        };

        // The same source with a fourth path added in each of the three positions a
        // new path can occupy relative to the existing ones. All three must be
        // flagged: a rule that only caught the trailing case would miss a path
        // inserted between two correctly accounted ones, and insertion order is not
        // something a future author picks with this guard in mind.
        var addedFirst = new[]
        {
            "                failedBatches++;",
            "                failedBatches++;",
            "                embedFailedBatches++;",
            "                failedBatches++;",
            "                storeFailedBatches++;",
        };

        var addedBetween = new[]
        {
            "                failedBatches++;",
            "                embedFailedBatches++;",
            "                failedBatches++;",
            "                failedBatches++;",
            "                storeFailedBatches++;",
        };

        var addedLast = new[]
        {
            "                failedBatches++;",
            "                embedFailedBatches++;",
            "                failedBatches++;",
            "                storeFailedBatches++;",
            "                failedBatches++;",
        };

        Assert.Multiple(() =>
        {
            Assert.That(FindTotalsWithNoComponent(attributed), Is.Empty,
                "The rule must accept source in which every total is attributed to a stage, or it would "
                + "be a guard nobody could satisfy.");
            Assert.That(FindTotalsWithNoComponent(addedFirst), Is.EqualTo(new[] { 1 }),
                "An unattributed total added BEFORE the accounted paths must be flagged.");
            Assert.That(FindTotalsWithNoComponent(addedBetween), Is.EqualTo(new[] { 3 }),
                "An unattributed total added BETWEEN two accounted paths must be flagged. This is the "
                + "case a backward-looking window would silently accept, because the preceding path's "
                + "component sits just above it.");
            Assert.That(FindTotalsWithNoComponent(addedLast), Is.EqualTo(new[] { 5 }),
                "An unattributed total added AFTER the accounted paths must be flagged.");
        });
    }

    /// <summary>
    /// Proves the adjacency rule flags a stage counted without a total, and accepts
    /// the two shapes the real source uses - a component on the line below the
    /// total, and a pair of components in the arms of an if/else a few lines below.
    /// </summary>
    [Test]
    public void The_failure_accounting_adjacency_rule_flags_a_stage_counted_without_a_total()
    {
        var realShapes = new[]
        {
            "                failedBatches++;",
            "                embedFailedBatches++;",
            "                failedBatches++;",
            "                if (stage == \"record\")",
            "                {",
            "                    recordFailedBatches++;",
            "                }",
            "                else",
            "                {",
            "                    storeFailedBatches++;",
            "                }",
        };

        var orphaned = new[]
        {
            "                failedBatches++;",
            "                embedFailedBatches++;",
            "                // ... twelve or more lines of unrelated work ...",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                //",
            "                retireFailedBatches++;",
        };

        Assert.Multiple(() =>
        {
            Assert.That(FindComponentsWithNoTotal(realShapes), Is.Empty,
                "Both shapes the production source actually uses must be accepted, including the "
                + "if/else pair whose components sit several lines below their total.");
            Assert.That(FindComponentsWithNoTotal(orphaned), Is.EqualTo(new[] { 15 }),
                "A stage incremented with no total above it must be flagged: it makes the census "
                + "over-report success.");
        });
    }

    /// <summary>
    /// Proves the total pattern does not match the stage counters or the unrelated
    /// consecutive-failure counter that shares its vocabulary. A pattern that
    /// matched <c>embedFailedBatches++</c> as a total would report every real site
    /// as correctly paired with itself, which is a guard that can never fail.
    /// </summary>
    [Test]
    public void The_failure_accounting_total_pattern_matches_the_total_and_nothing_that_resembles_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TotalIncrement.IsMatch("                failedBatches++;"), Is.True);
            Assert.That(TotalIncrement.IsMatch("                embedFailedBatches++;"), Is.False,
                "A stage counter is not the total. Matching one as the total would make every stage "
                + "increment look like a correctly paired total, and the pairing rule could never fail.");
            Assert.That(TotalIncrement.IsMatch("                consecutiveBatchFailures++;"), Is.False,
                "The saturation counter is not the failure total.");

            Assert.That(ComponentIncrement.IsMatch("                failedBatches++;"), Is.False,
                "The total is not a stage. Matching it as one would let a total pair with itself.");
            Assert.That(ComponentIncrement.IsMatch("                recordFailedBatches++;"), Is.True);
            Assert.That(ComponentIncrement.IsMatch("                consecutiveBatchFailures++;"), Is.False);

            Assert.That(CensusFailurePlaceholder.IsMatch("{EmbedFailed}"), Is.True);
            Assert.That(CensusFailurePlaceholder.IsMatch("{Succeeded}"), Is.False);
            Assert.That(CensusFailurePlaceholder.IsMatch("{Saturated}"), Is.False);
        });
    }
}
