using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The acceptance census of epic #4430 (issue #4442): every refinement note
/// under <c>spec/</c> is read, every behaviour-asserting row of every note is
/// an unqualified Yes, and no note cites an open issue as the owner of a gap.
/// <para>
/// The two per-module gates draw from discovery, so the synthetic-module
/// control in <see cref="SpecModuleDiscoveryControlTests"/> runs them over a
/// module they have never seen and breaks that module to prove each one goes
/// red. The repository-wide test closes the remaining hole: a note that no
/// module's manifest names would sit outside every per-module gate, so it
/// globs the notes from disk and requires each to be one the gates read.
/// </para>
/// </summary>
internal sealed partial class RefinementDetectorMappingTests
{
    private static readonly string[] BehaviourSections = [RefinementNote.ActionSection, RefinementNote.PropertySection];

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_detector_verdict_is_an_unqualified_yes(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var tables = module.ReadRefinementTables();
        var nonBehavioural = NonBehaviouralRows(module);
        var examined = RefinementDetectorRule.BehaviourRowsExamined(
            BehaviourSections.Select(s => tables[s]),
            nonBehavioural);

        var findings = RefinementAcceptanceCensus.VerdictFindings(tables, BehaviourSections, nonBehavioural);

        Assert.Multiple(() =>
        {
            Assert.That(
                examined,
                Is.GreaterThan(0),
                $"{module.Describe(module.RefinementNotePath)} yielded no behaviour-asserting rows, so the census "
                + "below would pass while reading nothing.");

            Assert.That(
                findings,
                Is.Empty,
                module.Describe(module.RefinementNotePath) + Environment.NewLine
                + string.Join(Environment.NewLine, findings)
                + Environment.NewLine
                + "Epic #4430 is accepted only when every row is Yes (#4442). A row that is not detected is a gap: "
                + "close it - a detector proven red against the production shape it pins - rather than writing "
                + "the gap back into the note.");
        });
    }

    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_note_cites_no_open_issue_as_a_gap(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var findings = RefinementAcceptanceCensus.OpenGapCitationFindings(module.ReadRefinementNote());

        Assert.That(
            findings,
            Is.Empty,
            module.Describe(module.RefinementNotePath) + Environment.NewLine
            + string.Join(Environment.NewLine, findings)
            + Environment.NewLine
            + "A refinement note records closed defects as history; it may not cite an issue as the place an "
            + "unfinished gap is tracked (#4442). Close the gap, or move the claim out of the note's coverage "
            + "and say so as a deliberate abstraction gap.");
    }

    [Test]
    public void Every_refinement_note_under_spec_is_one_the_census_reads()
    {
        var root = SpecModuleCatalogue.RepositorySpecRoot;
        var onDisk = Directory
            .EnumerateFiles(root, "*.md", SearchOption.AllDirectories)
            .Where(p => Path.GetFileName(p).Contains("Refinement", StringComparison.Ordinal))
            .Select(Path.GetFullPath)
            .Order(StringComparer.Ordinal)
            .ToArray();

        var modules = SpecModuleCatalogue.Repository();
        var read = modules.Select(m => m.RefinementNotePath).ToHashSet(StringComparer.Ordinal);
        var rows = modules.Sum(m => RefinementDetectorRule.BehaviourRowsExamined(
            BehaviourSections.Select(s => m.ReadRefinementTables()[s]),
            NonBehaviouralRows(m)));

        Assert.Multiple(() =>
        {
            Assert.That(onDisk, Is.Not.Empty, $"no refinement note was found under {root}, so the census reads nothing.");
            Assert.That(rows, Is.GreaterThan(0), "the census read no behaviour-asserting rows across every module.");
            Assert.That(
                onDisk.Where(p => !read.Contains(p)),
                Is.Empty,
                "a refinement note under spec/ is named by no module's manifest, so no census gate reads it. "
                + "Name it from its module's manifest, or rename it if it is not a refinement note.");
        });
    }

    [Test]
    public void The_verdict_rule_fires_on_every_verdict_that_is_not_yes()
    {
        // Anti-vacuity: the verdicts #4442 forbids, each in the shape a note
        // has used or an author would reach for.
        string[] behaviour =
        [
            "Partial: `Fixture.Test` covers one of the two production paths. Gap filed as #2554.",
            "None: no test reaches this branch (#4442).",
            "No: production cannot be driven here.",
            "Gap: #4549.",
            "Assumption: the environment never does this.",
            "Assumption-only: argued in the README.",
            "Argued: see the over-approximation argument above.",
            "Yes: partially, through `Fixture.Test`.",
            "Yes, by assumption: the sender never retries.",
            "Yes: `Fixture.Test`, assumption-only for the second path.",
            "`Fixture.Test` covers it.",
            "Not applicable: the step is environmental.",
        ];

        Assert.Multiple(() =>
        {
            foreach (var cell in behaviour)
            {
                Assert.That(
                    RefinementAcceptanceCensus.VerdictProblem(cell, behaviourAsserting: true),
                    Is.Not.Null,
                    $"the verdict rule accepted '{cell}' on a behaviour-asserting row.");
            }

            Assert.That(
                RefinementAcceptanceCensus.VerdictProblem("Partial: see #2554.", behaviourAsserting: false),
                Is.Not.Null,
                "the verdict rule accepted a Partial verdict on a row outside the behaviour tables.");
        });
    }

    [Test]
    public void The_verdict_rule_accepts_the_verdicts_the_notes_carry()
    {
        string[] behaviour =
        [
            "Yes: `Fixture.Test`.",
            "Yes, for both halves. Flipping either reds `Fixture.Test`.",
            "Yes, for what the step means here: `Fixture.Test`.",
            "Yes: `Fixture.Test` (none of the other tests wires the registry).",
        ];

        Assert.Multiple(() =>
        {
            foreach (var cell in behaviour)
            {
                Assert.That(
                    RefinementAcceptanceCensus.VerdictProblem(cell, behaviourAsserting: true),
                    Is.Null,
                    $"the verdict rule rejected the legitimate cell '{cell}'.");
            }

            Assert.That(
                RefinementAcceptanceCensus.VerdictProblem(
                    "Not applicable: not a protocol step, so there is no production behaviour to detect.",
                    behaviourAsserting: false),
                Is.Null,
                "the verdict rule rejected the Stutter row's verdict.");
            Assert.That(
                RefinementAcceptanceCensus.VerdictProblem("`WalRetentionBlockPinTests.Test`", behaviourAsserting: false),
                Is.Null,
                "the verdict rule rejected an auxiliary table's plain test list.");
        });
    }

    [Test]
    public void The_gap_citation_detector_fires_on_an_open_issue_cited_as_a_gap()
    {
        string[] citations =
        [
            "The receiver half is tracked by #4707.",
            "This row stays Partial until #4549 lands.",
            "The own-origin clamp (#4586, still open) is not modelled.",
            "#4641 (open) owns the empty-release frontier.",
            "The gap is filed as #4654.",
            "The second path is not yet covered; see #4615.",
            "#4615 remains open, so the reap row is argued.",
            "Owned by open issue #4673.",
            "Pending #4586 part 2b-2.",
            "The residual is carried by #4549.",
            "| `Deliver(e)` | role | code | Yes: `Fixture.Test`; the drain path is tracked by #4707. |",
            "Follow-up #4720 pins it.",
            "## Territory owned by other open issues\n\n### #4707: the stale-lineage drain path",
        ];

        Assert.Multiple(() =>
        {
            foreach (var citation in citations)
            {
                Assert.That(
                    RefinementAcceptanceCensus.OpenGapCitationFindings(citation),
                    Is.Not.Empty,
                    $"the gap-citation detector did not fire on '{citation}'.");
            }
        });
    }

    [Test]
    public void The_gap_citation_detector_ignores_closed_defects_recorded_as_history()
    {
        // Real prose from the notes: a closed defect, cited with its fix, is
        // what a refinement note is meant to carry.
        string[] history =
        [
            "Production pinned the floor at the frontier until #4476 (mutation BootstrapHandoffLosesNothingPinnedFloor).",
            "Before #4681 the batch carried no lineage and applied.",
            "| #4549 | A reaped delete was not reconciled. | #4675 | `EventualConvergenceForeignRowNotReconciled` |",
            "No open issue owns territory in this module.",
            "## Territory owned by other open issues\n\n### No open issue currently owns a claim here\n\n### Closed: #2319, #2320, #2325 and #2333",
            "The gap-citation gate checks that a row reporting a gap cites an issue, not that the issue is still open.",
            "The rows reporting `Partial` or `None` are the open gaps, and each cites the issue that closes it.",
            "Saga atomicity across a bootstrap (#4683, #4684, #4685) is the atomic-commit cross-cluster module's, not this directory's.",
            "```text\ntracked by #4707\n```",
        ];

        Assert.Multiple(() =>
        {
            foreach (var line in history)
            {
                Assert.That(
                    RefinementAcceptanceCensus.OpenGapCitationFindings(line),
                    Is.Empty,
                    $"the gap-citation detector fired on legitimate history: '{line}'.");
            }
        });
    }
}
