using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The two ways the <c>Detector</c> column gate could be defeated with the
/// whole suite green, kept as permanent regressions (issue #2561).
/// <para>
/// Both were found by a post-merge adversarial review of #2555 and both were
/// confirmed by experiment: perturbing <c>spec/Refinement.md</c> in either way
/// left the <c>Refinement</c> filter reporting 48 passed, 0 failed. The
/// perturbations are reproduced here verbatim against hand-written markdown,
/// so the holes cannot reopen without a red test, and so the evidence does not
/// depend on anyone re-running an experiment by hand.
/// </para>
/// <para>
/// They are driven by synthetic notes rather than by the real one on purpose.
/// A regression that perturbs the note on disk would either have to mutate a
/// tracked file or would drift the moment a sibling item rewrites the row it
/// picked; the rule under test is the same either way, because
/// <see cref="Every_row_claiming_detection_names_a_resolvable_test"/> runs it
/// over the real note.
/// </para>
/// </summary>
internal sealed partial class RefinementDetectorMappingTests
{
    /// <summary>
    /// The reviewer's hole-1 perturbation: the <c>DecisionDurability</c>
    /// Detector cell rewritten as prose that still claims detection.
    /// </summary>
    private const string ProseOnlyCell = "Yes: covered by the registry fixture terminal-transition tests.";

    /// <summary>
    /// The reviewer's hole-2 perturbation: the <c>MonotonicVisibility</c>
    /// detector rotted to a name that exists nowhere under <c>test/</c>, whose
    /// member part begins lower-case - the shape this repository's test
    /// methods conventionally take after the first word.
    /// </summary>
    private const string RottedDetectorName =
        "AtomicVisibilityGateTests.committed_but_already_terminal_orphan_DELETED_LONG_AGO";

    /// <summary>
    /// Wraps a single Detector cell in the three sections
    /// <see cref="RefinementNote.ParseTables"/> requires, so one cell can be
    /// put under the real rule in isolation.
    /// </summary>
    private static IReadOnlyList<RefinementTable> NoteWithDetectorCell(string detectorCell)
    {
        var note = $"""
            ## Variable mapping

            | Spec variable | Code counterpart |
            |---------------|------------------|
            | `revision` | `TxRegistryState.DecisionsRevision`. |

            ## Action mapping

            | Spec action | Code counterpart | Detector |
            |-------------|------------------|----------|
            | `DecideTx(t)` | `AtomicWriteGrain.RecordTerminalDecisionAsync`. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |

            ## Property mapping

            | Spec property | Code-level property it abstracts | Detector |
            |---------------|----------------------------------|----------|
            | `DecisionDurability` | `TxStatus` transitions are terminal. | {detectorCell} |
            """;

        var tables = RefinementNote.ParseTables(note);
        return [tables[RefinementNote.ActionSection], tables[RefinementNote.PropertySection]];
    }

    /// <summary>
    /// HOLE 1, closed. A cell that claims detection but names no test at all
    /// used to pass: the extractor returned zero references for it, so
    /// <see cref="Every_test_named_as_a_detector_exists"/> had nothing to
    /// iterate and its global anti-vacuity guard was satisfied by the other
    /// rows.
    /// <para>
    /// The resolver here is deliberately maximally permissive (everything
    /// resolves), so the test isolates the half that matters: even when no
    /// name could possibly fail to resolve, a row that names nothing is still
    /// rejected.
    /// </para>
    /// </summary>
    [Test]
    public void A_row_claiming_detection_in_prose_alone_is_rejected()
    {
        var failures = RefinementDetectorRule.BehaviourRowsWithoutAResolvableTest(
            NoteWithDetectorCell(ProseOnlyCell),
            NonBehaviouralRows,
            _ => true);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Has.Count.EqualTo(1));
            Assert.That(failures[0], Does.Contain("`DecisionDurability`"));
            Assert.That(failures[0], Does.Contain("names no test at all"));
        });
    }

    /// <summary>
    /// HOLE 2, first half. The rotted name is now extracted at all. Under the
    /// shared production pattern its lower-case member part meant it matched
    /// nothing, so it was never handed to the resolver and its row read as
    /// checked when it was not.
    /// </summary>
    [Test]
    public void A_detector_name_whose_member_begins_lower_case_is_extracted()
    {
        var extracted = RefinementCodeSymbols
            .ExtractDetectors(NoteWithDetectorCell($"Yes: `{RottedDetectorName}`."))
            .Select(d => d.Text);

        Assert.That(extracted, Does.Contain(RottedDetectorName));
    }

    /// <summary>
    /// HOLE 2, second half. Being extracted is only useful if it then fails,
    /// so this runs the extracted name against the repository's real
    /// <c>test/</c> tree and asserts the gate rejects the row.
    /// </summary>
    [Test]
    public void A_rotted_detector_name_that_begins_lower_case_is_rejected()
    {
        var resolver = RefinementDetectorResolver.ForRepository();

        var failures = RefinementDetectorRule.BehaviourRowsWithoutAResolvableTest(
            NoteWithDetectorCell($"Yes: `{RottedDetectorName}`."),
            NonBehaviouralRows,
            d => resolver.TestExists(d.TypeName, d.MemberName));

        Assert.Multiple(() =>
        {
            Assert.That(
                resolver.TestExists("AtomicVisibilityGateTests", "committed_but_already_terminal_orphan_DELETED_LONG_AGO"),
                Is.False,
                "the perturbation is only a perturbation if the name really is absent.");
            Assert.That(failures, Has.Count.EqualTo(1));
            Assert.That(failures[0], Does.Contain(RottedDetectorName));
        });
    }

    /// <summary>
    /// The false-positive guard on the relaxed Detector pattern. Relaxing the
    /// right-hand side is the change most likely to start manufacturing
    /// references out of things that are not symbols, so what it still refuses
    /// is pinned rather than assumed. The left-hand side keeps its upper-case
    /// requirement, multi-segment forms stay excluded, and text outside
    /// backticks is untouched.
    /// <para>
    /// The TLA+ cases matter most. Bare backticked identifiers are
    /// deliberately ungated across this whole area precisely because a spec
    /// variable, a spec string value and an enum member quoted without its
    /// type are not distinguishable from a code symbol without a judgement
    /// call. Widening the right-hand side must not smuggle any of them in
    /// through the back door, so they are asserted here rather than reasoned
    /// about.
    /// </para>
    /// </summary>
    [TestCase("`InFlight`", TestName = "Extracts_no_detector_from_a_bare_identifier")]
    [TestCase("`phase[t]`", TestName = "Extracts_no_detector_from_a_spec_variable")]
    [TestCase("`vote[t][k]`", TestName = "Extracts_no_detector_from_a_subscripted_spec_variable")]
    [TestCase("`prepared`", TestName = "Extracts_no_detector_from_a_spec_string_value")]
    [TestCase("`PrepareTx(t)`", TestName = "Extracts_no_detector_from_a_spec_action_name")]
    [TestCase("`decision[t] = \"commit\"`", TestName = "Extracts_no_detector_from_a_spec_assignment")]
    [TestCase("`_recentlyTerminal`", TestName = "Extracts_no_detector_from_a_bare_field")]
    [TestCase("`alreadyTerminal.foo`", TestName = "Extracts_no_detector_from_a_lower_case_left_hand_side")]
    [TestCase("`Orleans.Lattice.Tests`", TestName = "Extracts_no_detector_from_a_multi_segment_form")]
    [TestCase("`Fixture.2nd_case`", TestName = "Extracts_no_detector_from_a_digit_initial_member")]
    [TestCase("`terminal # \"none\"`", TestName = "Extracts_no_detector_from_a_spec_expression")]
    [TestCase("AtomicVisibilityGateTests.some_test", TestName = "Extracts_no_detector_from_outside_backticks")]
    public void Extracts_no_detector_from(string cellText)
    {
        var extracted = RefinementCodeSymbols
            .ExtractDetectors(NoteWithDetectorCell($"Yes: {cellText}."))
            .Where(d => d.Row == "`DecisionDurability`")
            .Select(d => d.Text);

        Assert.That(extracted, Is.Empty);
    }

    /// <summary>
    /// The cost of the relaxation, recorded rather than left to be discovered.
    /// A backticked dotted file name is what the upper-case requirement was
    /// protecting the production columns from, and in a Detector cell it is
    /// now extracted and will not resolve.
    /// <para>
    /// That is the safe direction and the trade is deliberate. In the
    /// production columns a false positive is the gate crying wolf about
    /// correct prose, which gets the gate suppressed; here it is one cell
    /// named in one loud failure, and Detector cells are known to cite
    /// <c>Fixture.TestMethod</c> names and nothing else. The alternative is
    /// what this issue reports: a real rot passing in silence.
    /// </para>
    /// </summary>
    [Test]
    public void A_dotted_file_name_in_a_detector_cell_is_extracted_and_fails_loudly()
    {
        var extracted = RefinementCodeSymbols
            .ExtractDetectors(NoteWithDetectorCell("Yes: `AtomicCommit.tla`."))
            .Select(d => d.Text);

        Assert.That(extracted, Does.Contain("AtomicCommit.tla"));
    }

    /// <summary>
    /// The production columns keep the narrower pattern, so relaxing the
    /// Detector column did not relax them. This is the regression that keeps
    /// the two patterns from being quietly unified back together.
    /// </summary>
    [Test]
    public void The_production_columns_still_refuse_a_lower_case_right_hand_side()
    {
        var note = """
            ## Variable mapping

            | Spec variable | Code counterpart |
            |---------------|------------------|
            | `revision` | `AtomicCommit.tla` and `TxRegistryState.decisionsRevision`. |

            ## Action mapping

            | Spec action | Code counterpart |
            |-------------|------------------|
            | `DecideTx(t)` | `AtomicWriteGrain.RecordTerminalDecisionAsync`. |

            ## Property mapping

            | Spec property | Code-level property it abstracts |
            |---------------|----------------------------------|
            | `StrictIsolation` | `AtomicVisibilityGate.ResolveKey`. |
            """;

        var tables = RefinementNote.ParseTables(note);
        var extracted = RefinementCodeSymbols
            .Extract(new[] { tables[RefinementNote.VariableSection] })
            .Select(s => s.Text);

        Assert.That(extracted, Is.Empty);
    }

    // RETIRED BY #2557, deliberately, and not by deleting the protection.
    //
    // #2561 wrote two tests here that ran the rule over the real note: the gate
    // itself, and an anti-vacuity companion counting rows that claimed
    // detection. Both were right while the rule was scoped to "Yes:" cells.
    // Once #2557 widened RefinementDetectorRule.MustNameAResolvableDetector to
    // cover every behaviour-asserting row, the pair degenerated: the gate became
    // identical to
    // RefinementDetectorMappingTests.Every_behaviour_asserting_row_names_a_resolvable_test,
    // and the companion collapsed into "the note has at least one row", which
    // reads as anti-vacuity while asserting almost nothing. #2561's author
    // anticipated exactly this and asked #2557 to re-think them rather than
    // keep them as ceremony.
    //
    // Both real-note assertions now live in one place, beside the floor they
    // replace, with anti-vacuity asserted on the examined denominator rather
    // than on a claim count. What stays in this file is what only this file
    // proves: the extraction regressions over hand-written notes, which is
    // where #2561's two holes were found and which no other fixture covers.
}
