using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Issue #2322's general rule, made a gate: every protocol action the
/// refinement note maps to production ships with a mutation that perturbs
/// that action and that some checked property catches.
/// <para>
/// WHY THE PROPERTY GATE WAS NOT ENOUGH. <see cref="SpecMutationCatalogueTests"/>
/// already requires a firing mutation for every property, but a property can be
/// made to fire by editing a read definition, a fairness assumption, or by
/// splicing in an action the protocol does not have. None of those says
/// anything about whether the spec would notice production deviating from the
/// step a given action row claims it models. Before this gate,
/// <c>ForgetDecision</c>'s drain guard - the whole safety argument for the
/// cleanup - had no specification-level mutation at all, and the README said
/// so in prose that nothing checked.
/// </para>
/// <para>
/// WHAT IT CHECKS, AND WHAT IT DOES NOT. It checks that the three artefacts
/// agree on the set of actions (the module's <c>Next</c>, the note's action
/// table, the catalogue's <c>PERTURBS:</c> headers) and that each declared
/// perturbation really edits the definition it names. It does not run TLC:
/// whether each mutation fires is <see cref="TlcModelCheckTests"/>'s job, and
/// both are needed for the claim to stand.
/// </para>
/// </summary>
[TestFixture]
public sealed class SpecActionMutationCoverageTests
{
    /// <summary>
    /// Actions in <c>Next</c> that model no protocol step and so have no
    /// production behaviour for a mutation to stand for. Explicit rather than
    /// inferred, matching <c>RefinementDetectorMappingTests</c>, so a new action
    /// cannot opt itself out by omission.
    /// </summary>
    private static readonly string[] NonBehaviouralActions = ["Stutter"];

    private static string SpecDirectory => Path.Combine(HygieneRepository.FindRepoRoot(), "spec");

    private static string BaseSpecification => File.ReadAllText(Path.Combine(SpecDirectory, "AtomicCommit.tla"));

    private static IReadOnlyList<SpecMutation> Mutations() =>
        SpecMutationCatalogue.Load(Path.Combine(SpecDirectory, "mutations"));

    private static IReadOnlyList<string> BehaviouralActions() =>
        SpecActions.ReadNextActions(BaseSpecification)
            .Where(a => !NonBehaviouralActions.Contains(a, StringComparer.Ordinal))
            .ToArray();

    /// <summary>
    /// The vacuity floor. Every other test here iterates these sets, so a
    /// parser regression that read nothing would otherwise pass them all.
    /// </summary>
    [Test]
    public void The_specification_and_catalogue_yield_actions_and_perturbations_to_check()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                BehaviouralActions(),
                Has.Count.GreaterThanOrEqualTo(5),
                "parsed fewer protocol actions out of spec/AtomicCommit.tla's Next relation than the "
                + "specification has ever had, so the gates in this fixture would under-check.");

            Assert.That(
                Mutations().SelectMany(m => m.Perturbs),
                Is.Not.Empty,
                "no mutation in spec/mutations/ declares PERTURBS:, so the coverage gate would be vacuous.");
        });
    }

    /// <summary>
    /// The module and the note agree on which actions exist. Adding an action
    /// to <c>Next</c> without mapping it, or leaving a row behind for an action
    /// that was removed, fails here.
    /// </summary>
    [Test]
    public void The_refinement_action_table_maps_exactly_the_actions_in_Next()
    {
        var inNext = SpecActions.ReadNextActions(BaseSpecification);
        var mapped = RefinementNote.ReadActionRows().Select(SpecActions.ActionNameOf).ToArray();

        Assert.That(
            mapped,
            Is.EquivalentTo(inNext),
            "spec/Refinement.md's action table must have one row per disjunct of Next in "
            + $"spec/AtomicCommit.tla. Next: [{string.Join(", ", inNext)}]. "
            + $"Rows: [{string.Join(", ", mapped)}].");
    }

    /// <summary>
    /// The rule itself: each behavioural action is perturbed by at least one
    /// mutation.
    /// </summary>
    [Test]
    public void Every_protocol_action_is_perturbed_by_a_mutation()
    {
        var perturbed = Mutations().SelectMany(m => m.Perturbs).ToHashSet(StringComparer.Ordinal);
        var unpaired = BehaviouralActions().Where(a => !perturbed.Contains(a)).ToArray();

        Assert.That(
            unpaired,
            Is.Empty,
            "every protocol action in spec/AtomicCommit.tla's Next needs a mutation in spec/mutations/ "
            + "that edits its definition, declares it under PERTURBS:, and makes a checked property fire "
            + $"(issue #2322). Unpaired: [{string.Join(", ", unpaired)}].");
    }

    /// <summary>
    /// A <c>PERTURBS:</c> header is a claim, so it is checked against the
    /// edits: at least one edit must anchor inside the named action's
    /// definition AND change what TLC reads there, not merely its comments
    /// (<see cref="SpecActions.MutationPerturbs"/>). A header naming an action
    /// the mutation never touches, or touches only with a comment while its
    /// real change lands elsewhere, would otherwise satisfy the rule above
    /// while proving nothing about it.
    /// </summary>
    [Test]
    public void Every_declared_perturbation_edits_the_action_it_names()
    {
        var baseSpec = BaseSpecification.ReplaceLineEndings("\n");
        var actions = SpecActions.ReadNextActions(baseSpec);

        Assert.Multiple(() =>
        {
            foreach (var mutation in Mutations())
            {
                foreach (var action in mutation.Perturbs)
                {
                    Assert.That(
                        actions,
                        Does.Contain(action),
                        $"mutation '{mutation.Name}' declares PERTURBS: {action}, which is not an action in "
                        + "spec/AtomicCommit.tla's Next relation.");

                    if (!actions.Contains(action, StringComparer.Ordinal))
                    {
                        continue;
                    }

                    Assert.That(
                        SpecActions.MutationPerturbs(baseSpec, action, mutation),
                        Is.True,
                        $"mutation '{mutation.Name}' declares PERTURBS: {action}, but none of its edits both "
                        + $"anchors inside {action}'s definition and changes more than comments there. The "
                        + "header is a claim about what the mutation changes, and this one is false.");
                }
            }
        });
    }

    /// <summary>
    /// The standing negative controls for the check above, exercising the
    /// gate's own logic rather than only the definition reader beneath it.
    /// Each rejected case is a way a <c>PERTURBS:</c> header could lie; the
    /// accepted case stops the controls passing because the check rejects
    /// everything.
    /// </summary>
    [Test]
    public void The_perturbation_check_rejects_comment_only_and_misplaced_edits()
    {
        var baseSpec = BaseSpecification.ReplaceLineEndings("\n");
        const string Anchor = "           allDone == \\A j \\in Written(t) : nterm[j] # \"none\"";
        const string RealChange = "           allDone == \\E j \\in Written(t) : nterm[j] # \"none\"";

        var broadcast = SpecActions.ReadDefinition(baseSpec, "BroadcastStep");
        var decide = SpecActions.ReadDefinition(baseSpec, "DecideTx");

        Assert.Multiple(() =>
        {
            Assert.That(
                broadcast,
                Does.Contain(Anchor),
                "the controls' anchor no longer lies in BroadcastStep, so every arm below would pass or fail "
                + "for the wrong reason. Re-derive it from spec/AtomicCommit.tla.");

            Assert.That(
                SpecActions.EditPerturbs(broadcast, new SpecEdit(Anchor, RealChange)),
                Is.True,
                "a real change anchored in BroadcastStep was refused, so the check rejects everything and the "
                + "rejections below prove nothing.");

            Assert.That(
                SpecActions.EditPerturbs(broadcast, new SpecEdit(Anchor, "\\* MUTATION: a note only.\n" + Anchor)),
                Is.False,
                "an edit that adds only a line comment inside BroadcastStep was accepted as perturbing it.");

            Assert.That(
                SpecActions.EditPerturbs(broadcast, new SpecEdit(Anchor, Anchor + " (* note (* nested *) *)")),
                Is.False,
                "an edit that adds only a block comment inside BroadcastStep was accepted as perturbing it.");

            Assert.That(
                SpecActions.EditPerturbs(decide, new SpecEdit(Anchor, RealChange)),
                Is.False,
                "a real change anchored in BroadcastStep was accepted as perturbing DecideTx, so the check "
                + "cannot tell one action from another.");

            var splitClaim = SpecMutationCatalogue.Parse(
                "SplitClaim",
                "MODULE: SplitClaim\nTARGET: NoStrandedPrepare\nCLASS: Temporal\nSUMMARY: control\n"
                + "PERTURBS: BroadcastStep\n\n"
                + "--- FIND\n" + Anchor + "\n--- REPLACE\n\\* MUTATION: decorative.\n" + Anchor + "\n--- END\n"
                + "--- FIND\n    /\\ phase[t] = \"prepared\"\n--- REPLACE\n    /\\ phase[t] = \"init\"\n--- END\n");

            Assert.That(
                SpecActions.MutationPerturbs(baseSpec, "BroadcastStep", splitClaim),
                Is.False,
                "a mutation whose only edit inside BroadcastStep is a comment, with its real change in another "
                + "action, was accepted as perturbing BroadcastStep.");
        });
    }

    /// <summary>
    /// The comment stripper the check above depends on: line and nested block
    /// comments go, a comment marker inside a string literal does not, and
    /// line structure survives so a change of conjunct layout still counts.
    /// </summary>
    [Test]
    public void StripComments_removes_TLA_comments_and_keeps_everything_TLC_reads()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                SpecActions.StripComments("    /\\ x' = 1 \\* trailing\n\\* whole line\n    /\\ y' = 2"),
                Is.EqualTo("    /\\ x' = 1\n    /\\ y' = 2"));
            Assert.That(
                SpecActions.StripComments("a (* one (* two *) still *) b"),
                Is.EqualTo("a  b"));
            Assert.That(
                SpecActions.StripComments("x == \"\\* not a comment\""),
                Is.EqualTo("x == \"\\* not a comment\""));
            Assert.That(
                SpecActions.StripComments("(* a\nb *)\nc"),
                Is.EqualTo("c"));
        });
    }

    /// <summary>
    /// The parser handles the module's real disjunct shapes: an unquantified
    /// action, one quantifier, and two nested quantifiers.
    /// </summary>
    [Test]
    public void Next_disjuncts_are_read_through_their_quantifiers()
    {
        const string spec = "Next ==\n"
            + "    \\/ \\E t \\in Txns : PrepareTx(t)\n"
            + "    \\/ \\E t \\in Txns : \\E k \\in Keys : BroadcastStep(t, k)\n"
            + "    \\/ Stutter\n"
            + "\n"
            + "Spec == Init\n";

        Assert.That(
            SpecActions.ReadNextActions(spec),
            Is.EqualTo(new[] { "PrepareTx", "BroadcastStep", "Stutter" }));
    }
}
