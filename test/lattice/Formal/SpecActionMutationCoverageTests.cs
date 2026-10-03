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
    /// edits: at least one edit's anchor must lie inside the named action's
    /// definition. A header naming an action the mutation never touches would
    /// otherwise satisfy the rule above while proving nothing about it.
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

                    var definition = SpecActions.ReadDefinition(baseSpec, action);
                    Assert.That(
                        mutation.Edits.Any(e => definition.Contains(e.Find, StringComparison.Ordinal)),
                        Is.True,
                        $"mutation '{mutation.Name}' declares PERTURBS: {action}, but none of its edits "
                        + $"anchors inside {action}'s definition. The header is a claim about what the "
                        + "mutation changes, and this one is false.");
                }
            }
        });
    }

    /// <summary>
    /// The standing negative control for the containment check above: a
    /// mutation whose only edit lies in a different action is refused. Without
    /// it the check's green would be indistinguishable from one that accepts
    /// everything.
    /// </summary>
    [Test]
    public void A_perturbation_anchored_in_a_different_action_is_not_accepted()
    {
        var baseSpec = BaseSpecification.ReplaceLineEndings("\n");
        var broadcastAnchor = Mutations()
            .Single(m => m.Name == "NoStrandedPrepareEarlyCompletion")
            .Edits[0].Find;

        Assert.Multiple(() =>
        {
            Assert.That(
                SpecActions.ReadDefinition(baseSpec, "BroadcastStep").Contains(broadcastAnchor, StringComparison.Ordinal),
                Is.True,
                "the control's anchor no longer lies in BroadcastStep, so the negative arm below would "
                + "pass for the wrong reason.");

            Assert.That(
                SpecActions.ReadDefinition(baseSpec, "DecideTx").Contains(broadcastAnchor, StringComparison.Ordinal),
                Is.False,
                "an anchor inside BroadcastStep was accepted as editing DecideTx, so the containment "
                + "check cannot tell one action from another.");
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
