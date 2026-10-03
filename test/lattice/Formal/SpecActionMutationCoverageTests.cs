namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Issue #2322's general rule, made a gate for every module: every protocol
/// action the refinement note maps to production ships with a mutation that
/// perturbs that action and that some checked property catches.
/// <para>
/// WHY THE PROPERTY GATE WAS NOT ENOUGH. <see cref="SpecMutationCatalogueTests"/>
/// already requires a firing mutation for every property, but a property can be
/// made to fire by editing a read definition, a fairness assumption, or by
/// splicing in an action the protocol does not have. None of those says
/// anything about whether the spec would notice production deviating from the
/// step a given action row claims it models. Before this gate, the
/// atomic-commit module's <c>ForgetDecision</c> drain guard - the whole safety
/// argument for the cleanup - had no specification-level mutation at all, and
/// the README said so in prose that nothing checked.
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
    private static IReadOnlyList<string> BehaviouralActions(SpecModule module) =>
        SpecActions.ReadNextActions(module.ReadSpecification())
            .Where(a => !module.Manifest.NonBehaviouralActions.Contains(a, StringComparer.Ordinal))
            .ToArray();

    /// <summary>
    /// The vacuity floor. Every other test here iterates these sets, so a
    /// parser regression that read nothing would otherwise pass them all. The
    /// action count is the manifest's, as an equality, so an action dropped
    /// from <c>Next</c> together with its row and its mutation still fails.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_specification_and_catalogue_yield_actions_and_perturbations_to_check(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var spec = module.Describe(module.SpecificationPath);

        Assert.Multiple(() =>
        {
            Assert.That(
                SpecActions.ReadNextActions(module.ReadSpecification()),
                Has.Count.EqualTo(module.Manifest.Counts.Actions),
                $"{spec}'s Next relation should disjoin {module.Manifest.Counts.Actions} actions. If that changed on "
                + $"purpose, update {module.Describe(module.ManifestPath)} and the counts table in "
                + $"{module.Describe(module.ReadmePath)}.");

            Assert.That(
                module.Manifest.NonBehaviouralActions.Except(SpecActions.ReadNextActions(module.ReadSpecification()), StringComparer.Ordinal),
                Is.Empty,
                $"{module.Describe(module.ManifestPath)} declares a non-behavioural action that is not in {spec}'s Next.");

            Assert.That(
                BehaviouralActions(module),
                Is.Not.Empty,
                $"every action in {spec}'s Next is declared non-behavioural, so the coverage gate checks nothing.");

            Assert.That(
                module.LoadMutations().SelectMany(m => m.Perturbs),
                Is.Not.Empty,
                $"no mutation in {module.Describe(module.MutationDirectory)}/ declares PERTURBS:, so the coverage gate "
                + "would be vacuous.");
        });
    }

    /// <summary>
    /// The module and the note agree on which actions exist. Adding an action
    /// to <c>Next</c> without mapping it, or leaving a row behind for an action
    /// that was removed, fails here.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_refinement_action_table_maps_exactly_the_actions_in_Next(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var inNext = SpecActions.ReadNextActions(module.ReadSpecification());
        var mapped = RefinementNote.ReadActionRows(module).Select(SpecActions.ActionNameOf).ToArray();

        Assert.That(
            mapped,
            Is.EquivalentTo(inNext),
            $"{module.Describe(module.RefinementNotePath)}'s action table must have one row per disjunct of Next in "
            + $"{module.Describe(module.SpecificationPath)}. Next: [{string.Join(", ", inNext)}]. "
            + $"Rows: [{string.Join(", ", mapped)}].");
    }

    /// <summary>
    /// The rule itself: each behavioural action is perturbed by at least one
    /// mutation.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_protocol_action_is_perturbed_by_a_mutation(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var perturbed = module.LoadMutations().SelectMany(m => m.Perturbs).ToHashSet(StringComparer.Ordinal);
        var unpaired = BehaviouralActions(module).Where(a => !perturbed.Contains(a)).ToArray();

        Assert.That(
            unpaired,
            Is.Empty,
            $"every protocol action in {module.Describe(module.SpecificationPath)}'s Next needs a mutation in "
            + $"{module.Describe(module.MutationDirectory)}/ that edits its definition, declares it under PERTURBS:, "
            + $"and makes a checked property fire (issue #2322). Unpaired: [{string.Join(", ", unpaired)}].");
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
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_declared_perturbation_edits_the_action_it_names(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var baseSpec = module.ReadSpecification().ReplaceLineEndings("\n");
        var actions = SpecActions.ReadNextActions(baseSpec);
        var spec = module.Describe(module.SpecificationPath);

        Assert.Multiple(() =>
        {
            foreach (var mutation in module.LoadMutations())
            {
                foreach (var action in mutation.Perturbs)
                {
                    Assert.That(
                        actions,
                        Does.Contain(action),
                        $"mutation '{mutation.Name}' declares PERTURBS: {action}, which is not an action in "
                        + $"{spec}'s Next relation.");

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
    /// gate's own logic over each module's own text rather than only the
    /// definition reader beneath it. Each rejected case is a way a
    /// <c>PERTURBS:</c> header could lie; the accepted case stops the controls
    /// passing because the check rejects everything.
    /// <para>
    /// The real change is taken from the module's catalogue - the first edit
    /// that genuinely perturbs a declared action - so the controls run against
    /// every module without hand-written anchors that would drift with it.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_perturbation_check_rejects_comment_only_and_misplaced_edits(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var baseSpec = module.ReadSpecification().ReplaceLineEndings("\n");
        var actions = SpecActions.ReadNextActions(baseSpec);

        var real = module.LoadMutations()
            .SelectMany(m => m.Perturbs.Where(a => actions.Contains(a, StringComparer.Ordinal)).Select(a => (Action: a, Mutation: m)))
            .SelectMany(p => p.Mutation.Edits
                .Where(e => SpecActions.EditPerturbs(SpecActions.ReadDefinition(baseSpec, p.Action), e))
                .Select(e => (p.Action, Edit: e)))
            .FirstOrDefault();

        Assert.That(
            real.Edit,
            Is.Not.Null,
            $"no mutation of {module.Name} has an edit that perturbs an action it declares, so these controls have "
            + "nothing to start from. The coverage gates in this fixture fail for the same reason.");

        var action = real.Action;
        var anchor = real.Edit!.Find.ReplaceLineEndings("\n");
        var definition = SpecActions.ReadDefinition(baseSpec, action);
        var elsewhere = actions
            .Where(a => a != action)
            .Select(a => SpecActions.ReadDefinition(baseSpec, a))
            .FirstOrDefault(d => !d.Contains(anchor, StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                SpecActions.EditPerturbs(definition, real.Edit),
                Is.True,
                $"a real change anchored in {action} was refused, so the check rejects everything and the "
                + "rejections below prove nothing.");

            Assert.That(
                SpecActions.EditPerturbs(definition, new SpecEdit(anchor, "\\* MUTATION: a note only.\n" + anchor)),
                Is.False,
                $"an edit that adds only a line comment inside {action} was accepted as perturbing it.");

            Assert.That(
                SpecActions.EditPerturbs(definition, new SpecEdit(anchor, anchor + " (* note (* nested *) *)")),
                Is.False,
                $"an edit that adds only a block comment inside {action} was accepted as perturbing it.");

            if (elsewhere is not null)
            {
                Assert.That(
                    SpecActions.EditPerturbs(elsewhere, real.Edit),
                    Is.False,
                    $"a real change anchored in {action} was accepted as perturbing a different action, so the "
                    + "check cannot tell one action from another.");
            }

            Assert.That(
                definition,
                Does.Not.Contain("Next =="),
                $"{action}'s definition runs into the Next relation, so the misplaced-edit control below would "
                + "anchor inside it.");

            var splitClaim = new SpecMutation
            {
                Name = "SplitClaim",
                Module = "SplitClaim",
                Target = SpecMutationCatalogue.TypeInvariant,
                PropertyClass = SpecPropertyClass.Invariant,
                Summary = "control",
                Perturbs = [action],
                Edits =
                [
                    new SpecEdit(anchor, "\\* MUTATION: decorative.\n" + anchor),
                    new SpecEdit("Next ==", "Next  =="),
                ],
            };

            Assert.That(
                SpecActions.MutationPerturbs(baseSpec, action, splitClaim),
                Is.False,
                $"a mutation whose only edit inside {action} is a comment, with its real change outside it, was "
                + $"accepted as perturbing {action}.");
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
    /// The parser handles the real disjunct shapes: an unquantified action, one
    /// quantifier, and two nested quantifiers.
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
