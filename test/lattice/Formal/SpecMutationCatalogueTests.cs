using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The gates over each module's mutation catalogue that need no external
/// toolchain: they are string work over <c>spec/</c> and run in milliseconds.
/// Every gate takes a <see cref="SpecModule"/> and runs once per discovered
/// module, with the module in its case name.
/// <para>
/// WHY THIS IS A SEPARATE FIXTURE FROM <see cref="TlcModelCheckTests"/>. These
/// checks need neither a JVM nor <c>tla2tools.jar</c>, so tagging them
/// <c>[Category("Tlc")]</c> alongside the model-checking tests would make them
/// disappear for exactly the contributor they are most useful to: the one
/// without the toolchain, whose <c>Assert.Ignore</c> would take the fast
/// drift signal with it. The drift gate's whole value is failing in
/// milliseconds with the offending anchor named, rather than surfacing later as
/// a confusing TLC parse error - and locally, "later" may be never.
/// </para>
/// <para>
/// Keeping them here also means a change to <c>spec/</c> is still partly gated
/// on a machine with no Java at all, which is the honest floor for what this
/// repository can promise a contributor.
/// </para>
/// </summary>
[TestFixture]
public sealed class SpecMutationCatalogueTests
{
    /// <summary>
    /// Completeness, driven by the model rather than by a hand-maintained list.
    /// Every name in the base cfg's INVARIANTS and PROPERTIES blocks must have
    /// at least one mutation, so adding a property without pairing it fails
    /// here.
    /// <para>
    /// This is the gate that keeps #2323 closed rather than merely satisfied
    /// once. A pairing rule that covers today's properties but not tomorrow's
    /// decays into exactly the state the audit found.
    /// </para>
    /// <para>
    /// At least one, not exactly one. A property may be the one that catches
    /// defects in several protocol actions, and issue #2322 asks for each
    /// action's claim to ship with a mutation of its own; requiring one
    /// mutation per property would force those into a single file or leave the
    /// action rows unpaired. What must stay unique is the mutation, so two
    /// files cannot claim to be the same experiment.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_property_the_base_model_checks_has_a_mutation(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var checkedProperties = SpecMutationCatalogue.ReadCheckedProperties(module.ReadConfig());
        var mutations = module.LoadMutations();
        var paired = mutations.Select(m => m.Target).Distinct(StringComparer.Ordinal).ToArray();

        Assert.That(
            checkedProperties,
            Is.Not.Empty,
            $"parsed no properties out of {module.Describe(module.ConfigPath)}, so this gate would be vacuous.");

        Assert.That(
            paired,
            Is.EquivalentTo(checkedProperties),
            $"every property {module.Describe(module.ConfigPath)} checks needs a mutation in "
            + $"{module.Describe(module.MutationDirectory)}/ that makes it fire, and every mutation needs to target a "
            + "property the model actually checks. "
            + $"Model checks: [{string.Join(", ", checkedProperties.Order(StringComparer.Ordinal))}]. "
            + $"Mutations target: [{string.Join(", ", paired.Order(StringComparer.Ordinal))}].");

        Assert.That(
            mutations.Select(m => m.Module),
            Is.Unique,
            "two mutations generate the same module name, so one mutant would overwrite the other.");
    }

    /// <summary>
    /// The pairing gate above compares two sets, so it stays green when a
    /// property and its mutation are deleted TOGETHER: both sides shrink and
    /// nothing notices. That is not hypothetical tidiness - it is how a
    /// coverage claim decays quietly, which is the audit's subject. These
    /// counts are the floor. They come from the module's manifest, and
    /// <see cref="SpecModuleDiscoveryTests"/> checks the module README's counts
    /// table against the same manifest, so deleting a property makes the README
    /// false AND the build red in the same commit.
    /// <para>
    /// Deliberately an equality, not a minimum. Adding a property should
    /// require touching the documented count, because the documented count is a
    /// claim about coverage that somebody has to re-make on purpose.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_model_checks_the_documented_number_of_invariants_and_properties(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(module.ReadConfig());
        var counts = module.Manifest.Counts;
        var remedy = $"If that changed on purpose, update {module.Describe(module.ManifestPath)} AND the counts table in "
            + $"{module.Describe(module.ReadmePath)}, which is otherwise now false.";

        Assert.Multiple(() =>
        {
            Assert.That(
                blocks[SpecMutationCatalogue.InvariantsBlock],
                Has.Count.EqualTo(counts.Invariants),
                $"{module.Describe(module.ConfigPath)} should check {counts.Invariants} invariants. {remedy}");

            Assert.That(
                blocks[SpecMutationCatalogue.PropertiesBlock],
                Has.Count.EqualTo(counts.Properties),
                $"{module.Describe(module.ConfigPath)} should check {counts.Properties} action and temporal "
                + $"properties. {remedy}");
        });
    }

    /// <summary>
    /// The same floor for the catalogue itself. Without it, deleting a second
    /// mutation of a property that has two would pass every gate, since the
    /// property is still paired, and the action that mutation perturbed might
    /// still be perturbed by another.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_catalogue_holds_the_documented_number_of_mutations(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        Assert.That(
            module.LoadMutations(),
            Has.Count.EqualTo(module.Manifest.Counts.Mutations),
            $"{module.Describe(module.MutationDirectory)}/ should hold {module.Manifest.Counts.Mutations} mutations. "
            + $"If that changed on purpose, update {module.Describe(module.ManifestPath)} and the counts table in "
            + $"{module.Describe(module.ReadmePath)}.");
    }

    /// <summary>
    /// Every generated cfg checks <c>TypeOK</c> alongside its target, so that a
    /// mutation which accidentally leaves a variable's domain reports as
    /// <c>TypeOK</c> rather than as a confident pairing. That guard is only
    /// real if the module defines a type invariant and its base model checks
    /// it; otherwise every generated cfg names a property TLC cannot find.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_base_model_checks_the_type_invariant(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        Assert.That(
            SpecMutationCatalogue.ReadCheckedPropertiesByBlock(module.ReadConfig())[SpecMutationCatalogue.InvariantsBlock],
            Does.Contain(SpecMutationCatalogue.TypeInvariant),
            $"{module.Describe(module.ConfigPath)} must check {SpecMutationCatalogue.TypeInvariant} under INVARIANTS. "
            + "Every generated mutation cfg carries it so that an out-of-domain mutation cannot pass as a pairing.");
    }

    /// <summary>
    /// A variant configuration checks the type invariant too, for the same
    /// reason the base does: a variant is a second claim about the same
    /// specification, and a bound changed by override can put a variable out of
    /// its declared domain without any other invariant noticing.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Variants))]
    public void Every_variant_configuration_checks_the_type_invariant(SpecModule module, string variant)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentException.ThrowIfNullOrEmpty(variant);

        Assert.That(
            SpecMutationCatalogue.ReadCheckedPropertiesByBlock(module.ReadVariantConfig(variant))[SpecMutationCatalogue.InvariantsBlock],
            Does.Contain(SpecMutationCatalogue.TypeInvariant),
            $"{module.Describe(module.VariantConfigPath(variant))} must check {SpecMutationCatalogue.TypeInvariant} under INVARIANTS.");
    }

    /// <summary>
    /// Every name a variant configuration assigns or overrides belongs to the
    /// specification, and the variant changes at least one assignment the base
    /// configuration makes.
    /// <para>
    /// WHY THIS EXISTS. TLC rejects an override whose name it cannot find
    /// (<c>Typo &lt;- Other</c>), but it ACCEPTS a value assignment to a name
    /// the specification does not have (<c>MaxFalts = 2</c>) and silently checks
    /// the unchanged model. A variant with a misspelt bound therefore passes as a
    /// second, larger check while re-checking the base - the exact vacuity this
    /// harness exists to refuse. The TLC gate catches the same mistake from the
    /// other side, by requiring the variant's state count to differ from the
    /// base's; this one catches it without a toolchain and names the culprit.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Variants))]
    public void Every_name_a_variant_configuration_assigns_belongs_to_the_specification(SpecModule module, string variant)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentException.ThrowIfNullOrEmpty(variant);

        var where = module.Describe(module.VariantConfigPath(variant));
        var assignments = SpecMutationCatalogue.ReadConstantAssignments(module.ReadVariantConfig(variant));
        var baseAssignments = SpecMutationCatalogue.ReadConstantAssignments(module.ReadConfig());
        var (declared, defined) = SpecificationNames(module);

        Assert.Multiple(() =>
        {
            Assert.That(
                assignments.Except(baseAssignments),
                Is.Not.Empty,
                $"{where} makes no assignment the base configuration does not already make, so it checks the base "
                + "under the base's own bound. A variant exists to change a bound; state the change in its CONSTANTS "
                + "block.");

            foreach (var assignment in assignments)
            {
                Assert.That(
                    declared(assignment.Name) || defined(assignment.Name),
                    Is.True,
                    $"{where} assigns '{assignment.Name}', which {module.Name} neither declares as a CONSTANT nor "
                    + "defines. TLC would accept the value assignment and silently check the unchanged model.");

                if (assignment.IsOverride)
                {
                    Assert.That(
                        defined(assignment.Value),
                        Is.True,
                        $"{where} overrides '{assignment.Name}' with '{assignment.Value}', which {module.Name} does not define.");
                }
            }
        });
    }

    /// <summary>
    /// Encodes the first of the two supporting controls issue #2323 asks for:
    /// a liveness property is declared under <c>PROPERTIES</c>, never under
    /// <c>INVARIANTS</c>.
    /// <para>
    /// It matters because TLC does not reject the mistake. An invariant is a
    /// state predicate, so a temporal formula placed in the INVARIANTS block is
    /// evaluated per state rather than over behaviours, and the run still
    /// completes - reporting a green that means something weaker than, and
    /// different from, what the name promises. That is the audit's shape
    /// exactly, and until this test the correct layout was merely inherited
    /// from whoever wrote the cfg. Nothing stopped it being undone.
    /// </para>
    /// <para>
    /// The mutation catalogue is the source of truth for which properties are
    /// temporal, because <see cref="SpecMutation.PropertyClass"/> already has
    /// to be right for the banner assertion to work. A module with no temporal
    /// property passes this vacuously, which is correct for that module; that
    /// the gate checks something somewhere is
    /// <see cref="The_temporal_layout_gate_checks_at_least_one_property"/>.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Temporal_properties_are_declared_under_PROPERTIES_not_INVARIANTS(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(module.ReadConfig());
        var invariantsBlock = blocks[SpecMutationCatalogue.InvariantsBlock];
        var propertiesBlock = blocks[SpecMutationCatalogue.PropertiesBlock];
        var cfg = module.Describe(module.ConfigPath);

        Assert.Multiple(() =>
        {
            foreach (var name in TemporalTargets(module))
            {
                Assert.That(
                    invariantsBlock,
                    Does.Not.Contain(name),
                    $"'{name}' is a temporal property but {cfg} declares it under INVARIANTS. "
                    + "TLC will evaluate it as a state predicate and still report success, so the green run "
                    + "would mean something weaker than the name claims. Move it to the PROPERTIES block.");

                Assert.That(
                    propertiesBlock,
                    Does.Contain(name),
                    $"'{name}' is a temporal property and must appear in {cfg}'s PROPERTIES "
                    + "block for TLC to check it over behaviours.");
            }
        });
    }

    /// <summary>
    /// The vacuity floor for the gate above, taken across every module: at
    /// least one mutation in the repository targets a temporal property, so the
    /// layout gate is checking something.
    /// </summary>
    [Test]
    public void The_temporal_layout_gate_checks_at_least_one_property()
    {
        Assert.That(
            SpecModuleCatalogue.Repository().SelectMany(TemporalTargets),
            Is.Not.Empty,
            "no mutation in any module declares CLASS: Temporal, so the temporal layout gate checks nothing.");
    }

    /// <summary>
    /// The <c>DEADLOCK: off</c> header is what licenses TLC's
    /// <c>-deadlock</c> switch, and nothing else may. This pins the mapping
    /// without a JVM: a mutation declaring the header gets exactly the switch,
    /// every other mutation gets no extra options at all. Whether a declaring
    /// mutation actually needs it is checked by <see cref="TlcModelCheckTests"/>,
    /// which runs it once with the check left on.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Deadlock_declarations_map_to_the_TLC_switch_and_nothing_else(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        Assert.Multiple(() =>
        {
            foreach (var mutation in module.LoadMutations())
            {
                Assert.That(
                    mutation.TlcOptions,
                    Is.EqualTo(mutation.DeadlockCheckDisabled ? new[] { SpecMutation.DeadlockSwitch } : Array.Empty<string>()),
                    $"mutation '{mutation.Name}' would run TLC with options its header does not declare.");
            }
        });
    }

    /// <summary>
    /// The vacuity floor for the mapping above, across every module: the
    /// header is in use somewhere, so the mapping is checked over something.
    /// </summary>
    [Test]
    public void Some_mutation_declares_the_deadlock_header()
    {
        Assert.That(
            SpecModuleCatalogue.Repository().SelectMany(m => m.LoadMutations()).Where(m => m.DeadlockCheckDisabled),
            Is.Not.Empty,
            "no mutation in any module declares DEADLOCK: off, so its mapping is checked over nothing. If that "
            + "is now intended, delete this test together with the header's support.");
    }

    /// <summary>
    /// The header's parse rules: <c>off</c> is the only accepted value, an
    /// absent header leaves the deadlock check on, and anything else - a typo,
    /// or <c>on</c>, which is the default and so reads like a decision while
    /// deciding nothing - is refused rather than quietly falling back.
    /// </summary>
    [Test]
    public void A_deadlock_header_is_off_or_absent()
    {
        static SpecMutation ParseWith(string header) => SpecMutationCatalogue.Parse(
            "DeadlockHeaderControl",
            "MODULE: DeadlockHeaderControl\nTARGET: Termination\nCLASS: Temporal\nSUMMARY: control\n"
            + header
            + "\n--- FIND\nx\n--- REPLACE\ny\n--- END\n");

        Assert.Multiple(() =>
        {
            Assert.That(ParseWith("DEADLOCK: off\n").DeadlockCheckDisabled, Is.True);
            Assert.That(ParseWith(string.Empty).DeadlockCheckDisabled, Is.False);
            Assert.That(() => ParseWith("DEADLOCK: on\n"), Throws.InvalidOperationException);
            Assert.That(() => ParseWith("DEADLOCK: Off\n"), Throws.InvalidOperationException);
        });
    }

    /// <summary>
    /// Mutation names are test-case names and mutant module names are the
    /// files TLC is handed, so both must be unique across every module, and no
    /// mutant may take a base module's name. A collision would make a
    /// <c>--filter</c> on one name run two experiments, or make a mutant
    /// overwrite the base it is compared against.
    /// </summary>
    [Test]
    public void Mutation_names_are_unique_across_every_module()
    {
        var modules = SpecModuleCatalogue.Repository();
        var mutations = modules.SelectMany(m => m.LoadMutations()).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(mutations.Select(m => m.Name), Is.Unique, "two modules declare mutations with the same file name.");
            Assert.That(mutations.Select(m => m.Module), Is.Unique, "two modules declare mutations with the same MODULE.");
            Assert.That(
                mutations.Select(m => m.Module).Intersect(modules.Select(m => m.Name), StringComparer.Ordinal),
                Is.Empty,
                "a mutation's MODULE equals a base module's name, so its mutant would overwrite that base.");
        });
    }

    /// <summary>
    /// The drift gate, and deliberately cheap: it applies every mutation to the
    /// current base without running TLC, so an edit to a module that
    /// invalidates an anchor fails in milliseconds with a message naming the
    /// mutation and the exact anchor text, instead of surfacing as a confusing
    /// TLC parse error minutes later.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_mutation_applies_cleanly_to_the_current_base_specification(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var baseSpec = module.ReadSpecification();
        var mutations = module.LoadMutations();

        Assert.That(mutations, Is.Not.Empty, $"expected at least one mutation in {module.Describe(module.MutationDirectory)}/.");

        Assert.Multiple(() =>
        {
            foreach (var mutation in mutations)
            {
                Assert.DoesNotThrow(
                    () => mutation.Apply(baseSpec, module.Name),
                    $"mutation '{mutation.Name}' no longer applies to {module.Describe(module.SpecificationPath)}.");
            }
        });
    }

    /// <summary>
    /// A mutation whose edits are all identity replacements applies cleanly,
    /// produces a mutant that differs from the base only in its module header,
    /// and therefore model-checks green - so the pairing test fails, but only
    /// after a TLC run and with a message about vacuity rather than about the
    /// typo that caused it. This catches the same fault in a millisecond and
    /// names it precisely.
    /// <para>
    /// It is the standing form of a negative experiment that was run by hand
    /// once: restoring the fairness conjunct into the <c>Termination</c>
    /// mutation neutered it, and nothing cheap noticed.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_mutation_actually_changes_something(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var mutations = module.LoadMutations();

        Assert.That(mutations, Is.Not.Empty, $"expected at least one mutation in {module.Describe(module.MutationDirectory)}/.");

        Assert.Multiple(() =>
        {
            foreach (var mutation in mutations)
            {
                Assert.That(
                    mutation.Edits.Any(e => !string.Equals(e.Find, e.Replace, StringComparison.Ordinal)),
                    Is.True,
                    $"every edit in mutation '{mutation.Name}' replaces its anchor with itself, so the "
                    + "generated mutant is the base specification under another name and cannot make "
                    + $"'{mutation.Target}' fire. A mutation that changes nothing proves nothing.");
            }
        });
    }

    /// <summary>
    /// Encodes the trap that issue #2323 asks the harness to control for: a cfg
    /// authored by prepending a property to an existing PROPERTIES block rather
    /// than replacing it, which yielded six conjuncts instead of the intended
    /// one, named the same property in two blocks, and misattributed two
    /// separate violations. That produced a confidently wrong headline which
    /// took four independent routes to overturn.
    /// <para>
    /// <see cref="SpecMutation.BuildConfig"/> writes each cfg whole rather than
    /// editing one, so the fault is currently unreachable by construction. That
    /// is exactly the claim this epic exists to distrust: "true by
    /// construction" and "asserted" are different states, and only the second
    /// survives somebody refactoring the generator into an edit. The check is
    /// cheap, so there is no reason to owe it to a code-reading.
    /// </para>
    /// <para>
    /// The header count is checked separately from the parsed names because
    /// <see cref="SpecMutationCatalogue.ReadCheckedPropertiesByBlock"/>
    /// accumulates a repeated block into one list. A duplicated PROPERTIES
    /// header is therefore invisible in the parse and visible only here.
    /// </para>
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_generated_config_names_its_target_once_in_a_single_block(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var baseConfig = module.ReadConfig();
        var mutations = module.LoadMutations();

        Assert.That(mutations, Is.Not.Empty, $"expected at least one mutation in {module.Describe(module.MutationDirectory)}/.");

        Assert.Multiple(() =>
        {
            foreach (var mutation in mutations)
            {
                var cfg = mutation.BuildConfig(baseConfig);
                var lines = cfg.ReplaceLineEndings("\n").Split('\n').Select(l => l.Trim()).ToArray();

                var invariantHeaders = lines.Count(l =>
                    l.StartsWith("INVARIANT", StringComparison.Ordinal));
                var propertyHeaders = lines.Count(l =>
                    l.StartsWith("PROPERT", StringComparison.Ordinal));

                Assert.That(
                    invariantHeaders,
                    Is.EqualTo(1),
                    $"the cfg generated for mutation '{mutation.Name}' has {invariantHeaders} INVARIANTS "
                    + "blocks. TLC accumulates them, so the run would check more than the one target this "
                    + "experiment isolates and a violation could be attributed to the wrong property.");

                Assert.That(
                    propertyHeaders,
                    Is.LessThanOrEqualTo(1),
                    $"the cfg generated for mutation '{mutation.Name}' has {propertyHeaders} PROPERTIES "
                    + "blocks. That is the authoring mistake #2323 records: the conjuncts accumulate and "
                    + "two separate violations get misattributed to one property.");

                var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(cfg);
                var invariants = blocks[SpecMutationCatalogue.InvariantsBlock];
                var properties = blocks[SpecMutationCatalogue.PropertiesBlock];

                Assert.That(
                    invariants.Intersect(properties, StringComparer.Ordinal),
                    Is.Empty,
                    $"the cfg generated for mutation '{mutation.Name}' names the same property under both "
                    + "INVARIANTS and PROPERTIES. TLC then checks it twice by two different semantics, "
                    + "which is how one defect reports as two violations.");

                var occurrences = invariants.Concat(properties)
                    .Count(n => string.Equals(n, mutation.Target, StringComparison.Ordinal));

                Assert.That(
                    occurrences,
                    Is.EqualTo(1),
                    $"the cfg generated for mutation '{mutation.Name}' names its target "
                    + $"'{mutation.Target}' {occurrences} times. The two-arm experiment depends on exactly "
                    + "one target being checked, because the banner assertion reads a single property name.");

                if (mutation.PropertyClass is SpecPropertyClass.Action or SpecPropertyClass.Temporal)
                {
                    Assert.That(
                        properties,
                        Is.EqualTo(new[] { mutation.Target }),
                        $"the cfg generated for mutation '{mutation.Name}' should declare exactly its one "
                        + $"target '{mutation.Target}' under PROPERTIES, and nothing else.");
                }
                else
                {
                    Assert.That(
                        properties,
                        Is.Empty,
                        $"mutation '{mutation.Name}' targets an invariant, so the generated cfg should have "
                        + "no PROPERTIES block at all.");
                }
            }
        });
    }

    /// <summary>
    /// A generated cfg must check the same bounded instance as the base model,
    /// whatever directives the module bounds it with. Everything but the
    /// checked-property blocks is carried over, and a <c>CONSTRAINT</c> after
    /// an <c>INVARIANTS</c> block ends that block rather than being read as two
    /// more invariants.
    /// </summary>
    [Test]
    public void Generated_configs_carry_every_directive_but_the_checked_properties()
    {
        const string config = """
            \* a comment
            CONSTANTS
                t1 = t1
            INVARIANTS
                TypeOK
                Safe
            CONSTRAINT
                Bounded
            SPECIFICATION Spec
            PROPERTIES
                Live
            SYMMETRY Perms
            """;

        var carried = SpecMutationCatalogue.CarriedConfiguration(config).ReplaceLineEndings("\n");
        var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(config);

        Assert.Multiple(() =>
        {
            Assert.That(
                carried,
                Is.EqualTo("CONSTANTS\n    t1 = t1\nCONSTRAINT\n    Bounded\nSPECIFICATION Spec\nSYMMETRY Perms"));
            Assert.That(blocks[SpecMutationCatalogue.InvariantsBlock], Is.EqualTo(new[] { "TypeOK", "Safe" }));
            Assert.That(blocks[SpecMutationCatalogue.PropertiesBlock], Is.EqualTo(new[] { "Live" }));
            Assert.That(
                () => SpecMutationCatalogue.CarriedConfiguration("CONSTANTS\n    t1 = t1\nINVARIANTS\n    TypeOK\n"),
                Throws.InvalidOperationException.With.Message.Contains("SPECIFICATION"),
                "a cfg naming no behaviour was carried into generated cfgs that TLC could not run.");
        });
    }

    /// <summary>
    /// Every bound a mutation's <c>BOUNDS:</c> header assigns is a name the
    /// specification declares or defines. TLC accepts a value assignment to a
    /// name the specification does not have and silently checks the unchanged
    /// model, so a misspelt bound would run the mutant at full size - harmless
    /// to the verdict, but the cost the header exists to cut would come back
    /// unannounced. Checked without a toolchain, naming the culprit.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Every_bound_a_mutation_assigns_belongs_to_the_specification(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var (declared, defined) = SpecificationNames(module);

        Assert.Multiple(() =>
        {
            foreach (var mutation in module.LoadMutations())
            {
                foreach (var bound in mutation.Bounds)
                {
                    Assert.That(
                        declared(bound.Name) || defined(bound.Name),
                        Is.True,
                        $"mutation '{mutation.Name}' bounds '{bound.Name}', which {module.Name} neither declares as a "
                        + "CONSTANT nor defines. TLC would accept the assignment and run the mutant at full size.");
                }
            }
        });
    }

    /// <summary>
    /// The control arm never carries a mutation's bounds and the mutant arm
    /// always does. This is the half of the BOUNDS design the experiment's
    /// meaning rests on: the control decides that the target holds on the base,
    /// and it must decide that over the module's own instance. It is pinned
    /// without a JVM for every mutation in every module; the synthetic control
    /// in <see cref="TlcModelCheckTests"/> proves it again with TLC, using a
    /// bound under which the base itself would fail.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void Only_the_mutant_arm_carries_a_mutations_bounds(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var baseConfig = module.ReadConfig();
        var baseAssignments = SpecMutationCatalogue.ReadConstantAssignments(baseConfig);

        Assert.Multiple(() =>
        {
            foreach (var mutation in module.LoadMutations())
            {
                var control = SpecMutationCatalogue.ReadConstantAssignments(mutation.BuildConfig(baseConfig));
                var mutant = SpecMutationCatalogue.ReadConstantAssignments(mutation.BuildMutantConfig(baseConfig));

                Assert.That(
                    control,
                    Is.EqualTo(baseAssignments),
                    $"the control-arm cfg for mutation '{mutation.Name}' assigns something the base cfg does not, so "
                    + "the control would not check the module's own instance.");
                Assert.That(
                    mutant,
                    Is.EqualTo(baseAssignments.Concat(mutation.Bounds)),
                    $"the mutant-arm cfg for mutation '{mutation.Name}' does not carry exactly its declared bounds.");
            }
        });
    }

    /// <summary>
    /// The vacuity floor for the two gates above, across every module: some
    /// mutation declares bounds, so they check something.
    /// </summary>
    [Test]
    public void Some_mutation_declares_bounds()
    {
        Assert.That(
            SpecModuleCatalogue.Repository().SelectMany(m => m.LoadMutations()).Where(m => m.Bounds.Count > 0),
            Is.Not.Empty,
            "no mutation in any module declares BOUNDS, so the bounds gates check nothing. If that is now "
            + "intended, delete this test together with the header's support.");
    }

    /// <summary>
    /// The header's parse rules: comma-separated <c>Name = value</c> entries
    /// with an integer or identifier value. A definition override, an empty
    /// entry, a repeated name or a malformed value is refused rather than
    /// passed to TLC, which would ignore what it could not resolve.
    /// </summary>
    [Test]
    public void A_bounds_header_holds_value_assignments_and_nothing_else()
    {
        static SpecMutation ParseWith(string header) => SpecMutationCatalogue.Parse(
            "BoundsHeaderControl",
            "MODULE: BoundsHeaderControl\nTARGET: Termination\nCLASS: Temporal\nSUMMARY: control\n"
            + header
            + "\n--- FIND\nx\n--- REPLACE\ny\n--- END\n");

        Assert.Multiple(() =>
        {
            Assert.That(ParseWith(string.Empty).Bounds, Is.Empty);
            Assert.That(
                ParseWith("BOUNDS: MaxWrites = 1, MaxFaults=0\n").Bounds,
                Is.EqualTo(new[] { new CfgAssignment("MaxWrites", false, "1"), new CfgAssignment("MaxFaults", false, "0") }));
            Assert.That(() => ParseWith("BOUNDS: MaxWrites <- One\n"), Throws.InvalidOperationException);
            Assert.That(() => ParseWith("BOUNDS: MaxWrites = 1,\n"), Throws.InvalidOperationException);
            Assert.That(() => ParseWith("BOUNDS: MaxWrites = 1, MaxWrites = 0\n"), Throws.InvalidOperationException);
            Assert.That(() => ParseWith("BOUNDS: MaxWrites = 1 + 1\n"), Throws.InvalidOperationException);
        });
    }

    /// <summary>
    /// The names a module's specification (and its siblings) declares as a
    /// <c>CONSTANT</c> and defines, for the gates that refuse a cfg assignment
    /// TLC would silently ignore.
    /// </summary>
    private static (Func<string, bool> Declared, Func<string, bool> Defined) SpecificationNames(SpecModule module)
    {
        var specifications = module.ReadSiblingSpecifications().Values.Select(SpecActions.StripComments).ToArray();

        bool Defined(string name) => specifications.Any(text =>
            Regex.IsMatch(text, $@"^{Regex.Escape(name)}(\([^)]*\))?\s*==", RegexOptions.Multiline));

        bool Declared(string name) => specifications.Any(text =>
            Regex.Matches(text, @"^\s*CONSTANTS?\b(?<names>[^\n]*(\n[ \t]+[^\n]*)*)", RegexOptions.Multiline)
                .Any(m => Regex.IsMatch(m.Groups["names"].Value, $@"\b{Regex.Escape(name)}\b")));

        return (Declared, Defined);
    }

    private static IEnumerable<string> TemporalTargets(SpecModule module) =>
        module.LoadMutations()
            .Where(m => m.PropertyClass == SpecPropertyClass.Temporal)
            .Select(m => m.Target);
}
