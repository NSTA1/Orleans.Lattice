using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The gates over the mutation catalogue that need no external toolchain: they
/// are string work over <c>spec/</c> and run in milliseconds.
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
    /// The counts <c>spec/README.md</c> and <c>spec/mutations/README.md</c>
    /// state in prose. Asserted here so the prose is machine-checked; see
    /// <see cref="The_model_checks_the_documented_number_of_invariants_and_properties"/>.
    /// </summary>
    private const int ExpectedInvariantCount = 7;

    private const int ExpectedTemporalPropertyCount = 5;

    private static string SpecDirectory => Path.Combine(HygieneRepository.FindRepoRoot(), "spec");

    private static string MutationDirectory => Path.Combine(SpecDirectory, "mutations");

    private static string BaseSpecification => File.ReadAllText(Path.Combine(SpecDirectory, "AtomicCommit.tla"));

    private static string BaseConfig => File.ReadAllText(Path.Combine(SpecDirectory, "AtomicCommit.cfg"));

    private static IReadOnlyList<SpecMutation> Mutations() => SpecMutationCatalogue.Load(MutationDirectory);

    /// <summary>
    /// Completeness, driven by the model rather than by a hand-maintained list.
    /// Every name in the base cfg's INVARIANTS and PROPERTIES blocks must have
    /// a mutation, so adding a property without pairing it fails here.
    /// <para>
    /// This is the gate that keeps #2323 closed rather than merely satisfied
    /// once. A pairing rule that covers today's properties but not tomorrow's
    /// decays into exactly the state the audit found.
    /// </para>
    /// </summary>
    [Test]
    public void Every_property_the_base_model_checks_has_a_mutation()
    {
        var checkedProperties = SpecMutationCatalogue.ReadCheckedProperties(BaseConfig);
        var paired = Mutations().Select(m => m.Target).ToArray();

        Assert.That(
            checkedProperties,
            Is.Not.Empty,
            "parsed no properties out of spec/AtomicCommit.cfg, so this gate would be vacuous.");

        Assert.That(
            paired,
            Is.EquivalentTo(checkedProperties),
            "every property spec/AtomicCommit.cfg checks needs a mutation in spec/mutations/ that makes "
            + "it fire, and every mutation needs to target a property the model actually checks. "
            + $"Model checks: [{string.Join(", ", checkedProperties.Order(StringComparer.Ordinal))}]. "
            + $"Mutations target: [{string.Join(", ", paired.Order(StringComparer.Ordinal))}].");

        Assert.That(paired, Is.Unique, "two mutations target the same property; each needs its own.");
    }

    /// <summary>
    /// The pairing gate above compares two sets, so it stays green when a
    /// property and its mutation are deleted TOGETHER: both sides shrink and
    /// nothing notices. That is not hypothetical tidiness - it is how a
    /// coverage claim decays quietly, which is the audit's subject. These
    /// counts are the floor, and they are the same numbers
    /// <c>spec/README.md</c> and <c>spec/mutations/README.md</c> state in
    /// prose, so deleting a property makes the prose false AND the build red in
    /// the same commit.
    /// <para>
    /// Deliberately an equality, not a minimum. Adding a property should
    /// require touching the documented count, because the documented count is a
    /// claim about coverage that somebody has to re-make on purpose.
    /// </para>
    /// </summary>
    [Test]
    public void The_model_checks_the_documented_number_of_invariants_and_properties()
    {
        var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(BaseConfig);

        Assert.Multiple(() =>
        {
            Assert.That(
                blocks[SpecMutationCatalogue.InvariantsBlock],
                Has.Count.EqualTo(ExpectedInvariantCount),
                $"spec/AtomicCommit.cfg should check {ExpectedInvariantCount} invariants. If that changed on "
                + "purpose, update this count AND the counts stated in spec/README.md and "
                + "spec/mutations/README.md, which are otherwise now false.");

            Assert.That(
                blocks[SpecMutationCatalogue.PropertiesBlock],
                Has.Count.EqualTo(ExpectedTemporalPropertyCount),
                $"spec/AtomicCommit.cfg should check {ExpectedTemporalPropertyCount} temporal properties. If that "
                + "changed on purpose, update this count AND the counts stated in spec/README.md and "
                + "spec/mutations/README.md, which are otherwise now false.");
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
    /// to be right for the banner assertion to work.
    /// </para>
    /// </summary>
    [Test]
    public void Temporal_properties_are_declared_under_PROPERTIES_not_INVARIANTS()
    {
        var blocks = SpecMutationCatalogue.ReadCheckedPropertiesByBlock(BaseConfig);
        var invariantsBlock = blocks[SpecMutationCatalogue.InvariantsBlock];
        var propertiesBlock = blocks[SpecMutationCatalogue.PropertiesBlock];

        var temporal = Mutations()
            .Where(m => m.PropertyClass == SpecPropertyClass.Temporal)
            .Select(m => m.Target)
            .ToArray();

        Assert.That(
            temporal,
            Is.Not.Empty,
            "no mutation declares CLASS: Temporal, so this gate would be vacuous.");

        Assert.Multiple(() =>
        {
            foreach (var name in temporal)
            {
                Assert.That(
                    invariantsBlock,
                    Does.Not.Contain(name),
                    $"'{name}' is a temporal property but spec/AtomicCommit.cfg declares it under INVARIANTS. "
                    + "TLC will evaluate it as a state predicate and still report success, so the green run "
                    + "would mean something weaker than the name claims. Move it to the PROPERTIES block.");

                Assert.That(
                    propertiesBlock,
                    Does.Contain(name),
                    $"'{name}' is a temporal property and must appear in spec/AtomicCommit.cfg's PROPERTIES "
                    + "block for TLC to check it over behaviours.");
            }
        });
    }

    /// <summary>
    /// The drift gate, and deliberately cheap: it applies every mutation to the
    /// current base without running TLC, so an edit to
    /// <c>spec/AtomicCommit.tla</c> that invalidates an anchor fails in
    /// milliseconds with a message naming the mutation and the exact anchor
    /// text, instead of surfacing as a confusing TLC parse error minutes later.
    /// </summary>
    [Test]
    public void Every_mutation_applies_cleanly_to_the_current_base_specification()
    {
        var baseSpec = BaseSpecification;
        var mutations = Mutations();

        Assert.That(mutations, Is.Not.Empty, "expected at least one mutation in spec/mutations/.");

        Assert.Multiple(() =>
        {
            foreach (var mutation in mutations)
            {
                Assert.DoesNotThrow(
                    () => mutation.Apply(baseSpec),
                    $"mutation '{mutation.Name}' no longer applies to spec/AtomicCommit.tla.");
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
    [Test]
    public void Every_mutation_actually_changes_something()
    {
        var mutations = Mutations();

        Assert.That(mutations, Is.Not.Empty, "expected at least one mutation in spec/mutations/.");

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
    [Test]
    public void Every_generated_config_names_its_target_once_in_a_single_block()
    {
        var baseConfig = BaseConfig;
        var mutations = Mutations();

        Assert.That(mutations, Is.Not.Empty, "expected at least one mutation in spec/mutations/.");

        Assert.Multiple(() =>
        {
            foreach (var mutation in mutations)
            {
                var cfg = mutation.BuildConfig(baseConfig);
                var lines = cfg.ReplaceLineEndings("\n").Split('\n').Select(l => l.Trim()).ToArray();

                var invariantHeaders = lines.Count(l =>
                    l.StartsWith(SpecMutationCatalogue.InvariantsBlock, StringComparison.Ordinal));
                var propertyHeaders = lines.Count(l =>
                    l.StartsWith(SpecMutationCatalogue.PropertiesBlock, StringComparison.Ordinal));

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
}
