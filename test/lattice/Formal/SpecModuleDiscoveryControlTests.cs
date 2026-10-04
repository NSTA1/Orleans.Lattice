namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The discovery controls: proof that the Formal gates see a module they were
/// never told about, check it, and report it when it is broken.
/// <para>
/// WHY THIS EXISTS. Every gate in this namespace is parameterised over
/// <see cref="SpecModuleCatalogue"/>, so its green run is only as good as
/// discovery. A module that discovery skipped, or a gate that quietly read a
/// fixed path instead of its module, would leave every case green - and a
/// module the gates cannot see is exactly the vacuity epic #2299 found. These
/// controls build a module in a temp directory, so a pass here cannot be
/// explained by anything the repository's own modules happen to contain.
/// </para>
/// <para>
/// The gates are found by reflection, by signature (see
/// <see cref="SpecModuleGates"/>), so a gate added later is covered without
/// being enrolled. The TLC gates have the same control in
/// <see cref="TlcModelCheckTests.A_synthetic_module_is_model_checked_by_every_TLC_gate"/>,
/// because they need the toolchain and this fixture must not.
/// </para>
/// </summary>
[TestFixture]
public sealed class SpecModuleDiscoveryControlTests
{
    /// <summary>
    /// The fixtures that must each contribute at least one per-module gate. A
    /// floor on the reflection, so a change that hid a whole fixture from it -
    /// a gate signature changed, a namespace moved - fails here rather than
    /// shrinking the control's reach in silence.
    /// </summary>
    private static readonly Type[] GateFixtures =
    [
        typeof(TlcModelCheckTests),
        typeof(SpecMutationCatalogueTests),
        typeof(SpecActionMutationCoverageTests),
        typeof(SpecModuleDiscoveryTests),
        typeof(RefinementNoteTests),
        typeof(RefinementPropertyCoverageTests),
        typeof(RefinementMappingStalenessTests),
        typeof(RefinementDetectorMappingTests),
    ];

    /// <summary>
    /// Every per-module gate draws its cases from discovery, through a source
    /// on <see cref="SpecModuleCases"/> that has a per-module expander. A gate
    /// with any other source - a hand-written list, a fixed path - would check
    /// only what it was given, and the synthetic control could not reach it.
    /// </summary>
    [Test]
    public void Every_module_gate_draws_its_cases_from_discovery()
    {
        var gates = SpecModuleGates.All();

        Assert.Multiple(() =>
        {
            Assert.That(
                gates.Select(g => g.Fixture).Distinct(),
                Is.SupersetOf(GateFixtures),
                "a Formal fixture that should declare per-module gates contributes none, so the reflection the "
                + "discovery controls rely on cannot see it.");

            foreach (var gate in gates)
            {
                Assert.That(
                    gate.Expander,
                    Is.Not.Null,
                    $"{gate.Name} takes a SpecModule but does not draw its cases from a {nameof(SpecModuleCases)} source "
                    + "with a matching '<Source>For(SpecModule)' expander, so it does not check every discovered module.");
            }
        });
    }

    /// <summary>
    /// A module built in a temp directory is discovered, and every
    /// toolchain-free gate runs over it and passes. Each gate must run at least
    /// one case, so a gate whose expander yields nothing for a module cannot
    /// pass here by doing nothing.
    /// </summary>
    [Test]
    public void A_synthetic_module_is_discovered_and_passes_every_gate()
    {
        using var synthetic = SyntheticSpecModule.Create();

        var discovered = SpecModuleCatalogue.Discover(synthetic.SpecRoot, minimumModules: 1);
        Assert.That(discovered.Select(m => m.Name), Is.EqualTo(new[] { SyntheticSpecModule.ModuleName }));

        var module = discovered[0];
        var gates = SpecModuleGates.All().Where(g => !g.RunsTlc).ToArray();
        Assert.That(gates, Is.Not.Empty, "found no toolchain-free gates to run.");

        foreach (var gate in gates)
        {
            Assert.That(
                SpecModuleGates.Run(gate, module),
                Is.GreaterThan(0),
                $"{gate.Name} ran no case for the synthetic module, so it would pass a module without checking it.");
        }
    }

    /// <summary>
    /// One way to break a well-formed module, and the gate that owns the fault.
    /// </summary>
    /// <param name="Name">The case name.</param>
    /// <param name="Gate">The name of the gate method that must fail.</param>
    /// <param name="Break">Breaks the synthetic module.</param>
    public sealed record Breakage(string Name, string Gate, Action<SyntheticSpecModule> Break)
    {
        /// <inheritdoc />
        public override string ToString() => Name;
    }

    private static readonly string ManifestFile = SyntheticSpecModule.ModuleName + SpecModuleManifest.FileSuffix;

    private static readonly string MutationFile = $"mutations/{SyntheticSpecModule.MutationName}.mutation";

    private static readonly string VariantFile = $"{SyntheticSpecModule.ModuleName}.{SyntheticSpecModule.VariantName}.cfg";

    /// <summary>The broken copies, each paired with the gate that must report it.</summary>
    public static IEnumerable<Breakage> Breakages() =>
    [
        new("invariant count differs from manifest", nameof(SpecMutationCatalogueTests.The_model_checks_the_documented_number_of_invariants_and_properties),
            s => s.Replace(ManifestFile, "\"invariants\": 1", "\"invariants\": 2")),
        new("README count differs from manifest", nameof(SpecModuleDiscoveryTests.The_module_README_states_the_manifest_counts),
            s => s.Replace("README.md", "| 2 | 3 |", "| 2 | 4 |")),
        new("module missing from index", nameof(SpecModuleDiscoveryTests.The_module_is_listed_in_the_spec_index),
            s => s.Replace("README.md", "| `Synthetic` |", "| `Other` |", atRoot: true)),
        new("property with no mutation", nameof(SpecMutationCatalogueTests.Every_property_the_base_model_checks_has_a_mutation),
            s => s.Replace($"{SyntheticSpecModule.ModuleName}.cfg", "    TypeOK", "    TypeOK\n    Unpaired")),
        new("type invariant unchecked", nameof(SpecMutationCatalogueTests.The_base_model_checks_the_type_invariant),
            s => s.Replace($"{SyntheticSpecModule.ModuleName}.cfg", "    TypeOK", "    Safe")),
        new("mutation count differs from manifest", nameof(SpecMutationCatalogueTests.The_catalogue_holds_the_documented_number_of_mutations),
            s => s.Write("mutations/Second.mutation", File.ReadAllText(Path.Combine(s.ModuleDirectory, MutationFile)).Replace("MODULE: SyntheticTypeOkStepOverflows", "MODULE: SyntheticSecond", StringComparison.Ordinal))),
        new("mutation anchor drifted", nameof(SpecMutationCatalogueTests.Every_mutation_applies_cleanly_to_the_current_base_specification),
            s => s.Replace(MutationFile, "--- FIND\n    /\\ x' = (x + 1) % Wrap", "--- FIND\n    /\\ x' = (x + 2) % Wrap")),
        new("mutation changes nothing", nameof(SpecMutationCatalogueTests.Every_mutation_actually_changes_something),
            s => s.Replace(MutationFile, "--- REPLACE\n    /\\ x' = (x + 1) % (Wrap + 1)", "--- REPLACE\n    /\\ x' = (x + 1) % Wrap")),
        new("action count differs from manifest", nameof(SpecActionMutationCoverageTests.The_specification_and_catalogue_yield_actions_and_perturbations_to_check),
            s => s.Replace(ManifestFile, "\"actions\": 1", "\"actions\": 2")),
        new("action row names the wrong action", nameof(SpecActionMutationCoverageTests.The_refinement_action_table_maps_exactly_the_actions_in_Next),
            s => s.Replace("Refinement.md", "| `Step` |", "| `Leap` |")),
        new("action perturbed by no mutation", nameof(SpecActionMutationCoverageTests.Every_protocol_action_is_perturbed_by_a_mutation),
            s => s.Replace(MutationFile, "PERTURBS: Step\n", string.Empty)),
        new("property neither mapped nor excluded", nameof(RefinementPropertyCoverageTests.Every_checked_property_is_mapped_or_explicitly_excluded),
            s => s.Replace("Refinement.md", "| `TypeOK` |", "| `Bogus` |")),
        new("note names a symbol that does not exist", nameof(RefinementMappingStalenessTests.Every_code_symbol_named_in_the_refinement_mapping_still_exists),
            s => s.Replace("Refinement.md", "`AtomicWriteGrain.RecordTerminalDecisionAsync` stands in", "`NoSuchType.NoSuchMember` stands in")),
        new("detector names a test that does not exist", nameof(RefinementDetectorMappingTests.Every_test_named_as_a_detector_exists),
            s => s.Replace("Refinement.md", $"RecordTerminalDecisionAsync`. | Yes: `{SyntheticSpecModule.Detector}`", "RecordTerminalDecisionAsync`. | Yes: `SpecModuleDiscoveryControlTests.No_such_detector`")),
        new("behaviour row count differs from manifest", nameof(RefinementDetectorMappingTests.The_note_yields_the_expected_behaviour_asserting_denominator),
            s => s.Replace(ManifestFile, "\"behaviourRows\": 2", "\"behaviourRows\": 3")),
        new("variant assigns a name the specification lacks", nameof(SpecMutationCatalogueTests.Every_name_a_variant_configuration_assigns_belongs_to_the_specification),
            s => s.Replace(VariantFile, "    Wrap = 2", "    Wrpa = 2")),
        new("variant overrides with an undefined name", nameof(SpecMutationCatalogueTests.Every_name_a_variant_configuration_assigns_belongs_to_the_specification),
            s => s.Replace(VariantFile, "    Wrap = 2", "    Wrap <- Narrower")),
        new("variant changes no assignment", nameof(SpecMutationCatalogueTests.Every_name_a_variant_configuration_assigns_belongs_to_the_specification),
            s => s.Replace(VariantFile, "CONSTANTS\n    Wrap = 2\n", string.Empty)),
        new("variant does not check the type invariant", nameof(SpecMutationCatalogueTests.Every_variant_configuration_checks_the_type_invariant),
            s => s.Replace(VariantFile, "    TypeOK", "    Safe")),
        new("note states a census count", nameof(RefinementDetectorMappingTests.The_note_records_no_hand_maintained_census_count),
            s => s.Replace("Refinement.md", "for the Formal discovery controls.", "for the Formal discovery controls. The census found ten rows detected, three partial or undetected.")),
    ];

    /// <summary>
    /// A broken synthetic module is reported, not silently passed: each
    /// breakage makes the gate that owns it fail. Run in an isolated assertion
    /// context, so the expected failure is observed rather than recorded.
    /// </summary>
    [TestCaseSource(nameof(Breakages))]
    public void A_broken_synthetic_module_fails_the_gate_that_owns_the_fault(Breakage breakage)
    {
        ArgumentNullException.ThrowIfNull(breakage);

        using var synthetic = SyntheticSpecModule.Create();
        breakage.Break(synthetic);
        var module = synthetic.Discover();

        var gate = SpecModuleGates.All().Single(g => g.Method.Name == breakage.Gate);
        var failure = Assert.Catch(() => SpecModuleGates.Run(gate, module));

        Assert.That(
            failure,
            Is.Not.Null,
            $"{gate.Name} passed a synthetic module broken by '{breakage.Name}'. A gate that cannot see the fault it "
            + "owns in a module it has never seen is not checking that module.");
    }

    /// <summary>One malformed module directory and a fragment of the error discovery must raise.</summary>
    /// <param name="Name">The case name.</param>
    /// <param name="Expected">A fragment the error must contain.</param>
    /// <param name="Break">Makes the synthetic root malformed.</param>
    public sealed record Malformation(string Name, string Expected, Action<SyntheticSpecModule> Break)
    {
        /// <inheritdoc />
        public override string ToString() => Name;
    }

    /// <summary>The malformed shapes discovery must refuse rather than skip.</summary>
    public static IEnumerable<Malformation> Malformations() =>
    [
        new("tla without cfg", "has no Synthetic.cfg", s => s.Delete($"{SyntheticSpecModule.ModuleName}.cfg")),
        new("helper tla without cfg", "Helper.tla has no Helper.cfg", s => s.Write("Helper.tla", "---- MODULE Helper ----\n====")),
        new("cfg without tla", "Orphan.cfg has no Orphan.tla", s => s.Write("Orphan.cfg", "SPECIFICATION Spec")),
        new("manifest without tla", "Orphan.manifest.json has no Orphan.tla", s => s.Write("Orphan.manifest.json", "{}")),
        new("tla without manifest", "has no Synthetic.manifest.json", s => s.Delete(ManifestFile)),
        new("manifest is not JSON", "not valid JSON", s => s.Write(ManifestFile, "{")),
        new("manifest has an unknown key", "Unknown: [extra]", s => s.Replace(ManifestFile, "\"mutations\": \"mutations\",", "\"mutations\": \"mutations\", \"extra\": 1,")),
        new("manifest lacks a count", "Missing: [distinctStates]", s => s.Replace(ManifestFile, ",\n    \"distinctStates\": 3", string.Empty)),
        new("manifest count is zero", "counts.mutations", s => s.Replace(ManifestFile, "\"mutations\": 1", "\"mutations\": 0")),
        new("manifest names a missing mutation directory", "mutation directory 'missing' does not exist", s => s.Replace(ManifestFile, "\"mutations\": \"mutations\"", "\"mutations\": \"missing\"")),
        new("manifest path escapes the module", "inside the module directory", s => s.Replace(ManifestFile, "\"Refinement.md\"", "\"../Refinement.md\"")),
        new("module README missing", "has no README.md", s => s.Delete("README.md")),
        new("module header does not match file", "does not open with '---- MODULE Synthetic ----'", s => s.Replace($"{SyntheticSpecModule.ModuleName}.tla", "---- MODULE Synthetic ----", "---- MODULE Other ----")),
        new("directory with no module", "spec/empty/ contains no .tla module", s => s.Write("empty/notes.txt", "not a module", atRoot: true)),
        new("module left directly in spec", "Stray.tla sits directly in spec/", s => s.Write("Stray.tla", "---- MODULE Stray ----\n====", atRoot: true)),
        new("module name declared twice", "is declared in more than one directory", s => s.CopyModuleTo("copy")),
        new("variant cfg the manifest does not declare", "does not declare under 'variants'", s => s.Replace(ManifestFile, "},\n  \"variants\": {\n    \"Narrow\": { \"distinctStates\": 2 }\n  }", "}")),
        new("manifest variant with no cfg", "declares variant 'Narrow', but Synthetic.Narrow.cfg does not exist", s => s.Delete(VariantFile)),
        new("variant cfg of no module", "is a variant configuration of Orphan, but there is no Orphan.tla", s => s.Write("Orphan.Narrow.cfg", "SPECIFICATION Spec")),
        new("variant name malformed", "variant name '2x'", s => s.Write("Synthetic.2x.cfg", "SPECIFICATION Spec")),
        new("manifest variants empty", "must be a non-empty object", s => s.Replace(ManifestFile, "\"Narrow\": { \"distinctStates\": 2 }", string.Empty)),
    ];

    /// <summary>
    /// A malformed module directory fails discovery loudly, naming the
    /// problem. Skipping it instead would leave the module outside every gate
    /// while the gates stayed green.
    /// </summary>
    [TestCaseSource(nameof(Malformations))]
    public void A_malformed_module_directory_fails_discovery(Malformation malformation)
    {
        ArgumentNullException.ThrowIfNull(malformation);

        using var synthetic = SyntheticSpecModule.Create();
        Assert.That(() => synthetic.Discover(), Throws.Nothing, "the unbroken synthetic module must be discovered.");

        malformation.Break(synthetic);

        Assert.That(
            () => SpecModuleCatalogue.Discover(synthetic.SpecRoot),
            Throws.InvalidOperationException.With.Message.Contains(malformation.Expected));
    }

    /// <summary>
    /// The population floor: a root with no module is refused when a floor is
    /// asked for, which is how the repository's discovery is always called.
    /// </summary>
    [Test]
    public void An_empty_spec_root_fails_the_population_floor()
    {
        using var synthetic = SyntheticSpecModule.Create();
        Directory.Delete(synthetic.ModuleDirectory, recursive: true);

        Assert.Multiple(() =>
        {
            Assert.That(SpecModuleCatalogue.Discover(synthetic.SpecRoot), Is.Empty);
            Assert.That(
                () => SpecModuleCatalogue.Discover(synthetic.SpecRoot, SpecModuleCatalogue.MinimumRepositoryModules),
                Throws.InvalidOperationException.With.Message.Contains("fewer than the floor"));
        });
    }
}
