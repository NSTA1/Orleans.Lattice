namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The gates over module discovery itself: the population floor, each
/// module's README counts table against its manifest, and the module index in
/// <c>spec/README.md</c>.
/// <para>
/// The counts table is how the manifest's counts reach a reader. Every count
/// gate elsewhere compares the module against its manifest; this compares the
/// README against the same manifest, so a count changed in one place and not
/// the other fails here, and the prose a reader trusts cannot fall behind the
/// numbers the build enforces.
/// </para>
/// <para>
/// The index is the second, independent record of which modules exist. A module
/// directory deleted or renamed makes the index disagree with discovery, so a
/// module cannot leave the gates' sight without somebody editing the index on
/// purpose.
/// </para>
/// </summary>
[TestFixture]
public sealed class SpecModuleDiscoveryTests
{
    /// <summary>
    /// The floor, stated as a passing test so a reader can see it. Discovery
    /// enforces it too, which makes every gate's case source fail rather than
    /// yield nothing.
    /// </summary>
    [Test]
    public void Discovery_finds_at_least_the_floor_of_modules()
    {
        Assert.That(
            SpecModuleCatalogue.Repository(),
            Has.Count.GreaterThanOrEqualTo(SpecModuleCatalogue.MinimumRepositoryModules),
            "discovery under spec/ returned fewer modules than the floor, so every Formal gate would be vacuous.");
    }

    /// <summary>
    /// The module's README states exactly the counts its manifest records,
    /// and lists no module its directory does not hold.
    /// </summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_module_README_states_the_manifest_counts(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var readme = module.Describe(module.ReadmePath);
        var table = SpecModuleReadme.ReadCounts(module.ReadReadme(), readme);
        var siblings = SpecModuleCatalogue.Discover(module.SpecRoot)
            .Where(m => string.Equals(m.Directory, module.Directory, StringComparison.Ordinal))
            .Select(m => m.Name)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                table.Keys,
                Is.EquivalentTo(siblings),
                $"{readme}'s '## {SpecModuleReadme.CountsSection}' table must have one row per module in its directory.");

            Assert.That(
                table.TryGetValue(module.Name, out var stated) ? stated : null,
                Is.EqualTo(module.Manifest.Counts),
                $"{readme}'s counts for {module.Name} differ from {module.Describe(module.ManifestPath)}. The README "
                + "is the prose a reader trusts and the manifest is what the gates enforce; change them together.");
        });
    }

    /// <summary>The module appears in the <c>spec/README.md</c> index.</summary>
    [TestCaseSource(typeof(SpecModuleCases), nameof(SpecModuleCases.Modules))]
    public void The_module_is_listed_in_the_spec_index(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var index = Path.Combine(module.SpecRoot, SpecModule.ReadmeFileName);
        var source = module.Describe(index);

        Assert.That(
            SpecModuleReadme.ReadIndex(File.ReadAllText(index), source),
            Does.Contain(module.Name),
            $"{source}'s '## {SpecModuleReadme.IndexSection}' index does not list {module.Name}.");
    }

    /// <summary>
    /// The index lists nothing discovery cannot see, and nothing twice. The
    /// converse of the per-module gate above: a module removed from disk but
    /// left in the index fails here.
    /// </summary>
    [Test]
    public void The_spec_index_lists_only_discovered_modules()
    {
        var index = Path.Combine(SpecModuleCatalogue.RepositorySpecRoot, SpecModule.ReadmeFileName);
        var listed = SpecModuleReadme.ReadIndex(File.ReadAllText(index), "spec/README.md");

        Assert.Multiple(() =>
        {
            Assert.That(listed, Is.Unique, "spec/README.md's module index lists a module twice.");
            Assert.That(
                listed,
                Is.SubsetOf(SpecModuleCatalogue.Repository().Select(m => m.Name)),
                "spec/README.md's module index lists a module discovery does not find under spec/.");
        });
    }

    /// <summary>
    /// The manifest parser's rules: every key present, no unknown key, counts
    /// positive except <c>properties</c>, and both paths inside the module
    /// directory. Each refusal is a way a manifest could make a gate check
    /// something nobody wrote down.
    /// </summary>
    [Test]
    public void The_manifest_parser_refuses_what_it_cannot_enforce()
    {
        const string valid = """
            {
              "mutations": "mutations",
              "refinement": "Refinement.md",
              "nonBehaviouralActions": ["Stutter"],
              "counts": { "invariants": 1, "properties": 0, "actions": 2, "mutations": 1, "behaviourRows": 2, "distinctStates": 3 }
            }
            """;

        static SpecModuleManifest Parse(string json) => SpecModuleManifest.Parse("control.manifest.json", json);

        Assert.Multiple(() =>
        {
            var manifest = Parse(valid);
            Assert.That(manifest.Counts, Is.EqualTo(new SpecModuleCounts(1, 0, 2, 1, 2, 3)));
            Assert.That(manifest.NonBehaviouralActions, Is.EqualTo(new[] { "Stutter" }));

            Assert.That(() => Parse(valid.Replace("\"actions\": 2", "\"actions\": 0", StringComparison.Ordinal)),
                Throws.InvalidOperationException.With.Message.Contains("counts.actions"));
            Assert.That(() => Parse(valid.Replace("\"properties\": 0", "\"properties\": -1", StringComparison.Ordinal)),
                Throws.InvalidOperationException.With.Message.Contains("counts.properties"));
            Assert.That(() => Parse(valid.Replace(", \"distinctStates\": 3", string.Empty, StringComparison.Ordinal)),
                Throws.InvalidOperationException.With.Message.Contains("Missing: [distinctStates]"));
            Assert.That(() => Parse(valid.Replace("\"mutations\": \"mutations\",", "\"mutations\": \"mutations\", \"extra\": true,", StringComparison.Ordinal)),
                Throws.InvalidOperationException.With.Message.Contains("Unknown: [extra]"));
            Assert.That(() => Parse(valid.Replace("\"Refinement.md\"", "\"../Refinement.md\"", StringComparison.Ordinal)),
                Throws.InvalidOperationException.With.Message.Contains("inside the module directory"));
            Assert.That(() => Parse(valid.Replace("[\"Stutter\"]", "\"Stutter\"", StringComparison.Ordinal)),
                Throws.InvalidOperationException.With.Message.Contains("nonBehaviouralActions"));
            Assert.That(() => Parse("{"), Throws.InvalidOperationException.With.Message.Contains("not valid JSON"));
        });
    }

    /// <summary>
    /// The counts-table reader's rules: the header is fixed, numbers may carry
    /// thousands separators, and a module may not appear twice.
    /// </summary>
    [Test]
    public void The_counts_table_reader_reads_the_fixed_layout_and_nothing_else()
    {
        const string readme = """
            # Module

            ## Counts

            | Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
            |--------|------------|------------|---------|-----------|----------------|-----------------|
            | `Example` | 7 | 6 | 8 | 20 | 17 | 31,684 |

            ## Next section
            """;

        Assert.Multiple(() =>
        {
            Assert.That(
                SpecModuleReadme.ReadCounts(readme, "control")["Example"],
                Is.EqualTo(new SpecModuleCounts(7, 6, 8, 20, 17, 31684)));
            Assert.That(
                () => SpecModuleReadme.ReadCounts(readme.Replace("| Actions |", "| Steps |", StringComparison.Ordinal), "control"),
                Throws.InvalidOperationException.With.Message.Contains("columns"));
            Assert.That(
                () => SpecModuleReadme.ReadCounts(readme.Replace("| 31,684 |", "| many |", StringComparison.Ordinal), "control"),
                Throws.InvalidOperationException.With.Message.Contains("not a whole number"));
            Assert.That(
                () => SpecModuleReadme.ReadCounts(readme.Replace("## Counts", "## Totals", StringComparison.Ordinal), "control"),
                Throws.InvalidOperationException.With.Message.Contains("no '## Counts' section"));
            Assert.That(
                () => SpecModuleReadme.ReadCounts(
                    readme.Replace("| `Example` | 7 | 6 | 8 | 20 | 17 | 31,684 |", "| `Example` | 7 | 6 | 8 | 20 | 17 | 31,684 |\n| `Example` | 1 | 1 | 1 | 1 | 1 | 1 |", StringComparison.Ordinal),
                    "control"),
                Throws.InvalidOperationException.With.Message.Contains("twice"));
        });
    }
}
