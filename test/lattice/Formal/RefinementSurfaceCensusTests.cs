using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Every production grain is named by a refinement note or listed, with a
/// reason, in the unmodelled-grain register <c>spec/UNMODELLED.md</c>; see
/// <see cref="RefinementSurfaceCensus"/>. The negative controls that prove each
/// failure is reachable live in <c>RefinementSurfaceCensusTests.Controls.cs</c>.
/// </summary>
[TestFixture]
public sealed partial class RefinementSurfaceCensusTests
{
    /// <summary>
    /// The fewest grains the census may find under <c>src/</c>. The tree holds
    /// well over a hundred; a population this small means the scan broke, and a
    /// gate over an empty population passes while checking nothing.
    /// </summary>
    private const int MinimumRepositoryGrains = 50;

    private static readonly Lazy<IReadOnlyList<RefinementSurfaceCensusGrain>> RepositoryGrains =
        new(() => RefinementSurfaceCensus.FindGrains(RefinementSurfaceCensus.RepositorySourceRoot));

    private static readonly Lazy<IReadOnlyList<(string Label, string Text)>> RepositoryNotes = new(ReadRepositoryNotes);

    /// <summary>
    /// The gate. A failure names each grain no note names and no row lists,
    /// and each register row that is stale.
    /// </summary>
    [Test]
    public void Every_production_grain_is_named_by_a_refinement_note_or_listed_in_the_register()
    {
        var grains = RepositoryGrains.Value;
        HygieneDenominator.RequireExamined(
            grains.Count, nameof(RefinementSurfaceCensusTests), "grain classes", "src/");
        Assert.That(grains, Has.Count.GreaterThanOrEqualTo(MinimumRepositoryGrains),
            "the census found implausibly few grains under src/, so the syntactic scan is broken");
        Assert.That(RepositoryNotes.Value, Is.Not.Empty, "no refinement note was read, so no grain could count as named");

        var problems = RefinementSurfaceCensus.Problems(
            grains, RepositoryNotes.Value, RepositoryRegister(), RefinementSurfaceCensus.RegisterRelativePath);

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The scan finds the grains the formal suite is built around, through each
    /// of the ways a class becomes a grain: deriving from <c>Grain</c>,
    /// implementing <c>IGrainBase</c>, and an abstract base grain.
    /// </summary>
    [Test]
    public void The_census_finds_known_grains_through_every_base_form()
    {
        var grains = RepositoryGrains.Value.Select(g => (g.Name, g.Package)).ToHashSet();

        Assert.Multiple(() =>
        {
            Assert.That(grains, Does.Contain(("BPlusLeafGrain", "lattice")), "a Grain<T> subclass with a primary constructor");
            Assert.That(grains, Does.Contain(("ShardRootGrain", "lattice")), "a partial grain declared across many files");
            Assert.That(grains, Does.Contain(("WalMaterialiserPinGrain", "lattice")), "an IGrainBase implementation");
            Assert.That(grains, Does.Contain(("IndexedGrain", "lattice.grainindex")), "an abstract base grain");
            Assert.That(RepositoryGrains.Value.Where(g => g.Name == "IndexedGrain").Select(g => g.IsAbstract), Is.All.True);
        });
    }

    /// <summary>
    /// The register is a second top-level markdown file under <c>spec/</c>,
    /// beside <c>spec/README.md</c>, and module discovery still succeeds with it
    /// there: discovery treats only directories (and a stray <c>.tla</c>) at the
    /// spec root as modules.
    /// </summary>
    [Test]
    public void The_register_sits_at_the_spec_root_and_module_discovery_accepts_it()
    {
        Assert.That(File.Exists(RefinementSurfaceCensus.RepositoryRegisterPath), Is.True,
            $"{RefinementSurfaceCensus.RegisterRelativePath} is missing");
        Assert.That(() => SpecModuleCatalogue.Repository(), Throws.Nothing);
    }

    /// <summary>
    /// Live-data controls: the gate over the real population and notes goes red
    /// when the real register loses a row or gains one for no grain, so a green
    /// gate is not a parse that matched nothing.
    /// </summary>
    [Test]
    public void The_repository_gate_fails_when_the_real_register_loses_or_gains_a_row()
    {
        var register = RepositoryRegister();
        Assert.That(register, Is.Not.Empty, "the register has no rows, so the control cannot remove one");

        var missing = RefinementSurfaceCensus.Problems(
            RepositoryGrains.Value, RepositoryNotes.Value, register.Skip(1).ToList(), RefinementSurfaceCensus.RegisterRelativePath);
        var phantom = RefinementSurfaceCensus.Problems(
            RepositoryGrains.Value,
            RepositoryNotes.Value,
            [.. register, new RefinementSurfaceCensusEntry("NoSuchCensusGrain", "zzz", "A control row.", 9999)],
            RefinementSurfaceCensus.RegisterRelativePath);

        Assert.Multiple(() =>
        {
            Assert.That(missing, Has.Some.Contains($"`{register[0].Grain}`").And.Contains("does not list"));
            Assert.That(phantom, Has.Some.Contains("`NoSuchCensusGrain`").And.Contains("is not a grain declared"));
        });
    }

    private static IReadOnlyList<RefinementSurfaceCensusEntry> RepositoryRegister() =>
        RefinementSurfaceCensus.ReadRegister(
            File.ReadAllText(RefinementSurfaceCensus.RepositoryRegisterPath), RefinementSurfaceCensus.RegisterRelativePath);

    private static IReadOnlyList<(string Label, string Text)> ReadRepositoryNotes()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        return SpecModuleCatalogue.Repository()
            .Select(module => module.RefinementNotePath)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .Select(path => (Path.GetRelativePath(repoRoot, path).Replace('\\', '/'), File.ReadAllText(path)))
            .ToList();
    }
}
