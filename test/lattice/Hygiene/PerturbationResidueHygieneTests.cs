using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Perturbation-residue hygiene gate. Contract logic lives in the shared base.
/// <para>
/// <b>This one fixture scans the whole repository, unlike the sliced gates
/// beside it.</b> The em-dash and mojibake gates are sliced per package because
/// they scan for authoring mistakes, which belong to whoever owns the file, and
/// slicing keeps a single package's test run from walking the solution. Residue
/// is not an authoring mistake and has no owner: a perturbation arm run from any
/// session can leave it in any package, most often in one the author was not
/// otherwise touching. Slicing it would mean the gate only fires when the
/// interrupted run happened to perturb a file in the same package as the test
/// project being run - which is the shape of coverage that reads as complete and
/// is not.
/// </para>
/// <para>
/// The scope is therefore repo-level with nothing excluded: no slice roots, and
/// an empty other-slices registry so the repo-level walk covers <c>src/</c> and
/// <c>test/</c> as well as everything outside them. It is deliberately NOT
/// registered in <see cref="CoreHygieneScope.AllPackageSliceRoots"/>, because
/// that registry exists to stop the sliced gates double-scanning and this gate
/// is not sliced.
/// </para>
/// </summary>
[TestFixture]
public sealed class PerturbationResidueHygieneTests : PerturbationResidueHygieneTestsBase
{
    /// <inheritdoc />
    protected override HygieneScanScope Scope { get; } =
        new([], OwnsRepoLevelFiles: true, []);
}
