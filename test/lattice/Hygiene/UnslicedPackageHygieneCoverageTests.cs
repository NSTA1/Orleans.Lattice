using System.IO;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Asserts the half of the hygiene partition that nothing else checks: that the
/// packages with <b>no</b> registered slice are actually reached by the core
/// project's repo-level scan.
/// <para>
/// Most packages under <c>src/</c> are absent from
/// <see cref="CoreHygieneScope.AllPackageSliceRoots"/>, and that is by design -
/// they are covered by the core fixture's repo-level remainder, which
/// enumerates the whole repository minus the registered slices. The design is
/// sound but the claim is load-bearing and, until this fixture, entirely
/// unasserted: <see cref="SliceCoverageCompletenessTests"/> checks that every
/// <i>registered</i> root has exactly one owner and says nothing at all about
/// the unregistered majority.
/// </para>
/// <para>
/// That leaves a silent failure mode. Narrow the repo-level enumeration so it
/// stops descending into <c>src/</c>, or add a package to the registry without
/// giving it a fixture, and coverage of those packages drops to zero while every
/// gate still reports a pass - the content gates end in
/// <c>Assert.That(violations, Is.Empty)</c>, and a scan that examined nothing
/// produces an empty violation list too. This fixture is the totality check
/// <see cref="HygieneDenominator"/> explicitly declines to be: it answers "did
/// the gate look at everything it claims to", where the denominator control
/// answers only "did it look at anything".
/// </para>
/// <para>
/// It asserts against the file set the real scanners consume
/// (<see cref="HygieneFiles.EnumerateTextFiles"/> over
/// <see cref="CoreHygieneScope.Value"/>) rather than re-deriving it, so a change
/// to the enumeration is caught rather than mirrored.
/// </para>
/// </summary>
[TestFixture]
public sealed class UnslicedPackageHygieneCoverageTests
{
    /// <summary>
    /// Every package under <c>src/</c> that no per-package fixture owns appears
    /// in the core repo-level scan, so no package is scanned by nobody.
    /// </summary>
    [Test]
    public void Every_unregistered_src_package_is_reached_by_the_core_repo_level_scan()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var srcPackages = Directory
            .EnumerateDirectories(Path.Combine(repoRoot, "src"))
            .Select(Path.GetFileName)
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(srcPackages, Is.Not.Empty,
            "Found no package directories under src/. The enumeration is broken, so every conclusion "
            + "below would be drawn from an empty set and this gate would pass having proved nothing.");

        var registered = CoreHygieneScope.AllPackageSliceRoots
            .Where(root => root.StartsWith("src/", StringComparison.Ordinal))
            .Select(root => root["src/".Length..])
            .ToHashSet(StringComparer.Ordinal);

        var unregistered = srcPackages
            .Where(package => !registered.Contains(package))
            .OrderBy(package => package, StringComparer.Ordinal)
            .ToList();

        Assert.That(unregistered, Is.Not.Empty,
            "Every package under src/ now has its own registered hygiene slice, so this fixture has no "
            + "population left to check and is asserting nothing. That is a legitimate end state, but it "
            + "must be a deliberate one: retire this fixture rather than leaving it silently vacuous.");

        var scanned = new HashSet<string>(StringComparer.Ordinal);
        var examined = 0;
        foreach (var file in HygieneFiles.EnumerateTextFiles(repoRoot, CoreHygieneScope.Value))
        {
            examined++;
            var relative = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
            if (!relative.StartsWith("src/", StringComparison.Ordinal)) continue;

            var rest = relative["src/".Length..];
            var slash = rest.IndexOf('/', StringComparison.Ordinal);
            if (slash > 0) scanned.Add(rest[..slash]);
        }

        HygieneDenominator.RequireExamined(
            examined,
            nameof(UnslicedPackageHygieneCoverageTests),
            "text files",
            HygieneDenominator.Describe(CoreHygieneScope.Value));

        var unreached = unregistered
            .Where(package => !scanned.Contains(package))
            .ToList();

        Assert.That(unreached, Is.Empty,
            "These packages under src/ are absent from CoreHygieneScope.AllPackageSliceRoots (so no "
            + "per-package hygiene fixture owns them) AND are not reached by the core project's "
            + "repo-level scan, so their text is checked by no content gate anywhere. Nothing will go "
            + "red when an em-dash or a mojibake run lands in one of them. Either restore the "
            + "repo-level enumeration so it covers them, or give each its own hygiene fixture and "
            + "register its slice roots:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", unreached));
    }

    /// <summary>
    /// The slice-root registry carries no duplicate entry. A duplicate is inert
    /// at scan time (the exclusion-prefix match and the per-package owner lookup
    /// are both set-like) but makes any count taken from the registry wrong, and
    /// signals that a hand-maintained list has been appended to twice.
    /// </summary>
    [Test]
    public void The_slice_root_registry_contains_no_duplicate_entries()
    {
        Assert.That(CoreHygieneScope.AllPackageSliceRoots, Is.Not.Empty,
            "The slice-root registry is empty, so the duplicate check below iterates nothing and "
            + "passes vacuously. An empty registry would also disable every per-package exclusion in "
            + "the core repo-level scan.");

        var duplicates = CoreHygieneScope.AllPackageSliceRoots
            .GroupBy(root => root, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => $"{group.Key} (x{group.Count()})")
            .OrderBy(entry => entry, StringComparer.Ordinal)
            .ToList();

        Assert.That(duplicates, Is.Empty,
            "CoreHygieneScope.AllPackageSliceRoots lists these roots more than once. De-duplicate it: "
            + "the list is hand-maintained, and a repeated entry makes any 'we cover N slices' count "
            + "taken from it wrong."
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", duplicates));
    }
}
