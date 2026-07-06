using System.IO;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Guards the repository ownership registry (<see cref="CoreHygieneScope.AllPackageSliceRoots"/>)
/// against accidental incomplete hygiene coverage. The content-hygiene gates
/// (em-dash, mojibake, tracker-id, deletion-mandate, ...) partition the
/// repository so every file is scanned exactly once: the core fixture scans
/// every repo-level file <em>except</em> the slices listed in the registry, and
/// each per-package fixture scans its own <see cref="HygieneScanScope.ForSlice(string[])"/>
/// roots.
/// <para>
/// The registry is hand-maintained, so it can silently drift from the set of
/// slices per-package fixtures actually declare. The two drift directions have
/// very different consequences:
/// </para>
/// <list type="bullet">
///   <item>A root registered but owned by <b>no</b> fixture is excluded from the
///   core repo-level scan yet scanned by nobody - a silent coverage <b>gap</b>
///   through which a violation can reach a pull request.</item>
///   <item>A root a fixture scans but that is <b>missing</b> from the registry is
///   scanned twice (once by its fixture, once by the core repo-level scan) - a
///   harmless but drift-indicating double scan.</item>
/// </list>
/// This fixture discovers the slices per-package fixtures declare by scanning
/// the test sources for <c>HygieneScanScope.ForSlice(...)</c> calls, then asserts
/// exact set-equality with the registry (plus the core project's own slice),
/// catching both directions the moment a package is added, moved, or removed.
/// </summary>
[TestFixture]
public sealed class SliceCoverageCompletenessTests
{
    private static readonly Regex ForSliceRegex =
        new(@"HygieneScanScope\.ForSlice\(([^)]*)\)", RegexOptions.Compiled);

    private static readonly Regex StringLiteralRegex =
        new("\"([^\"]+)\"", RegexOptions.Compiled);

    /// <summary>
    /// Every registered slice root is owned by exactly one scope - a per-package
    /// fixture's <c>ForSlice(...)</c> declaration or the core project's own slice -
    /// and every slice a fixture declares is registered, so the core repo-level
    /// scan skips it.
    /// </summary>
    [Test]
    public void Registry_and_per_package_fixture_slices_agree_exactly()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var owned = new HashSet<string>(DiscoverDeclaredSliceRoots(repoRoot), StringComparer.Ordinal);
        foreach (var coreRoot in CoreHygieneScope.CoreSliceRoots)
        {
            owned.Add(coreRoot);
        }

        var registered = new HashSet<string>(CoreHygieneScope.AllPackageSliceRoots, StringComparer.Ordinal);

        var unowned = registered.Where(r => !owned.Contains(r))
            .OrderBy(r => r, StringComparer.Ordinal).ToList();
        Assert.That(unowned, Is.Empty,
            "These slice roots are registered in CoreHygieneScope.AllPackageSliceRoots (so the core repo-level "
            + "hygiene scan skips them) but no per-package hygiene fixture declares them via "
            + "HygieneScanScope.ForSlice(...), so they are scanned by nobody. Add a per-package hygiene fixture "
            + "for each, or remove it from the registry:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", unowned));

        var unregistered = owned.Where(r => !registered.Contains(r))
            .OrderBy(r => r, StringComparer.Ordinal).ToList();
        Assert.That(unregistered, Is.Empty,
            "These slice roots are scanned by a per-package hygiene fixture but are absent from "
            + "CoreHygieneScope.AllPackageSliceRoots, so the core repo-level scan also scans them (a double scan). "
            + "Add each to the registry so each slice is scanned exactly once:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", unregistered));
    }

    /// <summary>
    /// Every registered slice root resolves to a real directory, catching a
    /// stale entry left behind when a package is renamed or removed.
    /// </summary>
    [Test]
    public void Every_registered_slice_root_exists_on_disk()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var missing = CoreHygieneScope.AllPackageSliceRoots
            .Where(r => !Directory.Exists(Path.Combine(repoRoot, r.Replace('/', Path.DirectorySeparatorChar))))
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToList();

        Assert.That(missing, Is.Empty,
            "These slice roots in CoreHygieneScope.AllPackageSliceRoots do not exist on disk (renamed or removed?):"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", missing));
    }

    private static IReadOnlySet<string> DiscoverDeclaredSliceRoots(string repoRoot)
    {
        var declared = new HashSet<string>(StringComparer.Ordinal);
        var testRoot = Path.Combine(repoRoot, "test");

        foreach (var file in HygieneRepository.EnumerateFiles(testRoot, "*.cs"))
        {
            var text = File.ReadAllText(file);
            foreach (Match call in ForSliceRegex.Matches(text))
            {
                foreach (Match literal in StringLiteralRegex.Matches(call.Groups[1].Value))
                {
                    declared.Add(literal.Groups[1].Value);
                }
            }
        }

        return declared;
    }
}
