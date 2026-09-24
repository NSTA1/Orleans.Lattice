using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Guards the seam behind issue #3088: every production acquisition of the tree
/// registry must go through
/// <see cref="Orleans.Lattice.BPlusTree.LatticeRegistryGrainFactoryExtensions.GetLatticeRegistry"/>,
/// which wraps the grain reference in the caller-side timing decorator. A raw
/// <c>GetGrain&lt;ILatticeRegistry&gt;</c> anywhere else under <c>src/</c> would
/// dispatch registry calls that
/// <see cref="LatticeMetrics.RegistryCallerDuration"/> never sees - silently, since
/// nothing else observes them - which is the same all-clear-by-absence that the
/// vanished Orleans <c>CurrentlyExecuting</c> log field produced.
/// </summary>
/// <remarks>
/// The seam replaces a silo-wide outgoing grain call filter, which was measured
/// at roughly 520 bytes per call on <i>every</i> grain call once registered. The
/// price of the cheaper seam is this guard: the filter could not be bypassed, the
/// seam can, so the bypass is made a build failure. Only <c>src/</c> is scanned;
/// tests may construct raw references to substitute them.
/// </remarks>
[TestFixture]
public sealed class RegistryAcquisitionSeamGuardTests
{
    private const string SeamFile = "src/lattice/BPlusTree/LatticeRegistryGrainFactoryExtensions.cs";

    private static readonly Regex RawAcquisition =
        new(@"GetGrain\s*<\s*(?:[\w.]+\.)?ILatticeRegistry\s*>", RegexOptions.Compiled);

    [Test]
    public void Every_registry_acquisition_in_src_goes_through_the_timing_seam()
    {
        var root = HygieneRepository.FindRepoRoot();
        var violations = new List<string>();
        var seamSites = 0;
        var filesScanned = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(Path.Combine(root, "src"), "*.cs"))
        {
            filesScanned++;
            var relative = Path.GetRelativePath(root, file).Replace('\\', '/');
            var isSeam = string.Equals(relative, SeamFile, StringComparison.Ordinal);
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (!IsRawAcquisition(lines[i]))
                {
                    continue;
                }

                if (isSeam)
                {
                    seamSites++;
                }
                else
                {
                    violations.Add($"{relative}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        Assert.That(filesScanned, Is.GreaterThan(0),
            "The scan found no .cs files under src/, so this guard is silently vacuous.");
        Assert.That(seamSites, Is.EqualTo(1),
            $"The seam {SeamFile} must contain exactly one raw registry acquisition. If it was moved "
            + "or renamed, update SeamFile; if the pattern no longer matches it, the guard is vacuous.");
        Assert.That(violations, Is.Empty,
            "A raw GetGrain<ILatticeRegistry> bypasses the caller-side registry timing (issue #3088), so "
            + "its calls are invisible on orleans.lattice.registry.caller.duration. Use "
            + "grainFactory.GetLatticeRegistry() instead.\n"
            + string.Join("\n", violations));
    }

    [TestCase("        var r = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);", true)]
    [TestCase("            .GetGrain<Orleans.Lattice.BPlusTree.ILatticeRegistry>(", true)]
    [TestCase("        var r = GrainFactory.GetGrain< ILatticeRegistry >(key);", true)]
    [TestCase("        var r = grainFactory.GetLatticeRegistry();", false)]
    [TestCase("        var l = grainFactory.GetGrain<ILattice>(treeId);", false)]
    [TestCase("    /// never call GetGrain<ILatticeRegistry> directly", false)]
    [TestCase("        // GetGrain<ILatticeRegistry> is banned here", false)]
    public void IsRawAcquisition_flags_code_and_ignores_comments(string line, bool expected)
    {
        Assert.That(IsRawAcquisition(line), Is.EqualTo(expected));
    }

    private static bool IsRawAcquisition(string line) =>
        !line.TrimStart().StartsWith("//", StringComparison.Ordinal) && RawAcquisition.IsMatch(line);
}
