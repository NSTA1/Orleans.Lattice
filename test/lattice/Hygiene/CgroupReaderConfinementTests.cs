using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Confines every read of the cgroup filesystem to the one shared folder,
/// <c>src/lattice/Internal/Cgroups</c> (issue #2828).
/// </summary>
/// <remarks>
/// <para>
/// Before #2828 the repository held three independent cgroup readers. The
/// guard that kept two of them in step (a byte-identity drift test between the
/// core library's CPU reader and the embedding companion's mirror) was scoped to
/// that pair by construction, so the third - the memory reader inside
/// <c>LeafResidentWorkingSet</c> - sat outside every guard, and the three had
/// already diverged on how they spell unknown and whether they short-circuit off
/// Linux. #2817 removed the mirror; #2828 moved the memory reader into the
/// shared folder behind <see cref="Orleans.Lattice.Internal.Cgroups.CgroupFileSystem"/>.
/// </para>
/// <para>
/// This fixture is what stops a fourth reader. It is class-level rather than
/// pair-level: it does not know which readers exist, it scans every tracked C#
/// file outside <c>test/</c> for a cgroup filesystem path and requires each hit
/// to live in the shared folder. Comments and strings are scanned alike, because
/// the paths are string literals and because a doc comment naming a cgroup path
/// outside the folder is almost always describing a reader that should not be
/// there. Tests are exempt: they legitimately assert the canonical paths.
/// </para>
/// <para>
/// The signal is the path spelling, so a reader that assembles the path from
/// fragments (<c>Path.Combine("/sys", "fs", "cgroup")</c>) evades it. That is an
/// accepted limit of a lexical guard, recorded here so nobody mistakes the gate
/// for a proof.
/// </para>
/// </remarks>
[TestFixture]
public sealed class CgroupReaderConfinementTests
{
    private const string SharedFolder = "src/lattice/Internal/Cgroups/";

    private static readonly Regex CgroupPath = new(
        @"sys/fs/cgroup|/proc/self/cgroup",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Reports whether a file at <paramref name="repoRelativePath"/> holding
    /// <paramref name="content"/> reads the cgroup filesystem from outside the
    /// shared folder. Pure, so the classifier's own verdicts are testable without
    /// planting files in the working tree.
    /// </summary>
    internal static bool IsStrayReader(string repoRelativePath, string content)
    {
        var path = repoRelativePath.Replace('\\', '/');

        if (!path.EndsWith(".cs", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("test/", StringComparison.Ordinal)
            || path.StartsWith(SharedFolder, StringComparison.Ordinal))
        {
            return false;
        }

        return CgroupPath.IsMatch(content);
    }

    private static readonly Lazy<IReadOnlyList<(string RelativePath, string Content)>> Sources =
        new(LoadTrackedSources, LazyThreadSafetyMode.ExecutionAndPublication);

    private static IReadOnlyList<(string RelativePath, string Content)> TrackedSources() => Sources.Value;

    private static IReadOnlyList<(string RelativePath, string Content)> LoadTrackedSources()
    {
        var root = HygieneRepository.FindRepoRoot();

        return HygieneRepository.TrackedFiles(root)
            .Where(static f => f.EndsWith(".cs", StringComparison.OrdinalIgnoreCase))
            .Select(f => (Full: f, Relative: Path.GetRelativePath(root, f).Replace('\\', '/')))
            .Where(static f => !f.Relative.StartsWith("test/", StringComparison.Ordinal))
            .Select(static f => (f.Relative, File.ReadAllText(f.Full)))
            .ToList();
    }

    [Test]
    public void Every_cgroup_filesystem_read_lives_in_the_shared_folder()
    {
        var sources = TrackedSources();

        var strays = sources
            .Where(static s => IsStrayReader(s.RelativePath, s.Content))
            .Select(static s => s.RelativePath)
            .ToList();

        Assert.That(
            strays,
            Is.Empty,
            "A cgroup filesystem path appears outside " + SharedFolder + ". Read the cgroup "
            + "filesystem through CgroupFileSystem and put the parser beside ContainerCpuGrant "
            + "and ContainerMemoryLimit, so every reader shares one probe, one read policy, and "
            + "one spelling of unknown (issue #2828).");
    }

    [Test]
    public void The_scan_reaches_the_shared_folder_and_finds_its_readers()
    {
        // Anti-vacuity: a scan that saw no source, or that no longer sees the
        // readers it is guarding, would report a clean repository it never read.
        var sources = TrackedSources();

        var readersInFolder = sources
            .Where(static s => s.RelativePath.StartsWith(SharedFolder, StringComparison.Ordinal)
                && CgroupPath.IsMatch(s.Content))
            .Select(static s => Path.GetFileName(s.RelativePath))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(sources, Has.Count.GreaterThan(100), "the tracked-source scan returned implausibly few files");
            Assert.That(readersInFolder, Does.Contain("ContainerCpuGrant.cs"));
            Assert.That(readersInFolder, Does.Contain("ContainerMemoryLimit.cs"));
        });
    }

    [Test]
    public void Classifier_flags_a_reader_outside_the_folder_and_nothing_else()
    {
        const string reader = """private const string P = "/sys/fs/cgroup/memory.max";""";
        const string procReader = """var p = "/proc/self/cgroup";""";

        Assert.Multiple(() =>
        {
            Assert.That(IsStrayReader("src/lattice/BPlusTree/Grains/Stray.cs", reader), Is.True);
            Assert.That(IsStrayReader("apps/embedding-onnx/Embedding/Stray.cs", reader), Is.True);
            Assert.That(IsStrayReader(@"src\lattice\Stray.cs", procReader), Is.True, "Windows separators must not hide a hit");
            Assert.That(IsStrayReader(SharedFolder + "ContainerMemoryLimit.cs", reader), Is.False);
            Assert.That(IsStrayReader("test/lattice/Internal/Cgroups/Tests.cs", reader), Is.False);
            Assert.That(IsStrayReader("src/lattice/Stray.md", reader), Is.False);
            Assert.That(IsStrayReader("src/lattice/Clean.cs", "var x = 1;"), Is.False);
        });
    }
}
