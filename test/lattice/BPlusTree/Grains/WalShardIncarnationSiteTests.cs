using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The site pin for the WAL partition incarnation (issue #5896, condition I1):
/// every production path that removes entries from a WAL partition must either
/// advance the partition's incarnation after it, or be listed here with the
/// reason it is not a reset. A new trim path that does neither fails this test
/// instead of silently letting a replay page ring serve entries the log no
/// longer holds.
/// </summary>
[TestFixture]
public class WalShardIncarnationSiteTests
{
    /// <summary>
    /// Trim sites that are not resets, each with its reason. A reset is a change
    /// after which a reader may request offsets the replay ring already holds and
    /// be owed something other than what it holds.
    /// </summary>
    private static readonly Dictionary<string, string> NotResets = new(StringComparer.Ordinal)
    {
        ["src/lattice/LatticeWalGc.cs"] =
            "GC trims only below every registered consumer's cursor, so no reader asks for a trimmed offset again.",
        ["src/lattice/BPlusTree/Grains/LatticeAdminGrain.WalMove.cs"] =
            "A WAL move copies the same entries at the same offsets to another provider; the source trim follows the flip.",
        ["src/lattice.storage.file/FileWalStorageProvider.cs"] =
            "The provider's own TrimAsync implementation, not a caller.",
    };

    private static readonly Regex TrimCall = new(@"\.TrimAsync\(", RegexOptions.Compiled);

    [Test]
    public void Every_wal_reset_path_advances_the_partition_incarnation_before_new_appends()
    {
        var root = HygieneRepository.FindRepoRoot();
        var sites = Directory.EnumerateFiles(Path.Combine(root, "src"), "*.cs", SearchOption.AllDirectories)
            .Select(path => (Path: path, Relative: Path.GetRelativePath(root, path).Replace('\\', '/')))
            .Select(f => (f.Relative, Lines: File.ReadAllLines(f.Path)))
            .SelectMany(f => f.Lines
                .Select((line, index) => (f.Relative, f.Lines, Index: index, Line: line.Trim()))
                .Where(l => !l.Line.StartsWith("//", StringComparison.Ordinal) && TrimCall.IsMatch(l.Line)))
            .ToList();

        Assert.That(sites, Is.Not.Empty,
            "the scan matched no WAL trim call at all, so it cannot be checking anything");

        var resets = sites.Where(s => !NotResets.ContainsKey(s.Relative)).ToList();
        Assert.That(resets, Is.Not.Empty,
            "no reset path was found; the purge trim in TreeDeletionGrain must be one");

        Assert.Multiple(() =>
        {
            foreach (var site in resets)
            {
                var after = string.Join('\n', site.Lines.Skip(site.Index + 1).Take(40));
                Assert.That(after, Does.Contain("AdvanceIncarnationAsync"),
                    $"{site.Relative}:{site.Index + 1} removes WAL entries without advancing the partition's "
                    + "incarnation after it (issue #5896, I1); advance it, or list the site as not a reset with its reason");
            }

            foreach (var listed in NotResets.Keys)
            {
                Assert.That(sites.Any(s => s.Relative == listed), Is.True,
                    $"{listed} is listed as a non-reset trim site but no longer trims; remove it from the list");
            }
        });
    }
}
