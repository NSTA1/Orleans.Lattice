using System.IO;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="RepoTreeWalker"/>'s directory-modification-time pruning
/// (<see cref="RepoWalkPruning"/>): a directory whose modification time is unchanged
/// since the previous walk carries its known files forward without a stat, while every
/// subdirectory is still descended so a nested change is never missed, and a forced full
/// sweep ignores the snapshot entirely.
/// </summary>
[TestFixture]
public sealed class RepoTreeWalkerPruningTests
{
    /// <summary>A stored digest that never matches on-disk bytes, so reusing it proves no read happened.</summary>
    private const string Sentinel = "xx128:00000000000000000000000000000000";

    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "rcb-prune-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private void Write(string relativePath, string content)
    {
        var full = Path.Combine(_root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllBytes(full, Encoding.UTF8.GetBytes(content));
    }

    /// <summary>
    /// A fixed, stable modification time stamped onto every directory before priming
    /// so the priming capture and the later pruned walk observe identical directory
    /// modification times. Without this an NTFS directory's lazily-flushed
    /// modification time can differ by a tick between the two walks under parallel
    /// load, defeating pruning non-deterministically.
    /// </summary>
    private static readonly DateTime PinnedDirTime = new(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>Pins <paramref name="root"/> and every subdirectory to <see cref="PinnedDirTime"/>.</summary>
    private static void PinDirectoryMtimes(string root)
    {
        Directory.SetLastWriteTimeUtc(root, PinnedDirTime);
        foreach (var dir in Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories))
        {
            Directory.SetLastWriteTimeUtc(dir, PinnedDirTime);
        }
    }

    /// <summary>
    /// Runs a cold full walk to capture a directory snapshot, then builds a
    /// <c>knownFiles</c> map that seeds a sentinel digest for every discovered file with a
    /// far-future ingest anchor, so a later pruned walk that carries a file forward returns
    /// the sentinel (proving it never read the file).
    /// </summary>
    private (IReadOnlyDictionary<string, long> PreviousMtimes, Dictionary<string, StoredFileMeta> Known) Prime(
        DateTime? anchor = null)
    {
        PinDirectoryMtimes(_root);
        var priming = new RepoWalkPruning { ForceFull = true };
        var cold = RepoTreeWalker.Walk(_root, null, null, pruning: priming);
        var anchorTicks = (anchor ?? DateTime.UtcNow.AddYears(1)).Ticks;
        var known = cold.ToDictionary(
            e => e.RelativePath,
            e => new StoredFileMeta(Sentinel, e.Language, e.SizeBytes, anchorTicks, []),
            StringComparer.Ordinal);
        return (priming.CurrentDirectoryMtimes, known);
    }

    [Test]
    public void Walk_with_a_matching_snapshot_carries_files_forward_without_reading()
    {
        Write("root.cs", "r");
        Write("a/x.cs", "x");
        Write("a/b/y.cs", "y");
        var (previous, known) = Prime();

        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = previous };
        var entries = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning)
            .ToDictionary(e => e.RelativePath, e => e.Digest, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            // Every file was carried forward from the store, so each shows the sentinel
            // digest that does not match its real bytes: none were read.
            Assert.That(entries["root.cs"], Is.EqualTo(Sentinel));
            Assert.That(entries["a/x.cs"], Is.EqualTo(Sentinel));
            Assert.That(entries["a/b/y.cs"], Is.EqualTo(Sentinel));
            Assert.That(pruning.PrunedFileCount, Is.EqualTo(3));
            // The root, a, and a/b were all unchanged and pruned.
            Assert.That(pruning.PrunedDirectoryCount, Is.EqualTo(3));
        });
    }

    [Test]
    public void Walk_with_ForceFull_ignores_the_snapshot_and_reads_every_file()
    {
        Write("a/x.cs", "x");
        // A past ingest anchor defeats the stat fast-path, so a genuine read is forced
        // and the recomputed digest replaces the sentinel - proving the full walk stat'd
        // and read the file rather than pruning it.
        var (previous, known) = Prime(anchor: DateTime.UtcNow.AddYears(-1));

        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = previous, ForceFull = true };
        var entry = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning)
            .Single();

        Assert.Multiple(() =>
        {
            // A forced full sweep re-reads the file, so the real digest replaces the
            // sentinel and nothing is pruned.
            Assert.That(entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("x"))));
            Assert.That(pruning.PrunedFileCount, Is.Zero);
            Assert.That(pruning.PrunedDirectoryCount, Is.Zero);
        });
    }

    [Test]
    public void Walk_with_an_empty_snapshot_reads_every_file()
    {
        Write("a/x.cs", "x");
        // A past ingest anchor defeats the stat fast-path, so the file is genuinely read.
        var anchor = DateTime.UtcNow.AddYears(-1).Ticks;
        var known = new Dictionary<string, StoredFileMeta>(StringComparer.Ordinal)
        {
            ["a/x.cs"] = new StoredFileMeta(Sentinel, "csharp", 1, anchor, []),
        };

        // A pruning context with no prior snapshot is the cold case: pruning cannot apply.
        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = null };
        var entry = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning)
            .Single();

        Assert.Multiple(() =>
        {
            Assert.That(entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("x"))));
            Assert.That(pruning.PrunedFileCount, Is.Zero);
            // The snapshot is still recorded for the next walk, root plus a.
            Assert.That(pruning.CurrentDirectoryMtimes.Keys, Does.Contain(string.Empty));
            Assert.That(pruning.CurrentDirectoryMtimes.Keys, Does.Contain("a"));
        });
    }

    [Test]
    public void Walk_re_walks_only_the_directory_whose_modification_time_changed()
    {
        Write("a/x.cs", "x");
        Write("c/z.cs", "z");
        var (previous, known) = Prime();

        // Add a file to c/, which bumps only c's modification time - a's is unchanged.
        Write("c/new.cs", "new");
        // Pin c's modification time to a distinct later value so the prune decision is
        // deterministic regardless of the test filesystem's timestamp resolution.
        Directory.SetLastWriteTimeUtc(Path.Combine(_root, "c"), DateTime.UtcNow.AddMinutes(1));

        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = previous };
        var entries = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning)
            .ToDictionary(e => e.RelativePath, e => e.Digest, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            // a was pruned: its file is carried forward with the sentinel.
            Assert.That(entries["a/x.cs"], Is.EqualTo(Sentinel));
            // c was re-walked because its modification time changed: the new file is found.
            Assert.That(entries.ContainsKey("c/new.cs"), Is.True);
            Assert.That(entries["c/new.cs"], Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("new"))));
        });
    }

    [Test]
    public void Walk_detects_a_change_nested_under_an_unchanged_ancestor()
    {
        Write("a/x.cs", "x");
        Write("a/b/y.cs", "y");
        var (previous, known) = Prime();

        // Add a file to the nested a/b, which bumps a/b's modification time but NOT a's:
        // a's own direct entries are unchanged. The walk must still descend a/b through a.
        Write("a/b/deep.cs", "deep");
        // Pin a/b's modification time to a distinct later value so the prune decision is
        // deterministic regardless of the test filesystem's timestamp resolution.
        Directory.SetLastWriteTimeUtc(Path.Combine(_root, "a", "b"), DateTime.UtcNow.AddMinutes(1));

        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = previous };
        var entries = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning)
            .ToDictionary(e => e.RelativePath, e => e.Digest, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            // a was pruned (x.cs carried forward) yet its subdirectory a/b was still
            // descended, so the nested addition is detected.
            Assert.That(entries["a/x.cs"], Is.EqualTo(Sentinel));
            Assert.That(entries.ContainsKey("a/b/deep.cs"), Is.True);
            Assert.That(entries["a/b/deep.cs"], Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("deep"))));
        });
    }

    [Test]
    public void Walk_records_a_modification_time_for_every_directory_it_visits()
    {
        Write("a/x.cs", "x");
        Write("a/b/y.cs", "y");
        var (previous, known) = Prime();

        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = previous };
        RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning);

        // Every directory - root, a, a/b - is recorded whether pruned or walked, so the
        // snapshot handed to the next walk stays complete.
        Assert.That(
            pruning.CurrentDirectoryMtimes.Keys,
            Is.EquivalentTo(new[] { string.Empty, "a", "a/b" }));
    }

    [Test]
    public void Walk_carrying_forward_a_file_with_no_ingest_anchor_falls_back_to_reading_it()
    {
        Write("a/x.cs", "x");
        var priming = new RepoWalkPruning { ForceFull = true };
        RepoTreeWalker.Walk(_root, null, null, pruning: priming);

        // A carried-forward file whose stored ingest anchor is zero cannot use the stat
        // fast-path, so the walk safely reads the real file rather than trusting a stale
        // sentinel.
        var known = new Dictionary<string, StoredFileMeta>(StringComparer.Ordinal)
        {
            ["a/x.cs"] = new StoredFileMeta(Sentinel, "csharp", 1, 0, []),
        };
        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = priming.CurrentDirectoryMtimes };
        var entry = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning)
            .Single();

        Assert.That(entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("x"))));
    }

    /// <summary>
    /// Pins the per-directory grouping at wide fan-out: many sibling directories, each
    /// holding many files, must every one be carried forward exactly once and in source
    /// order. The grouping builds exact-width buckets in a counting pass and fills them in
    /// a second pass, so an off-by-one in either pass would drop, duplicate, or reorder a
    /// directory's files without any narrow fixture noticing.
    /// </summary>
    [Test]
    public void Walk_carries_forward_every_file_across_a_wide_fan_out_of_directories()
    {
        var expected = new List<string>();
        for (var d = 0; d < 12; d++)
        {
            for (var f = 0; f < 7; f++)
            {
                var relative = $"pkg{d:D2}/area/File{f:D2}.cs";
                Write(relative, $"{d}-{f}");
                expected.Add(relative);
            }
        }

        var (previous, known) = Prime();

        var pruning = new RepoWalkPruning { PreviousDirectoryMtimes = previous };
        var entries = RepoTreeWalker.Walk(_root, null, null, knownFiles: known, pruning: pruning);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.RelativePath), Is.EquivalentTo(expected));
            Assert.That(entries.Select(e => e.Digest), Is.All.EqualTo(Sentinel));
        });
    }
}
