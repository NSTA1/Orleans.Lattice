using System.IO;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="RepoTreeWalker"/>: it walks a working tree into an ordered
/// set of <see cref="RepoFileEntry"/> with a digest per file, always skips the
/// <c>.git</c> metadata directory, applies include then exclude filters (exclude
/// wins), and normalises paths to a <c>'/'</c>-separated, repository-relative form.
/// </summary>
[TestFixture]
public sealed class RepoTreeWalkerTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "rcb-walker-" + Guid.NewGuid().ToString("N"));
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

    private void WriteBytes(string relativePath, byte[] content)
    {
        var full = Path.Combine(_root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllBytes(full, content);
    }

    [Test]
    public void Walk_returns_every_file_with_a_relative_posix_path_and_digest()
    {
        Write("a.cs", "one");
        Write("dir/b.cs", "two");

        var entries = RepoTreeWalker.Walk(_root, null, null);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "a.cs", "dir/b.cs" }));
            Assert.That(entries[0].Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("one"))));
            Assert.That(entries[1].Language, Is.EqualTo("csharp"));
            Assert.That(entries[0].SizeBytes, Is.EqualTo(3));
        });
    }

    [Test]
    public void Walk_orders_entries_by_ordinal_path()
    {
        Write("z.cs", "z");
        Write("a/b.cs", "b");
        Write("a.cs", "a");

        var entries = RepoTreeWalker.Walk(_root, null, null);

        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { "a.cs", "a/b.cs", "z.cs" }));
    }

    [Test]
    public void Walk_always_skips_the_git_directory()
    {
        Write("keep.cs", "keep");
        Write(".git/config", "gitconfig");
        Write(".git/objects/aa/bb", "obj");

        var entries = RepoTreeWalker.Walk(_root, null, null);

        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "keep.cs" }));
    }

    [Test]
    public void Walk_keeps_only_files_matching_an_include_glob()
    {
        Write("src/Program.cs", "code");
        Write("notes.txt", "text");

        var entries = RepoTreeWalker.Walk(_root, new[] { "**/*.cs" }, null);

        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "src/Program.cs" }));
    }

    [Test]
    public void Walk_applies_exclude_over_include()
    {
        Write("src/Program.cs", "code");
        Write("src/Generated.g.cs", "gen");

        var entries = RepoTreeWalker.Walk(_root, new[] { "**/*.cs" }, new[] { "**/*.g.cs" });

        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "src/Program.cs" }));
    }

    [Test]
    public void Walk_with_no_includes_treats_every_file_as_a_candidate()
    {
        Write("a.cs", "a");
        Write("b.txt", "b");

        var entries = RepoTreeWalker.Walk(_root, Array.Empty<string>(), null);

        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "a.cs", "b.txt" }));
    }

    [Test]
    public void Walk_produces_a_stable_digest_across_runs_for_unchanged_content()
    {
        Write("a.cs", "stable");

        var first = RepoTreeWalker.Walk(_root, null, null).Single().Digest;
        var second = RepoTreeWalker.Walk(_root, null, null).Single().Digest;

        Assert.That(first, Is.EqualTo(second));
    }

    [Test]
    public void Walk_rejects_a_null_root()
        => Assert.Throws<ArgumentNullException>(() => RepoTreeWalker.Walk(null!, null, null));

    [Test]
    public void Walk_throws_for_a_missing_root()
        => Assert.Throws<DirectoryNotFoundException>(
            () => RepoTreeWalker.Walk(Path.Combine(_root, "does-not-exist"), null, null));

    [Test]
    public void Walk_does_not_follow_a_symlinked_directory()
    {
        Write("real/a.cs", "a");

        var outside = Path.Combine(Path.GetTempPath(), "rcb-walker-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(outside);
        File.WriteAllText(Path.Combine(outside, "secret.cs"), "secret");
        try
        {
            var linkPath = Path.Combine(_root, "link");
            try
            {
                Directory.CreateSymbolicLink(linkPath, outside);
            }
            catch (Exception ex) when (ex is UnauthorizedAccessException or IOException or PlatformNotSupportedException)
            {
                Assert.Ignore("Creating a symbolic link is not permitted in this environment.");
                return;
            }

            var entries = RepoTreeWalker.Walk(_root, null, null);

            // The real file is walked; nothing behind the symlinked directory is.
            Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "real/a.cs" }));
        }
        finally
        {
            Directory.Delete(outside, recursive: true);
        }
    }

    [Test]
    public void Walk_does_not_read_a_symlinked_file()
    {
        Write("real.cs", "real");

        var outside = Path.Combine(Path.GetTempPath(), "rcb-walker-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(outside);
        var target = Path.Combine(outside, "target.cs");
        File.WriteAllText(target, "secret");
        try
        {
            var linkPath = Path.Combine(_root, "link.cs");
            try
            {
                File.CreateSymbolicLink(linkPath, target);
            }
            catch (Exception ex) when (ex is UnauthorizedAccessException or IOException or PlatformNotSupportedException)
            {
                Assert.Ignore("Creating a symbolic link is not permitted in this environment.");
                return;
            }

            var entries = RepoTreeWalker.Walk(_root, null, null);

            Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "real.cs" }));
        }
        finally
        {
            Directory.Delete(outside, recursive: true);
        }
    }

    [Test]
    public void Walk_ignores_gitignore_rules_when_the_flag_is_off()
    {
        Write(".gitignore", "*.log\n");
        Write("keep.cs", "keep");
        Write("noise.log", "noise");

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: false);

        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { ".gitignore", "keep.cs", "noise.log" }));
    }

    [Test]
    public void Walk_honours_a_root_gitignore_file_pattern()
    {
        Write(".gitignore", "*.log\n");
        Write("keep.cs", "keep");
        Write("noise.log", "noise");
        Write("nested/deep.log", "deep");

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: true);

        // The .gitignore file itself is content and stays walked; a bare pattern
        // matches at any depth.
        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { ".gitignore", "keep.cs" }));
    }

    [Test]
    public void Walk_prunes_an_ignored_directory_and_its_whole_subtree()
    {
        Write(".gitignore", "bin/\n");
        Write("src/Program.cs", "code");
        Write("bin/Debug/app.dll", "dll");
        Write("bin/obj/tmp", "tmp");

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: true);

        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { ".gitignore", "src/Program.cs" }));
    }

    [Test]
    public void Walk_anchors_a_slash_pattern_to_the_gitignore_directory()
    {
        Write(".gitignore", "/build\n");
        Write("build/out.o", "o");
        Write("src/build/keep.cs", "keep");

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: true);

        // The anchored '/build' matches only the root-level build directory, not a
        // nested one.
        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { ".gitignore", "src/build/keep.cs" }));
    }

    [Test]
    public void Walk_layers_a_nested_gitignore_over_a_parent()
    {
        Write(".gitignore", "*.log\n");
        Write("keep.log", "root-log");
        Write("sub/.gitignore", "!keep.log\n");
        Write("sub/keep.log", "sub-log");
        Write("sub/other.log", "other");

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: true);

        // The parent ignores every *.log; the nested .gitignore re-includes
        // keep.log for its own subtree only.
        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { ".gitignore", "sub/.gitignore", "sub/keep.log" }));
    }

    [Test]
    public void Walk_skips_comment_and_blank_lines_in_a_gitignore()
    {
        Write(".gitignore", "# a comment\n\n   \n*.tmp\n");
        Write("keep.cs", "keep");
        Write("scratch.tmp", "tmp");

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: true);

        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { ".gitignore", "keep.cs" }));
    }

    [Test]
    public void Walk_layers_gitignore_under_include_and_exclude_globs()
    {
        Write(".gitignore", "*.log\n");
        Write("src/Program.cs", "code");
        Write("src/Generated.g.cs", "gen");
        Write("src/trace.log", "log");

        var entries = RepoTreeWalker.Walk(
            _root, new[] { "**/*.cs" }, new[] { "**/*.g.cs" }, respectGitignore: true);

        // gitignore drops the .log; the include keeps only .cs; the exclude drops
        // the generated file. (The .gitignore file is not a .cs, so the include
        // filter naturally excludes it here.)
        Assert.That(
            entries.Select(e => e.RelativePath),
            Is.EqualTo(new[] { "src/Program.cs" }));
    }

    [Test]
    public void Walk_excludes_a_binary_file_when_the_flag_is_on()
    {
        Write("code.cs", "text");
        WriteBytes("blob.bin", new byte[] { 0x01, 0x02, 0x00, 0x03 });

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: false, excludeBinary: true);

        // The NUL byte in blob.bin classifies it as binary; only the text file survives.
        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "code.cs" }));
    }

    [Test]
    public void Walk_keeps_a_binary_file_when_the_flag_is_off()
    {
        Write("code.cs", "text");
        WriteBytes("blob.bin", new byte[] { 0x01, 0x02, 0x00, 0x03 });

        var entries = RepoTreeWalker.Walk(_root, null, null, respectGitignore: false, excludeBinary: false);

        Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "blob.bin", "code.cs" }));
    }

    [Test]
    public void Walk_reports_progress_and_settles_on_the_exact_included_count()
    {
        Write("a.cs", "one");
        Write("dir/b.cs", "two");
        Write("dir/c.cs", "three");

        var reports = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var entries = RepoTreeWalker.Walk(
            _root, null, null, respectGitignore: false, excludeBinary: false,
            onProgress: reports.Enqueue);

        Assert.Multiple(() =>
        {
            // Progress was reported at least once, and the final reported value is
            // the authoritative included count (also what the walk returned).
            Assert.That(reports, Is.Not.Empty);
            Assert.That(reports.Last(), Is.EqualTo(entries.Count));
            Assert.That(entries, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void Walk_reports_the_final_count_excluding_dropped_binaries()
    {
        Write("code.cs", "text");
        WriteBytes("blob.bin", new byte[] { 0x00, 0x01 });

        var reports = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var entries = RepoTreeWalker.Walk(
            _root, null, null, respectGitignore: false, excludeBinary: true,
            onProgress: reports.Enqueue);

        // The binary file is dropped, so the settled progress count is the one text
        // file, never counting the excluded blob.
        Assert.Multiple(() =>
        {
            Assert.That(reports.Last(), Is.EqualTo(1));
            Assert.That(entries.Select(e => e.RelativePath), Is.EqualTo(new[] { "code.cs" }));
        });
    }

    [Test]
    public void Walk_reuses_the_stored_digest_without_reading_when_the_stat_fast_path_hits()
    {
        // A file whose size matches and whose modification time is strictly older
        // than the stored ingest anchor is assumed unchanged. Prove the read is
        // skipped by seeding a stored digest that does NOT match the on-disk bytes:
        // if the walk reused it verbatim, it never read the file.
        Write("a.cs", "12345");
        var path = Path.Combine(_root, "a.cs");
        File.SetLastWriteTimeUtc(path, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var anchor = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        var known = new Dictionary<string, StoredFileMeta>
        {
            ["a.cs"] = new StoredFileMeta("xx128:deadbeefdeadbeefdeadbeefdeadbeef", "csharp", 5, anchor),
        };

        var entry = RepoTreeWalker.Walk(
            _root, null, null, knownFiles: known).Single();

        Assert.Multiple(() =>
        {
            Assert.That(entry.Digest, Is.EqualTo("xx128:deadbeefdeadbeefdeadbeefdeadbeef"));
            Assert.That(entry.Language, Is.EqualTo("csharp"));
            Assert.That(entry.AnchorStale, Is.False);
        });
    }

    [Test]
    public void Walk_reads_and_recomputes_when_the_on_disk_size_differs_from_the_stored_size()
    {
        Write("a.cs", "grown content");
        var path = Path.Combine(_root, "a.cs");
        File.SetLastWriteTimeUtc(path, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var anchor = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        // Stored size is stale, so the fast-path cannot apply and the file is read.
        var known = new Dictionary<string, StoredFileMeta>
        {
            ["a.cs"] = new StoredFileMeta("xx128:deadbeefdeadbeefdeadbeefdeadbeef", "csharp", 3, anchor),
        };

        var entry = RepoTreeWalker.Walk(
            _root, null, null, knownFiles: known).Single();

        Assert.That(
            entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("grown content"))));
    }

    [Test]
    public void Walk_reads_and_recomputes_when_the_modification_time_is_at_or_after_the_anchor()
    {
        Write("a.cs", "edited");
        var path = Path.Combine(_root, "a.cs");
        var anchor = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(path, anchor);

        // Modification time equals the anchor (the racy-clean window): the strict
        // comparison forces a read even though the size matches.
        var known = new Dictionary<string, StoredFileMeta>
        {
            ["a.cs"] = new StoredFileMeta("xx128:deadbeefdeadbeefdeadbeefdeadbeef", "csharp", 6, anchor.Ticks),
        };

        var entry = RepoTreeWalker.Walk(
            _root, null, null, knownFiles: known).Single();

        Assert.That(
            entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("edited"))));
    }

    [Test]
    public void Walk_keeps_the_stored_digest_and_flags_the_anchor_stale_when_a_touched_file_is_unchanged()
    {
        // The file was touched (modification time bumped past the anchor) but the
        // bytes are identical. The fast-path misses, the read confirms the content
        // matches the stored digest, so the digest is kept verbatim and the entry is
        // flagged so the reconcile rewrites the node to refresh the anchor.
        Write("a.cs", "same");
        var content = Encoding.UTF8.GetBytes("same");
        var storedDigest = FileDigest.Compute(content);
        var path = Path.Combine(_root, "a.cs");
        var anchor = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(path, anchor.AddDays(1));

        var known = new Dictionary<string, StoredFileMeta>
        {
            ["a.cs"] = new StoredFileMeta(storedDigest, "csharp", content.Length, anchor.Ticks),
        };

        var entry = RepoTreeWalker.Walk(
            _root, null, null, knownFiles: known).Single();

        Assert.Multiple(() =>
        {
            Assert.That(entry.Digest, Is.EqualTo(storedDigest));
            Assert.That(entry.AnchorStale, Is.True);
        });
    }

    [Test]
    public void Walk_never_skips_a_file_whose_stored_anchor_is_unknown()
    {
        Write("a.cs", "abc");
        var path = Path.Combine(_root, "a.cs");
        File.SetLastWriteTimeUtc(path, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // A zero anchor (a record ingested before the anchor was recoverable) must
        // fall through to a real read rather than trusting the stale digest.
        var known = new Dictionary<string, StoredFileMeta>
        {
            ["a.cs"] = new StoredFileMeta("xx128:deadbeefdeadbeefdeadbeefdeadbeef", "csharp", 3, 0),
        };

        var entry = RepoTreeWalker.Walk(
            _root, null, null, knownFiles: known).Single();

        Assert.That(
            entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("abc"))));
    }

    [Test]
    public void Walk_migrates_a_legacy_sha256_digest_only_when_the_content_changes()
    {
        // A legacy SHA-256 store that is genuinely edited (fast-path miss, content
        // differs) is rewritten with the modern tagged digest, so the algorithm
        // migrates lazily as content evolves rather than in a forced bulk re-hash.
        Write("a.cs", "new bytes");
        var path = Path.Combine(_root, "a.cs");
        var anchor = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(path, anchor.AddDays(1));

        var legacyDigest = System.Convert.ToHexStringLower(
            System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes("old bytes")));
        var known = new Dictionary<string, StoredFileMeta>
        {
            ["a.cs"] = new StoredFileMeta(legacyDigest, "csharp", 9, anchor.Ticks),
        };

        var entry = RepoTreeWalker.Walk(
            _root, null, null, knownFiles: known).Single();

        Assert.Multiple(() =>
        {
            Assert.That(entry.Digest, Does.StartWith("xx128:"));
            Assert.That(
                entry.Digest, Is.EqualTo(FileDigest.Compute(Encoding.UTF8.GetBytes("new bytes"))));
            Assert.That(entry.AnchorStale, Is.False);
        });
    }
}
