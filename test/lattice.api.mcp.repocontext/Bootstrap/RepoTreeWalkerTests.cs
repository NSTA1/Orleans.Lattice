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
}
