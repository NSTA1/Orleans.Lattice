using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="DataPathGuard"/>: it creates a missing directory,
/// accepts a writable one, and rejects an empty path.
/// </summary>
[TestFixture]
public sealed class DataPathGuardTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
        => _root = Path.Combine(Path.GetTempPath(), "repocontext-guard-" + Guid.NewGuid().ToString("N"));

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public void EnsureDirectoryWritable_creates_a_missing_directory()
    {
        var target = Path.Combine(_root, "wal");

        DataPathGuard.EnsureDirectoryWritable(target, "WAL");

        Assert.That(Directory.Exists(target), Is.True);
    }

    [Test]
    public void EnsureDirectoryWritable_accepts_an_existing_writable_directory()
    {
        Directory.CreateDirectory(_root);

        Assert.That(() => DataPathGuard.EnsureDirectoryWritable(_root, "data"), Throws.Nothing);
    }

    [Test]
    public void EnsureDirectoryWritable_leaves_no_probe_file_behind()
    {
        DataPathGuard.EnsureDirectoryWritable(_root, "data");

        Assert.That(Directory.GetFiles(_root), Is.Empty);
    }

    [TestCase("")]
    [TestCase("   ")]
    public void EnsureDirectoryWritable_rejects_an_empty_path(string path)
        => Assert.That(
            () => DataPathGuard.EnsureDirectoryWritable(path, "data"),
            Throws.ArgumentException);
}
