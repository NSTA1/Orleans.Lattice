namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the class remarks of the purged-id reuse partial (issue #4294). Issue
/// #4219 stopped a read from re-registering a purged tree id, but the remarks
/// went on claiming that "the next read or write" registers a new tree. The
/// behaviour itself is pinned by the cluster tests in
/// <c>TreeDeletionIntegrationTests.PurgedIdResurrection</c>; this test keeps the
/// prose from drifting back to the pre-#4219 claim.
/// </summary>
public partial class TreeDeletionGrainTests
{
    [Test]
    public void Reuse_remarks_do_not_claim_a_read_reregisters_a_purged_id()
    {
        var path = RepositorySourceFile("src/lattice/BPlusTree/Grains/TreeDeletionGrain.Reuse.cs");
        Assert.That(File.Exists(path), Is.True, $"Source file not found: {path}");

        var text = File.ReadAllText(path);
        Assert.Multiple(() =>
        {
            Assert.That(text, Is.Not.Empty);
            Assert.That(text, Does.Not.Contain("next read or write"),
                "A read never re-registers a purged id (issue #4219); the remarks must not say it does.");
            Assert.That(text, Does.Contain("PurgedTreeRegistrationGuard"),
                "The remarks should point at the guard that keeps a read from re-registering a purged id.");
            Assert.That(text, Does.Contain("#4219"));
        });
    }

    private static string RepositorySourceFile(string relative)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Orleans.Lattice.slnx")))
        {
            dir = dir.Parent;
        }

        Assert.That(dir, Is.Not.Null, "Could not locate the repository root (Orleans.Lattice.slnx).");
        return Path.Combine(dir!.FullName, relative.Replace('/', Path.DirectorySeparatorChar));
    }
}
