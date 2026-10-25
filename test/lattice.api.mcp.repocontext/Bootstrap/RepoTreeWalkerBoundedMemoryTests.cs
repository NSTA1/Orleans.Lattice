using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

[TestFixture]
[NonParallelizable]
public sealed class RepoTreeWalkerBoundedMemoryTests
{
    [Test]
    public void Walk_large_files_do_not_allocate_in_proportion_to_their_length()
    {
        var root = Path.Combine(Path.GetTempPath(), "rc-bounded-scan-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var path = Path.Combine(root, "large.txt");
            var growth = AllocationProbe.Growth(
                size =>
                {
                    File.WriteAllText(path, new string('x', size));
                    return root;
                },
                (directory, size) =>
                {
                    var files = RepoTreeWalker.Walk(directory, null, null);
                    Assert.That(files, Has.Count.EqualTo(1));
                    Assert.That(files[0].SizeBytes, Is.EqualTo(size));
                    AllocationProbe.EscapeSink = files;
                },
                smallSize: 512 * 1024,
                largeSize: 16 * 1024 * 1024,
                attempts: 3,
                crossesThreads: true);

            Assert.That(growth, Is.LessThan(1024 * 1024),
                "Adding 15 MiB to one file must not add a whole-file allocation to the scan.");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
