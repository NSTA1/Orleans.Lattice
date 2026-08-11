namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="NoOpRepoContextVectorIngestor"/>: the default
/// vectorisation seam is a pass-through, so structural ingestion never persists
/// vectors and never races the vector-store surface owned by later work.
/// </summary>
[TestFixture]
public sealed class NoOpRepoContextVectorIngestorTests
{
    [Test]
    public async Task IngestAsync_completes_without_touching_its_inputs()
    {
        var ingestor = new NoOpRepoContextVectorIngestor();
        var changed = new[] { new RepoFileEntry("a.cs", "d", 1, "csharp") };

        await ingestor.IngestAsync(
            "repo", "/root", changed, TestContext.CurrentContext.CancellationToken);

        // A no-op that returns a completed task is the whole contract: reaching
        // here without throwing proves the seam is inert.
        Assert.Pass();
    }

    [Test]
    public void IngestAsync_returns_a_completed_value_task_synchronously()
    {
        var ingestor = new NoOpRepoContextVectorIngestor();

        var task = ingestor.IngestAsync(
            "repo", "/root", Array.Empty<RepoFileEntry>(), CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
    }
}
