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

        var embedded = await ingestor.IngestAsync(
            "repo", "/root", changed, onProgress: null, TestContext.CurrentContext.CancellationToken);

        // A no-op that embeds nothing is the whole contract: it reports zero files
        // embedded and reaching here without throwing proves the seam is inert.
        Assert.That(embedded, Is.EqualTo(0));
    }

    [Test]
    public void IngestAsync_returns_a_completed_value_task_synchronously()
    {
        var ingestor = new NoOpRepoContextVectorIngestor();

        var task = ingestor.IngestAsync(
            "repo", "/root", Array.Empty<RepoFileEntry>(), onProgress: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(task.IsCompletedSuccessfully, Is.True);
            Assert.That(task.Result, Is.EqualTo(0));
        });
    }
}
