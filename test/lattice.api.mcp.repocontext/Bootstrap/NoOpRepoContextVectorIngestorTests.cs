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
        var unchanged = new[] { new RepoFileEntry("b.cs", "e", 2, "csharp") };

        var embedded = await ingestor.IngestAsync(
            "repo", "/root", changed, unchanged, onProgress: null, TestContext.CurrentContext.CancellationToken);

        // A no-op that embeds nothing is the whole contract: it reports zero files
        // embedded and reaching here without throwing proves the seam is inert -
        // it ignores both the changed and the unchanged offering.
        Assert.That(embedded, Is.EqualTo(0));
    }

    [Test]
    public void IngestAsync_returns_a_completed_value_task_synchronously()
    {
        var ingestor = new NoOpRepoContextVectorIngestor();

        var task = ingestor.IngestAsync(
            "repo",
            "/root",
            Array.Empty<RepoFileEntry>(),
            Array.Empty<RepoFileEntry>(),
            onProgress: null,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(task.IsCompletedSuccessfully, Is.True);
            Assert.That(task.Result, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task RetireAsync_completes_without_persisting_anything()
    {
        var ingestor = new NoOpRepoContextVectorIngestor();

        // The default seam never stored a vector, so retiring a removed file is
        // inert: it completes without throwing and touches no store.
        await ingestor.RetireAsync(
            "repo", new[] { "gone.cs" }, TestContext.CurrentContext.CancellationToken);

        Assert.Pass("The no-op retire completed without touching any store.");
    }
}
