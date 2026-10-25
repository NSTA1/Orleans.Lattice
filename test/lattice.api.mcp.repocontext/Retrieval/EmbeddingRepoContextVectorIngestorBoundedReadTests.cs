using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class EmbeddingRepoContextVectorIngestorBoundedReadTests
{
    [Test]
    public async Task IngestAsync_large_files_allocate_only_the_bounded_embedding_prefix()
    {
        var root = Path.Combine(Path.GetTempPath(), "rc-bounded-embed-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var ct = TestContext.CurrentContext.CancellationToken;
        try
        {
            await using var harness = await RepoContextMcpHarness.StartAsync(
                new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, ct);
            var provider = new FakeEmbeddingProvider();
            var ingestor = new EmbeddingRepoContextVectorIngestor(
                harness.Services.GetRequiredService<RepoContextVectorWriter>(),
                harness.GrainFactory,
                harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
                NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
                provider);
            const string tail = "must-not-reach-the-embedder";
            var growth = AllocationProbe.Growth(
                size =>
                {
                    provider.CapturedTexts.Clear();
                    File.WriteAllText(Path.Combine(root, "large.txt"), new string('x', size) + tail);
                    return new[] { new RepoFileEntry("large.txt", "digest-" + size, size + tail.Length, "text") };
                },
                (files, _) =>
                {
                    var result = ingestor.IngestAsync(
                        "bounded", root, files, [], onProgress: null, ct).GetAwaiter().GetResult();
                    Assert.That(result.FilesEmbedded, Is.EqualTo(1));
                    Assert.That(provider.CapturedTexts, Is.Not.Empty);
                    Assert.That(provider.CapturedTexts.All(text => !text.Contains(tail, StringComparison.Ordinal)),
                        Is.True);
                },
                smallSize: 512 * 1024,
                largeSize: 16 * 1024 * 1024,
                attempts: 3,
                crossesThreads: true);

            Assert.That(growth, Is.LessThan(2 * 1024 * 1024),
                "The 64 Ki-character prefix must bound the read, not merely truncate an unbounded string afterwards.");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
