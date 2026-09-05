using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration test for the multi-page enumeration loop of
/// <see cref="RepoContextStore.ListTopicsAsync"/>. Seeding more memory entries than
/// a single scan page holds (the store pages at 500) forces the loop to follow a
/// continuation token onto a second page; the aggregated per-topic count then only
/// equals the seeded total if the second page was actually read, which pins the
/// continue-past-the-page-boundary arm.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreListTopicsPagingTests
{
    private const string RepoId = "acme";
    private const string Topic = "bulk";
    private const int SeededEntries = 501;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task List_topics_reads_every_page_and_counts_all_entries()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var memory = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);

        // Raw membership writes under one topic: ListTopicsAsync only parses the key
        // to extract the topic, never the value, so a one-byte marker per key is
        // enough. More than one page (500) forces the continuation-token arm.
        var entries = new List<KeyValuePair<string, byte[]>>(SeededEntries);
        for (var i = 0; i < SeededEntries; i++)
        {
            entries.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.Memory(RepoId, Topic, $"e{i:D4}"), new byte[] { 1 }));
        }

        await memory.SetManyAsync(entries, Ct);

        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var result = await store.ListTopicsAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Topics, Has.Count.EqualTo(1), "All entries share one topic.");
            Assert.That(result.Topics[0].Topic, Is.EqualTo(Topic));
            Assert.That(result.Topics[0].EntryCount, Is.EqualTo(SeededEntries),
                "The second page is read and its entries are counted; a single-page read would report 500.");
        });
    }
}
