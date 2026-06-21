using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// F-116 entry / key-range inspection multi-silo coverage. A scan opens a
/// snapshot cursor that lives on the originating silo, and a single-key detail
/// read fans out to whichever shard owns the key; both must return the same
/// result when served by a facade on a different silo from the one that wrote
/// the data.
/// </summary>
public sealed partial class MultiSiloStateApiIntegrationTests
{
    [Test]
    public async Task ScanEntries_pages_all_entries_from_another_silo()
    {
        const string treeId = "multisilo-scan";
        const int count = 60;
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: count, shardCount: MultiSiloStateApiClusterFixture.ShardCount);

        var query = _fixture.QueryFromOtherSilo();
        var all = new List<EntryRecord>();
        string? token = null;
        do
        {
            var page = await query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = treeId,
                PageSize = 8,
                ContinuationToken = token,
            });
            Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
            all.AddRange(page.Entries);
            token = page.ContinuationToken;
        }
        while (token is not null);

        var keys = all.Select(e => e.Key).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Has.Length.EqualTo(count), "a cross-silo scan must drain every entry");
            Assert.That(keys, Is.Unique, "a cross-silo paged scan must not duplicate entries");
            Assert.That(keys, Is.Ordered, "a cross-silo scan must preserve ascending key order");
        });
    }

    [Test]
    public async Task GetEntry_reads_from_another_silo()
    {
        const string treeId = "multisilo-detail";
        var tree = await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 30, shardCount: MultiSiloStateApiClusterFixture.ShardCount);
        await tree.SetAsync("focus-key", Encoding.UTF8.GetBytes("focus-value"));

        var result = await _fixture.QueryFromOtherSilo().GetEntryAsync(treeId, "focus-key");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found),
            "a single-key detail read must resolve from a facade on any silo");
        Assert.That(Encoding.UTF8.GetString(result.Entry!.ValuePreview), Is.EqualTo("focus-value"));
    }
}
