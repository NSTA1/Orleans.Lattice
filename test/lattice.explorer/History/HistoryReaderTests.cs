using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Tests.Data;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryReaderTests
{
    [Test]
    public async Task LoadAsync_BuildsRequestWithKeyLimitBudgetAndForwardOrder()
    {
        var client = new FakeEntryStateClient();
        var reader = new HistoryReader(client);

        await reader.LoadAsync("tree-1", "key-1", limit: 25);

        Assert.Multiple(() =>
        {
            Assert.That(client.LastHistory!.TreeId, Is.EqualTo("tree-1"));
            Assert.That(client.LastHistory!.Key, Is.EqualTo("key-1"));
            Assert.That(client.LastHistory!.Limit, Is.EqualTo(25));
            Assert.That(client.LastHistory!.ValuePreviewBudget, Is.EqualTo(HistoryReader.HistoryPreviewBudget));
            Assert.That(client.LastHistory!.Reverse, Is.False, "paging always advances oldest-first");
            Assert.That(client.LastHistory!.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task LoadAsync_PassesContinuationToken()
    {
        var client = new FakeEntryStateClient();
        var reader = new HistoryReader(client);

        await reader.LoadAsync("tree-1", "key-1", limit: 25, continuationToken: "more");

        Assert.That(client.LastHistory!.ContinuationToken, Is.EqualTo("more"));
    }

    [Test]
    public async Task LoadAsync_MapsRevisionsAndMetadata()
    {
        var client = new FakeEntryStateClient
        {
            OnHistory = r => new EntryHistoryResponse
            {
                TreeId = r.TreeId,
                Key = r.Key,
                Status = StateQueryStatus.Found,
                Bound = EntryHistoryBound.Truncated,
                EarliestAvailable = RevisionFactory.Hlc(5),
                ContinuationToken = "next",
                Revisions = new[]
                {
                    RevisionFactory.Set(10, value: "v1"),
                    RevisionFactory.Set(20, value: "v2"),
                },
            },
        };
        var reader = new HistoryReader(client);

        var page = await reader.LoadAsync("t", "k", limit: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(page.Bound, Is.EqualTo(EntryHistoryBound.Truncated));
            Assert.That(page.EarliestAvailable.WallClockTicks, Is.EqualTo(5));
            Assert.That(page.ContinuationToken, Is.EqualTo("next"));
            Assert.That(page.Revisions, Has.Count.EqualTo(2));
            Assert.That(page.Revisions[0].RenderMode, Is.EqualTo(HistoryRowRenderMode.ValueDiff));
        });
    }

    [Test]
    public async Task LoadAsync_EmptyResponse_ReturnsNoRevisions()
    {
        var client = new FakeEntryStateClient
        {
            OnHistory = r => new EntryHistoryResponse { TreeId = r.TreeId, Key = r.Key, Status = StateQueryStatus.KeyNotFound },
        };
        var reader = new HistoryReader(client);

        var page = await reader.LoadAsync("t", "missing", limit: 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
            Assert.That(page.Revisions, Is.Empty);
        });
    }

    [Test]
    public void LoadAsync_NullTree_Throws()
    {
        var reader = new HistoryReader(new FakeEntryStateClient());

        Assert.That(async () => await reader.LoadAsync("", "k", 50), Throws.TypeOf<ArgumentException>());
    }
}
