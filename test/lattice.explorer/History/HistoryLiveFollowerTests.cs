using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryLiveFollowerTests
{
    [Test]
    public async Task FollowAsync_PushedForSelectedKey_Appears()
    {
        var client = FakeObserveStateClient.WithSequence(
            NotificationFactory.Set("k", 10),
            NotificationFactory.Set("k", 20));
        var follower = new HistoryLiveFollower(client);
        var tail = new HistoryLiveTail("k");

        var rows = await CollectAsync(follower, tail);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].Hlc.WallClockTicks, Is.EqualTo(10));
            Assert.That(rows[1].Hlc.WallClockTicks, Is.EqualTo(20));
            Assert.That(rows, Has.All.Matches<HistoryRevisionRow>(r => r.IsLiveTail));
            Assert.That(client.LastObserve!.TreeId, Is.EqualTo("tree-1"));
            Assert.That(client.LastObserve!.IncludeMaintenance, Is.False);
        });
    }

    [Test]
    public async Task FollowAsync_NotificationForOtherKey_FilteredOut()
    {
        var client = FakeObserveStateClient.WithSequence(
            NotificationFactory.Set("k", 10),
            NotificationFactory.Set("other", 15),
            NotificationFactory.Set("k", 20));
        var follower = new HistoryLiveFollower(client);
        var tail = new HistoryLiveTail("k");

        var rows = await CollectAsync(follower, tail);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 10, 20 }));
        });
    }

    [Test]
    public async Task FollowAsync_OverlapWithLoadedPage_DeDuplicated()
    {
        // The loaded page already shows clock 10; the live tail re-emits it then a
        // new clock 20 - only the fresh one must surface.
        var loaded = new[] { HistoryRevisionRow.From(RevisionFactory.Set(10, value: "v")) };
        var tail = new HistoryLiveTail("k", loaded);
        var client = FakeObserveStateClient.WithSequence(
            NotificationFactory.Set("k", 10),
            NotificationFactory.Set("k", 20));
        var follower = new HistoryLiveFollower(client);

        var rows = await CollectAsync(follower, tail);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(1));
            Assert.That(rows[0].Hlc.WallClockTicks, Is.EqualTo(20));
        });
    }

    [Test]
    public async Task FollowAsync_Cancelled_StopsEnumeration()
    {
        var client = FakeObserveStateClient.Channelled();
        var follower = new HistoryLiveFollower(client);
        var tail = new HistoryLiveTail("k");
        using var cts = new CancellationTokenSource();

        var seen = new List<HistoryRevisionRow>();
        var consume = Task.Run(async () =>
        {
            await foreach (var row in follower.FollowAsync("tree-1", tail, cts.Token))
            {
                seen.Add(row);
            }
        });

        client.Push(NotificationFactory.Set("k", 10));
        await WaitForAsync(() => seen.Count == 1);

        // Simulate a key change / tab dispose: cancelling the token must end the
        // subscription rather than leaking the loop.
        cts.Cancel();

        Assert.That(async () => await consume, Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(seen, Has.Count.EqualTo(1));
            Assert.That(client.ObserveCancelled, Is.True);
        });
    }

    [Test]
    public void FollowAsync_NullTree_Throws()
    {
        var follower = new HistoryLiveFollower(FakeObserveStateClient.WithSequence());

        Assert.That(async () =>
        {
            await foreach (var _ in follower.FollowAsync("", new HistoryLiveTail("k")))
            {
            }
        }, Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void FollowAsync_NullTail_Throws()
    {
        var follower = new HistoryLiveFollower(FakeObserveStateClient.WithSequence());

        Assert.That(async () =>
        {
            await foreach (var _ in follower.FollowAsync("tree-1", null!))
            {
            }
        }, Throws.ArgumentNullException);
    }

    private static async Task<List<HistoryRevisionRow>> CollectAsync(
        IHistoryLiveFollower follower,
        HistoryLiveTail tail)
    {
        var rows = new List<HistoryRevisionRow>();
        await foreach (var row in follower.FollowAsync("tree-1", tail))
        {
            rows.Add(row);
        }

        return rows;
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++)
        {
            await Task.Delay(10);
        }

        Assert.That(condition(), Is.True, "condition was not met within the timeout");
    }
}
