using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Tests.History;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class EntryLiveFollowerTests
{
    [Test]
    public async Task FollowAsync_PushedForSelectedKey_Signals()
    {
        var client = FakeObserveStateClient.WithSequence(
            NotificationFactory.Set("k", 10),
            NotificationFactory.Set("k", 20));
        var follower = new EntryLiveFollower(client);

        var signals = await CollectAsync(follower, "k");

        Assert.Multiple(() =>
        {
            Assert.That(signals, Has.Count.EqualTo(2));
            Assert.That(signals[0].Hlc.WallClockTicks, Is.EqualTo(10));
            Assert.That(signals[1].Hlc.WallClockTicks, Is.EqualTo(20));
            Assert.That(signals, Has.All.Matches<EntryChangeSignal>(s => s.Key == "k"));
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
        var follower = new EntryLiveFollower(client);

        var signals = await CollectAsync(follower, "k");

        Assert.Multiple(() =>
        {
            Assert.That(signals, Has.Count.EqualTo(2));
            Assert.That(signals.Select(s => s.Hlc.WallClockTicks), Is.EqualTo(new long[] { 10, 20 }));
        });
    }

    [Test]
    public async Task FollowAsync_SetSignal_CarriesKindAndClock()
    {
        var client = FakeObserveStateClient.WithSequence(NotificationFactory.Delete("k", 30));
        var follower = new EntryLiveFollower(client);

        var signals = await CollectAsync(follower, "k");

        Assert.Multiple(() =>
        {
            Assert.That(signals, Has.Count.EqualTo(1));
            Assert.That(signals[0].Key, Is.EqualTo("k"));
            Assert.That(signals[0].Kind, Is.EqualTo(StateChangeKind.Delete));
            Assert.That(signals[0].Hlc.WallClockTicks, Is.EqualTo(30));
        });
    }

    [Test]
    public async Task FollowAsync_RangeDeleteCoveringKey_Signals()
    {
        // A half-open range delete [a, m) sweeps "k": the followed entry is
        // affected, so the tab must be told to refetch.
        var client = FakeObserveStateClient.WithSequence(NotificationFactory.DeleteRange("a", "m", 10));
        var follower = new EntryLiveFollower(client);

        var signals = await CollectAsync(follower, "k");

        Assert.Multiple(() =>
        {
            Assert.That(signals, Has.Count.EqualTo(1));
            Assert.That(signals[0].Key, Is.EqualTo("k"));
            Assert.That(signals[0].Kind, Is.EqualTo(StateChangeKind.DeleteRange));
        });
    }

    [Test]
    public async Task FollowAsync_RangeDeleteNotCoveringKey_FilteredOut()
    {
        // The range [a, c) does not include "k" (c <= k), so nothing surfaces.
        var client = FakeObserveStateClient.WithSequence(NotificationFactory.DeleteRange("a", "c", 10));
        var follower = new EntryLiveFollower(client);

        var signals = await CollectAsync(follower, "k");

        Assert.That(signals, Is.Empty);
    }

    [Test]
    public async Task FollowAsync_Cancelled_StopsEnumeration()
    {
        var client = FakeObserveStateClient.Channelled();
        var follower = new EntryLiveFollower(client);
        using var cts = new CancellationTokenSource();

        var seen = new List<EntryChangeSignal>();
        var consume = Task.Run(async () =>
        {
            await foreach (var signal in follower.FollowAsync("tree-1", "k", cts.Token))
            {
                seen.Add(signal);
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
    public void FollowAsync_EmptyTree_Throws()
    {
        var follower = new EntryLiveFollower(FakeObserveStateClient.WithSequence());

        Assert.That(async () =>
        {
            await foreach (var _ in follower.FollowAsync("", "k"))
            {
            }
        }, Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void FollowAsync_NullKey_Throws()
    {
        var follower = new EntryLiveFollower(FakeObserveStateClient.WithSequence());

        Assert.That(async () =>
        {
            await foreach (var _ in follower.FollowAsync("tree-1", null!))
            {
            }
        }, Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_NullClient_Throws()
    {
        Assert.That(() => new EntryLiveFollower(null!), Throws.ArgumentNullException);
    }

    private static async Task<List<EntryChangeSignal>> CollectAsync(IEntryLiveFollower follower, string key)
    {
        var signals = new List<EntryChangeSignal>();
        await foreach (var signal in follower.FollowAsync("tree-1", key))
        {
            signals.Add(signal);
        }

        return signals;
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
