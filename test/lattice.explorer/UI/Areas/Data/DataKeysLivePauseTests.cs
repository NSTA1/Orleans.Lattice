using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// Pausing live updates on the Keys tab (#5798): what the change feed already set in motion is
/// dropped, what the user asked for still lands, and resuming catches up exactly once.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataKeysLivePauseTests : DataTestContext
{
    [Test]
    public async Task Pausing_discards_a_change_driven_refresh_in_flight_and_resuming_catches_up_once()
    {
        Client.WithTree("orders", keys: 2);
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstScans = 0;
        Client.ScanReply = async (request, _) =>
        {
            if (request.ContinuationToken is null && Interlocked.Increment(ref firstScans) == 2)
            {
                // The refresh the change feed starts: still reading when the user pauses.
                await held.Task;
            }

            return Current(request.TreeId);
        };
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
        });

        Client.Entries["orders"]["key/0002"] = FakeStateClient.Entry("key/0002", "\"new\"");
        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("orders", "key/0002"));
        cut.WaitUntil(() => Assert.That(Volatile.Read(ref firstScans), Is.EqualTo(2)));

        // The feed's loop is still inside the refresh, so it closes only once that ends.
        Switch(cut, "Live updates").Click();
        cut.WaitUntil(() => Assert.That(Switch(cut, "Live updates").GetAttribute("aria-checked"), Is.EqualTo("false")));
        Client.Entries["orders"]["key/0003"] = FakeStateClient.Entry("key/0003", "\"new\"");
        held.SetResult();

        Assert.That(await TestPoll.TryUntilAsync(
            () => Rows(cut).Count != 2 || Volatile.Read(ref firstScans) != 2,
            TimeSpan.FromSeconds(1)), Is.False, "paused, the table holds still and reads nothing more");

        Switch(cut, "Live updates").Click();
        cut.WaitUntil(() =>
        {
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(Rows(cut).Select(row => row.QuerySelector("th")!.TextContent), Is.EqualTo(new[] { "key/0000", "key/0001", "key/0002", "key/0003" }));
        });
        Assert.That(await TestPoll.TryUntilAsync(() => Volatile.Read(ref firstScans) > 3, TimeSpan.FromSeconds(1)), Is.False, "resuming reads the page exactly once");
        Assert.That(Volatile.Read(ref firstScans), Is.EqualTo(3));
    }

    [Test]
    public void Pausing_keeps_a_read_the_user_started()
    {
        Client.WithTree("orders", keys: 2);
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.ScanReply = async (request, _) =>
        {
            if (request.PageSize == 50)
            {
                await held.Task;
            }

            return Current(request.TreeId);
        };
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
        });

        Control(cut, "Page size").Change("50");
        cut.WaitUntil(() => Assert.That(Client.Calls.Count(call => call == "ScanEntriesAsync:first"), Is.EqualTo(2)));
        Switch(cut, "Live updates").Click();
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(0)));

        Client.Entries["orders"]["key/0002"] = FakeStateClient.Entry("key/0002", "\"new\"");
        held.SetResult();

        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(3)));
    }

    private EntryScanResponse Current(string tree) => new()
    {
        TreeId = tree,
        Entries = [.. Client.Entries[tree].Values],
    };
}
