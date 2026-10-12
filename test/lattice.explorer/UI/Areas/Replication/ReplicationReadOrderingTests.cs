using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>Overlapping replication reads must keep the newest request's result.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationReadOrderingTests : ReplicationTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Estate_cache_does_not_remember_an_older_reply_or_fault(bool fault)
    {
        var pending = new TaskCompletionSource<ReplicationPeerStatusPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(_ => pending.Task, _ => Task.FromResult(Page("current")));
        Services.AddKeyedSingleton(ShellFacades.Key, status);

        var previous = Data.GetEstateAsync(refresh: true, CancellationToken.None);
        await Data.GetEstateAsync(refresh: true, CancellationToken.None);
        Complete(pending, Page("stale"), fault);
        await previous;
        var cached = await Data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cached.Fault, Is.Null);
            Assert.That(cached.Value?.Links.Single().PeerRegionId, Is.EqualTo("current"));
            Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(2), "the current result is reused, not refetched");
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Enrolment_cache_does_not_remember_an_older_reply_or_fault(bool fault)
    {
        var pending = new TaskCompletionSource<ReplicationConfigReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(_ => pending.Task, _ => Task.FromResult(new ReplicationConfigReport([Tree("current")])));
        Services.AddKeyedSingleton(ShellFacades.Key, control);

        var previous = Data.GetConfigAsync(refresh: true, CancellationToken.None);
        await Data.GetConfigAsync(refresh: true, CancellationToken.None);
        Complete(pending, new ReplicationConfigReport([Tree("stale")]), fault);
        await previous;
        var cached = await Data.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cached.Fault, Is.Null);
            Assert.That(cached.Value?.Trees.Single().TreeId, Is.EqualTo("current"));
            Assert.That(control.ReceivedCalls().Count(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Estate_and_enrolment_reads_do_not_supersede_each_others_cache()
    {
        var estatePending = new TaskCompletionSource<ReplicationPeerStatusPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var configPending = new TaskCompletionSource<ReplicationConfigReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var status = Substitute.For<ILatticeReplicationStatus>();
        var control = Substitute.For<ILatticeReplicationControl>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(_ => estatePending.Task);
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>()).Returns(_ => configPending.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, status);
        Services.AddKeyedSingleton(ShellFacades.Key, control);

        var estateRead = Data.GetEstateAsync(refresh: true, CancellationToken.None);
        var configRead = Data.GetConfigAsync(refresh: true, CancellationToken.None);
        estatePending.SetResult(Page("current"));
        configPending.SetResult(new ReplicationConfigReport([Tree("current")]));
        await Task.WhenAll(estateRead, configRead);
        await Data.GetEstateAsync(refresh: false, CancellationToken.None);
        await Data.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(1));
            Assert.That(control.ReceivedCalls().Count(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Cancelled_estate_read_is_not_cached_when_the_facade_ignores_cancellation()
    {
        var pending = new TaskCompletionSource<ReplicationPeerStatusPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(_ => pending.Task, _ => Task.FromResult(Page("current")));
        Services.AddKeyedSingleton(ShellFacades.Key, status);
        using var cancellation = new CancellationTokenSource();

        var read = Data.GetEstateAsync(refresh: true, cancellation.Token);
        cancellation.Cancel();
        pending.SetResult(Page("stale"));
        await read;
        var cached = await Data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.That(cached.Value?.Links.Single().PeerRegionId, Is.EqualTo("current"));
        Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(2));
    }

    [Test]
    public async Task Cancelled_enrolment_read_is_not_cached_when_the_facade_ignores_cancellation()
    {
        var pending = new TaskCompletionSource<ReplicationConfigReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(_ => pending.Task, _ => Task.FromResult(new ReplicationConfigReport([Tree("current")])));
        Services.AddKeyedSingleton(ShellFacades.Key, control);
        using var cancellation = new CancellationTokenSource();

        var read = Data.GetConfigAsync(refresh: true, cancellation.Token);
        cancellation.Cancel();
        pending.SetResult(new ReplicationConfigReport([Tree("stale")]));
        await read;
        var cached = await Data.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.That(cached.Value?.Trees.Single().TreeId, Is.EqualTo("current"));
        Assert.That(control.ReceivedCalls().Count(), Is.EqualTo(2));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Enrolled_trees_discard_a_superseded_retry_reply_or_fault(bool fault)
    {
        var pending = new TaskCompletionSource<ReplicationConfigReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromException<ReplicationConfigReport>(new TimeoutException()),
                _ => pending.Task,
                _ => Task.FromResult(new ReplicationConfigReport([Tree("current")])));
        Services.AddKeyedSingleton(ShellFacades.Key, control);
        var cut = RenderAt<ReplicationTreesPage>("replication/trees");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Does.Contain("could not be read")));

        var previous = cut.Find(".lt-empty button").ClickAsync();
        cut.WaitUntil(() => Assert.That(control.ReceivedCalls().Count(), Is.EqualTo(2)));
        await cut.Find(".lt-empty button").ClickAsync();
        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("current")));
        Complete(pending, new ReplicationConfigReport([Tree("stale")]), fault);
        await previous;

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-empty"), Is.Empty);
            Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("current"));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Tree_page_discards_its_initial_reply_after_a_newer_cadence_read(bool fault)
    {
        var pending = new TaskCompletionSource<ReplicationPeerStatusPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(_ => pending.Task, _ => Task.FromResult(Page("current")));
        Services.AddKeyedSingleton(ShellFacades.Key, status);
        Control.Trees.Add(Tree("orders"));
        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");
        cut.WaitUntil(() => Assert.That(cut.Instance.Loop?.IsRunning, Is.True));
        Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(1));

        await cut.InvokeAsync(() => Time.Advance(new ReplicationOptions().RefreshInterval));
        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("current")));
        Complete(pending, Page("stale"), fault);

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("stale", StringComparison.Ordinal)
                || cut.Markup.Contains("The last refresh failed", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("current"));
    }

    private static ReplicationPeerStatusPage Page(string peer) => new("eu-west", [Link("orders", peer)], null);

    private static void Complete<T>(TaskCompletionSource<T> pending, T value, bool fault)
    {
        if (fault)
        {
            pending.SetException(new TimeoutException());
        }
        else
        {
            pending.SetResult(value);
        }
    }
}
