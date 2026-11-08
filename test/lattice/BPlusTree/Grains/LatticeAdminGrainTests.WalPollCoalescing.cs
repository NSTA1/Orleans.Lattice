using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6017: every silo's poller calls the singleton admin grain's
/// interleaved <see cref="ILatticeAdmin.PollWalUsageAsync"/>, so concurrent
/// calls must share one fan-out rather than multiplying the WAL scans they
/// issue, a finished poll must never be reused, and one caller giving up must
/// not cancel the poll the others joined.
/// </summary>
public sealed partial class LatticeAdminGrainTests
{
    [Test]
    public async Task PollWalUsageAsync_concurrent_callers_share_one_fan_out()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha" }));
        var factory = Substitute.For<IGrainFactory>();
        var gate = new TaskCompletionSource<TreeWalUsageReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>()).Returns(_ => gate.Task);
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        var grain = CreateGrain(factory, registry);

        var polls = Enumerable.Range(0, 3).Select(_ => grain.PollWalUsageAsync(CancellationToken.None)).ToList();
        gate.SetResult(new TreeWalUsageReport { TreeId = "alpha" });
        await Task.WhenAll(polls).WaitAsync(TimeSpan.FromSeconds(5));

        await walAlpha.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
        await registry.Received(1).GetAllTreeIdsAsync();
    }

    [Test]
    public async Task PollWalUsageAsync_after_a_faulted_poll_starts_a_fresh_one()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(
            _ => Task.FromException<IReadOnlyList<string>>(new InvalidOperationException("registry down")),
            _ => Task.FromResult<IReadOnlyList<string>>(new[] { "alpha" }));
        var factory = Substitute.For<IGrainFactory>();
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>()).Returns(new TreeWalUsageReport { TreeId = "alpha" });
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        var grain = CreateGrain(factory, registry);

        Assert.That(async () => await grain.PollWalUsageAsync(CancellationToken.None), Throws.InvalidOperationException);
        Assert.That(async () => await grain.PollWalUsageAsync(CancellationToken.None), Throws.Nothing,
            "a later caller must start a fresh poll, not be served the earlier fault");

        await walAlpha.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PollWalUsageAsync_cancelling_one_caller_detaches_it_without_cancelling_the_shared_poll()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha" }));
        var factory = Substitute.For<IGrainFactory>();
        var gate = new TaskCompletionSource<TreeWalUsageReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var usageTokens = new List<CancellationToken>();
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>()).Returns(call =>
        {
            usageTokens.Add(call.Arg<CancellationToken>());
            return gate.Task;
        });
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        var grain = CreateGrain(factory, registry);

        using var callerASource = new CancellationTokenSource();
        var callerA = grain.PollWalUsageAsync(callerASource.Token);
        var callerB = grain.PollWalUsageAsync(CancellationToken.None);

        callerASource.Cancel();
        Assert.That(
            async () => await callerA.WaitAsync(TimeSpan.FromSeconds(5)),
            Throws.InstanceOf<OperationCanceledException>(),
            "the cancelling caller stops waiting at once");
        Assert.That(callerB.IsCompleted, Is.False, "the other caller is still waiting on the shared poll");

        gate.SetResult(new TreeWalUsageReport { TreeId = "alpha" });
        await callerB.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(usageTokens, Has.Count.EqualTo(1), "both callers share one fan-out");
            Assert.That(usageTokens.All(t => !t.IsCancellationRequested), Is.True,
                "one caller's cancellation must not cancel the poll another caller joined");
        });
    }
}
