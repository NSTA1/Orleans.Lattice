using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [TestCase(-1L, -1L, 0L)]
    [TestCase(9L, 4L, 4L)]
    [TestCase(9L, -1L, 10L)]
    [TestCase(9L, 12L, 10L)]
    public async Task GetResumeFloorAsync_preserves_the_next_minus_live_calculation(long highest, long lowest, long expected)
    {
        var provider = Substitute.For<IWalStorageProvider>();
        provider.GetHighestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(highest);
        provider.GetLowestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(lowest);
        var grain = await CreateGrainAsync(provider);
        provider.ClearReceivedCalls();

        Assert.That(await grain.GetResumeFloorAsync(CancellationToken.None), Is.EqualTo(expected));
        await provider.Received(1).GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        if (highest >= 0)
        {
            Received.InOrder(() =>
            {
                provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
                provider.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
            });
        }
        else
            await provider.DidNotReceiveWithAnyArgs().GetLowestOffsetAsync(default!, default, default);
    }

    [Test]
    public async Task GetResumeFloorAsync_samples_allocator_before_the_provider_read()
    {
        var provider = Substitute.For<IWalStorageProvider>();
        provider.GetHighestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(9L);
        var grain = await CreateGrainAsync(provider);
        var highest = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        provider.GetHighestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(highest.Task);
        provider.GetLowestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(5L);

        var floor = grain.GetResumeFloorAsync(CancellationToken.None);
        var append = grain.AppendAsync(MakeEntry("new"), CancellationToken.None);
        highest.SetResult(10L);
        await append;

        Assert.That(await floor, Is.EqualTo(4L), "sampled next=10 minus the later live span=6, not lowest=5");
        Assert.That(await grain.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(11L));
    }

    [Test]
    public async Task GetResumeFloorAsync_precancelled_token_never_queries_provider()
    {
        var provider = Substitute.For<IWalStorageProvider>();
        var grain = await CreateGrainAsync(provider);
        provider.ClearReceivedCalls();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => grain.GetResumeFloorAsync(cts.Token));
        Assert.That(provider.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task GetResumeFloorAsync_propagates_provider_failure_without_reading_lowest()
    {
        var provider = Substitute.For<IWalStorageProvider>();
        var grain = await CreateGrainAsync(provider);
        provider.GetHighestOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>())
            .Returns<Task<long>>(_ => throw new TimeoutException("unavailable"));
        provider.ClearReceivedCalls();

        Assert.ThrowsAsync<TimeoutException>(() => grain.GetResumeFloorAsync(CancellationToken.None));
        await provider.DidNotReceiveWithAnyArgs().GetLowestOffsetAsync(default!, default, default);
    }
}
