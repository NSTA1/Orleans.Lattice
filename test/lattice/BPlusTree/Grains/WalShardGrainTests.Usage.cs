using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [TestCase(100L, 800L)]
    [TestCase(100L, -1L)]
    [TestCase(-1L, -1L)]
    public async Task GetByteSizesAsync_returns_provider_observations_without_rewriting_sentinels(
        long retained, long physical)
    {
        var provider = Substitute.For<IWalStorageProvider>();
        provider.GetPhysicalByteSizeAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(physical);
        provider.GetRetainedByteSizeAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>()).Returns(retained);
        var grain = await CreateGrainAsync(provider);
        provider.ClearReceivedCalls();

        var result = await grain.GetByteSizesAsync(CancellationToken.None);

        Assert.That(result, Is.EqualTo((retained, physical)));
        Received.InOrder(() =>
        {
            provider.GetPhysicalByteSizeAsync(TreeId, ShardIndex, CancellationToken.None);
            provider.GetRetainedByteSizeAsync(TreeId, ShardIndex, CancellationToken.None);
        });
        await provider.Received(1).GetPhysicalByteSizeAsync(TreeId, ShardIndex, CancellationToken.None);
        await provider.Received(1).GetRetainedByteSizeAsync(TreeId, ShardIndex, CancellationToken.None);
    }

    [Test]
    public async Task GetByteSizesAsync_precancelled_token_does_not_query_provider()
    {
        var provider = Substitute.For<IWalStorageProvider>();
        var grain = await CreateGrainAsync(provider);
        provider.ClearReceivedCalls();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() => grain.GetByteSizesAsync(cts.Token));

        await provider.DidNotReceiveWithAnyArgs().GetPhysicalByteSizeAsync(default!, default, default);
        await provider.DidNotReceiveWithAnyArgs().GetRetainedByteSizeAsync(default!, default, default);
    }

    [Test]
    public async Task GetByteSizesAsync_provider_failure_propagates_without_a_success_pair()
    {
        var provider = Substitute.For<IWalStorageProvider>();
        var grain = await CreateGrainAsync(provider);
        provider.GetPhysicalByteSizeAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>())
            .Returns<Task<long>>(_ => throw new TimeoutException("unavailable"));
        provider.ClearReceivedCalls();

        Assert.ThrowsAsync<TimeoutException>(() => grain.GetByteSizesAsync(CancellationToken.None));

        await provider.DidNotReceiveWithAnyArgs().GetRetainedByteSizeAsync(default!, default, default);
    }
}
