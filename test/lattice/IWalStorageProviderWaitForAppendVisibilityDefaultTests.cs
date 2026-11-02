namespace Orleans.Lattice.Tests;

/// <summary>
/// The default <see cref="IWalStorageProvider.WaitForAppendVisibilityAsync"/>
/// completes at once: a provider whose appends are readable when the append
/// returns has nothing to publish (issue #5470).
/// </summary>
[TestFixture]
public sealed class IWalStorageProviderWaitForAppendVisibilityDefaultTests
{
    [Test]
    public async Task The_default_completes_at_once_and_leaves_returned_appends_readable()
    {
        var inMemory = new InMemoryWalStorageProvider();
        IWalStorageProvider provider = inMemory;
        await provider.AppendBatchAsync(
            "tree", 0, [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = "tree", Key = "key" } }],
            CancellationToken.None);

        var barrier = provider.WaitForAppendVisibilityAsync("tree", 0, CancellationToken.None);

        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync("tree", 0, -1, 10, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }
        Assert.Multiple(() =>
        {
            Assert.That(barrier.IsCompletedSuccessfully, Is.True);
            Assert.That(offsets, Is.EqualTo(new long[] { 0 }));
        });
    }
}
