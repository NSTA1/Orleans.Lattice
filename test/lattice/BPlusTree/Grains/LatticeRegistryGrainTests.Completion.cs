using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    [Test]
    public async Task RegisterAsync_available_registry_replies_complete_within_bound()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("completion-tree").Returns(Task.FromResult(false));
        tree.SetAsync("completion-tree", Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.RegisterAsync("completion-tree").WaitAsync(TimeSpan.FromSeconds(3));

        await tree.Received(1).SetAsync("completion-tree", Arg.Any<byte[]>());
    }
}
