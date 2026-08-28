using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the projection-digest override and one-way latch
/// methods on <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain"/>:
/// <see cref="ILatticeRegistry.SetMaintainProjectionDigestAsync"/> and
/// <see cref="ILatticeRegistry.LatchProjectionDigestPermanentlyDisabledAsync"/>.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    [Test]
    public async Task SetMaintainProjectionDigestAsync_persists_override()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        byte[]? captured = null;
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));

        await grain.SetMaintainProjectionDigestAsync("my-tree", false);

        await tree.Received(1).SetAsync("my-tree", Arg.Any<byte[]>());
        Assert.That(captured, Is.Not.Null);
    }

    [Test]
    public async Task SetMaintainProjectionDigestAsync_null_clears_override()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        byte[]? captured = null;
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));

        await grain.SetMaintainProjectionDigestAsync("my-tree", null);

        // Round-trip the persisted entry to confirm the override is null.
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry!.MaintainProjectionDigest, Is.Null);
    }

    [Test]
    public async Task SetMaintainProjectionDigestAsync_preserves_other_fields()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        var existing = new TreeRegistryEntry
        {
            MaxLeafKeys = 512,
            MaxInternalChildren = 64,
            ShardCount = 4,
            PublishEvents = true,
        };
        byte[]? captured = null;
        tree.ExistsAsync("my-tree").Returns(false);
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));
        await grain.RegisterAsync("my-tree", existing);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));

        await grain.SetMaintainProjectionDigestAsync("my-tree", false);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var roundTripped = await grain.GetEntryAsync("my-tree");

        Assert.That(roundTripped!.MaxLeafKeys, Is.EqualTo(512));
        Assert.That(roundTripped.PublishEvents, Is.True);
        Assert.That(roundTripped.MaintainProjectionDigest, Is.False);
    }

    [Test]
    public async Task LatchProjectionDigestPermanentlyDisabledAsync_stamps_when_unset()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        byte[]? captured = null;
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));

        await grain.LatchProjectionDigestPermanentlyDisabledAsync("my-tree");

        await tree.Received(1).SetAsync("my-tree", Arg.Any<byte[]>());
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry!.ProjectionDigestPermanentlyDisabled, Is.True);
    }

    [Test]
    public async Task LatchProjectionDigestPermanentlyDisabledAsync_is_idempotent()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        var alreadyLatched = new TreeRegistryEntry
        {
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
            ShardCount = 1,
            ProjectionDigestPermanentlyDisabled = true,
        };
        byte[]? captured = null;
        tree.ExistsAsync("my-tree").Returns(false);
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));
        await grain.RegisterAsync("my-tree", alreadyLatched);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        tree.ClearReceivedCalls();

        await grain.LatchProjectionDigestPermanentlyDisabledAsync("my-tree");

        // No additional write must happen when the latch is already set.
        await tree.DidNotReceive().SetAsync("my-tree", Arg.Any<byte[]>());
    }

    [Test]
    public void SetMaintainProjectionDigestAsync_throws_on_null_treeId()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await grain.SetMaintainProjectionDigestAsync(null!, true));
    }

    [Test]
    public void LatchProjectionDigestPermanentlyDisabledAsync_throws_on_null_treeId()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await grain.LatchProjectionDigestPermanentlyDisabledAsync(null!));
    }
}

