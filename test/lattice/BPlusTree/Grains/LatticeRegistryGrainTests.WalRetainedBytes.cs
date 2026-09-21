using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the per-tree runtime WAL retained-byte ceiling control-plane
/// setter on
/// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain"/>:
/// <see cref="ILatticeRegistry.SetWalMaxRetainedBytesAsync"/>. Mirrors the
/// cache-value-cap override tests beside it: it persists / clears the override,
/// preserves the other registry fields, and enforces the same <c>&gt;= 1</c>
/// validation the static <see cref="LatticeOptions.WalMaxRetainedBytes"/> option
/// enforces (issue #3333).
/// </summary>
public partial class LatticeRegistryGrainTests
{
    [Test]
    public async Task SetWalMaxRetainedBytesAsync_persists_override()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        byte[]? captured = null;
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));

        await grain.SetWalMaxRetainedBytesAsync("my-tree", 21474836480);

        await tree.Received(1).SetAsync("my-tree", Arg.Any<byte[]>());
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry!.WalMaxRetainedBytes, Is.EqualTo(21474836480));
    }

    [Test]
    public async Task SetWalMaxRetainedBytesAsync_null_clears_override()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        byte[]? captured = null;
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));

        await grain.SetWalMaxRetainedBytesAsync("my-tree", null);

        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry!.WalMaxRetainedBytes, Is.Null);
    }

    [Test]
    public async Task SetWalMaxRetainedBytesAsync_accepts_boundary_value_of_one()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        byte[]? captured = null;
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));

        await grain.SetWalMaxRetainedBytesAsync("my-tree", 1);

        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry!.WalMaxRetainedBytes, Is.EqualTo(1),
            "1 is the inclusive lower bound and must be accepted.");
    }

    [Test]
    public async Task SetWalMaxRetainedBytesAsync_preserves_other_fields()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        var existing = new TreeRegistryEntry
        {
            MaxLeafKeys = 512,
            MaxInternalChildren = 64,
            ShardCount = 4,
            PublishEvents = true,
            MaintainProjectionDigest = false,
            MaxCacheValueBytes = 2048,
        };
        byte[]? captured = null;
        tree.ExistsAsync("my-tree").Returns(false);
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => captured = b));
        await grain.RegisterAsync("my-tree", existing);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));

        await grain.SetWalMaxRetainedBytesAsync("my-tree", 8589934592);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(captured));
        var roundTripped = await grain.GetEntryAsync("my-tree");

        Assert.That(roundTripped!.MaxLeafKeys, Is.EqualTo(512));
        Assert.That(roundTripped.MaxInternalChildren, Is.EqualTo(64));
        Assert.That(roundTripped.ShardCount, Is.EqualTo(4));
        Assert.That(roundTripped.PublishEvents, Is.True);
        Assert.That(roundTripped.MaintainProjectionDigest, Is.False);
        Assert.That(roundTripped.MaxCacheValueBytes, Is.EqualTo(2048),
            "The neighbouring byte-count override must survive untouched; the two are independent.");
        Assert.That(roundTripped.WalMaxRetainedBytes, Is.EqualTo(8589934592));
    }

    [Test]
    public void SetWalMaxRetainedBytesAsync_throws_on_zero()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await grain.SetWalMaxRetainedBytesAsync("my-tree", 0),
            "0 is below the inclusive floor of 1 and must be rejected, mirroring the static-option validator.");
    }

    [Test]
    public void SetWalMaxRetainedBytesAsync_throws_on_negative()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await grain.SetWalMaxRetainedBytesAsync("my-tree", -1));
    }

    [Test]
    public void SetWalMaxRetainedBytesAsync_throws_on_null_treeId()
    {
        var (grain, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await grain.SetWalMaxRetainedBytesAsync(null!, 1024));
    }
}
