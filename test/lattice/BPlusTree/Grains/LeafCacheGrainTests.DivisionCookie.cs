using System.Reflection;
using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The consumer-side arm for issue #2796. The companion arm in
/// <see cref="BPlusLeafGrainTests"/> establishes the unit fact - that a
/// division advances the donor's revision cookie - by reading the registry
/// directly. This arm establishes that the fact is load-bearing, by showing a
/// real <see cref="Orleans.Lattice.BPlusTree.Grains.LeafCacheGrain"/> acting on
/// it.
///
/// The distinction matters because the two can fail independently: the cookie
/// could advance while no reader consults it, and a reader could consult it
/// while nothing advances it. Only the pair together says a stale read is
/// actually prevented.
/// </summary>
public partial class LeafCacheGrainTests
{
    /// <summary>
    /// Builds the sibling a division hands rows to. The division path calls
    /// <c>GetGrainId()</c> on it, which an <see cref="IBPlusLeafGrain"/>
    /// substitute alone cannot serve, so it is substituted as
    /// <see cref="IGrainBase"/> as well and given a context with a real
    /// <see cref="GrainId"/>.
    /// </summary>
    private static IBPlusLeafGrain CreateDivisionSibling(List<string> migrated)
    {
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(call =>
            {
                migrated.AddRange(call.Arg<Dictionary<string, LwwValue<byte[]>>>().Keys);
                return Task.FromResult<SplitResult?>(null);
            });
        return sibling;
    }

    private static async Task InvokeDivisionAsync(
        Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain leaf)
    {
        var split = typeof(Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain).GetMethod(
            "SplitAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                "SplitAsync not found on BPlusLeafGrain - it is the division entry point, "
                + "and driving it directly is what isolates the transfer from a triggering "
                + "write. If it has been renamed, update this helper.");

        await (Task<SplitResult>)split.Invoke(leaf, [])!;
    }

    /// <summary>
    /// Issue #2796. A division moves rows off the donor, so a cache that has
    /// already snapshotted the donor is holding keys the donor no longer owns
    /// and must refresh.
    ///
    /// The <see cref="LatticeOptions.CacheTtl"/> here is deliberately large,
    /// and that is what pins the ordering rather than merely relying on it.
    /// <c>LeafCacheGrain</c> consults the cookie ahead of the TTL gate on
    /// purpose, because on the same silo the revision is the source of truth
    /// and the TTL is only a bandwidth bound for cross-silo. Under a long TTL
    /// that ordering is the only thing that lets the post-division read reach
    /// the primary at all: were the two checks ever transposed, the TTL would
    /// swallow the refresh and this arm would redden, instead of the ordering
    /// changing silently underneath the argument that rents it.
    /// </summary>
    [Test]
    public async Task A_division_on_the_primary_forces_the_cache_to_refresh_even_under_a_long_CacheTtl()
    {
        var migrated = new List<string>();
        var sibling = CreateDivisionSibling(migrated);

        var (cache, registryPopulator, mockPrimary, _) = CreateCacheWithRegistryPopulator(
            nameof(A_division_on_the_primary_forces_the_cache_to_refresh_even_under_a_long_CacheTtl),
            options: new LatticeOptions { CacheTtl = TimeSpan.FromHours(1) },
            populatorSibling: sibling,
            populatorMaxLeafKeys: 64);

        for (var i = 0; i < 16; i++)
        {
            await registryPopulator.SetAsync($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // Snapshot the primary. This read must take the cross-grain path
        // because the cache has not seen a cookie yet.
        await cache.GetAsync("any");
        await mockPrimary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());

        await InvokeDivisionAsync(registryPopulator);

        // Vacuity controls, and they are load-bearing in both directions: the
        // call-count assertion below passes trivially if the division never
        // happened (no rows left, so there was nothing for the cache to miss)
        // and the arm would then be asserting nothing about divisions at all.
        Assert.That(
            migrated,
            Is.Not.Empty,
            "no rows reached the sibling, so no division occurred and the refresh "
            + "assertion below says nothing about the behaviour this arm exists to pin");

        foreach (var key in migrated)
        {
            Assert.That(
                await registryPopulator.GetAsync(key),
                Is.Null,
                $"migrated key '{key}' is still readable from the donor, so the rows did "
                + "not actually leave and the cache had nothing stale to serve");
        }

        await cache.GetAsync("any");

        await mockPrimary.Received(2).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }
}
