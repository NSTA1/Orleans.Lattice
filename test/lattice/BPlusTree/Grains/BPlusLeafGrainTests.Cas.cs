using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- GetWithVersionAsync ---

    [Test]
    public async Task GetWithVersion_returns_null_value_for_missing_key()
    {
        var grain = CreateGrain();
        var result = await grain.GetWithVersionAsync("nonexistent");
        Assert.That(result.Value, Is.Null);
        Assert.That(result.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task GetWithVersion_returns_value_and_version_after_set()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var result = await grain.GetWithVersionAsync("k1");
        Assert.That(result.Value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result.Value!), Is.EqualTo("v1"));
        Assert.That(result.Version, Is.Not.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task GetWithVersion_returns_null_value_after_delete()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        var result = await grain.GetWithVersionAsync("k1");
        Assert.That(result.Value, Is.Null);
        Assert.That(result.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    // --- SetIfVersionAsync ---

    [Test]
    public async Task SetIfVersion_succeeds_when_version_matches()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var versioned = await grain.GetWithVersionAsync("k1");
        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), versioned.Version);

        Assert.That(result.Success, Is.True);
        Assert.That(result.CurrentVersion, Is.Not.EqualTo(versioned.Version));

        var readBack = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task SetIfVersion_fails_when_version_mismatches()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var staleVersion = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), staleVersion);

        Assert.That(result.Success, Is.False);
        Assert.That(result.Split, Is.Null);

        var readBack = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task SetIfVersion_succeeds_for_new_key_with_zero_version()
    {
        var grain = CreateGrain();
        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v1"), HybridLogicalClock.Zero);

        Assert.That(result.Success, Is.True);
        Assert.That(result.CurrentVersion, Is.Not.EqualTo(HybridLogicalClock.Zero));

        var readBack = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task SetIfVersion_fails_for_new_key_with_nonzero_version()
    {
        var grain = CreateGrain();
        var staleVersion = new HybridLogicalClock { WallClockTicks = 999, Counter = 0 };
        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v1"), staleVersion);

        Assert.That(result.Success, Is.False);
        Assert.That(result.CurrentVersion, Is.EqualTo(HybridLogicalClock.Zero));

        var readBack = await grain.GetAsync("k1");
        Assert.That(readBack, Is.Null);
    }

    [Test]
    public async Task SetIfVersion_succeeds_for_tombstoned_key_with_zero_version()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), HybridLogicalClock.Zero);

        Assert.That(result.Success, Is.True);
        var readBack = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task SetIfVersion_returns_current_version_on_failure()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var versioned = await grain.GetWithVersionAsync("k1");

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1-updated"));

        var result = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("v2"), versioned.Version);

        Assert.That(result.Success, Is.False);
        Assert.That(result.CurrentVersion, Is.Not.EqualTo(versioned.Version));
        Assert.That(result.CurrentVersion, Is.Not.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task SetIfVersion_concurrent_writers_only_one_succeeds()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v0"));

        var versioned = await grain.GetWithVersionAsync("k1");

        var result1 = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("writer1"), versioned.Version);
        Assert.That(result1.Success, Is.True);

        var result2 = await grain.SetIfVersionAsync("k1", Encoding.UTF8.GetBytes("writer2"), versioned.Version);
        Assert.That(result2.Success, Is.False);

        var readBack = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("writer1"));
    }
}