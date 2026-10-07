using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The WAL clock floor end to end against real grains (issue #4586): a stamp
/// the leaf cannot renew - an idempotency key, a range delete's issue stamp -
/// meets a partition floor raised past it.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalClockFloorIntegrationTests
{
    private WalClockFloorClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new WalClockFloorClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static HybridLogicalClock Ago(TimeSpan age) =>
        new() { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks - age.Ticks, Counter = 0 };

    [Test]
    public async Task An_idempotency_key_older_than_the_floor_is_refused_as_expired_and_nothing_is_written()
    {
        var tree = $"floor-idem-stale-{Guid.NewGuid():N}";
        var router = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await router.SetAsync("warm", Utf8("w"));
        var floor = await _fixture.RaiseFloorsAsync(tree);
        var stale = new LatticeIdempotencyKey { Timestamp = Ago(TimeSpan.FromSeconds(30)) };
        Assert.That(stale.Timestamp, Is.LessThan(floor), "precondition: the key is older than the floor");

        LatticeIdempotencyKeyExpiredException? expired;
        using (LatticeIdempotencyContext.With(stale))
        {
            expired = Assert.ThrowsAsync<LatticeIdempotencyKeyExpiredException>(async () => await router.SetAsync("k", Utf8("v")));
        }

        var value = await router.GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(expired!.KeyTimestamp, Is.EqualTo(stale.Timestamp));
            Assert.That(expired.Floor, Is.GreaterThan(stale.Timestamp));
            Assert.That(value, Is.Null, "a refused idempotent write is not applied");
        });
    }

    [Test]
    public async Task An_idempotency_key_retried_inside_the_floor_lag_commits_once()
    {
        var tree = $"floor-idem-fresh-{Guid.NewGuid():N}";
        var router = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await router.SetAsync("warm", Utf8("w"));
        var key = LatticeIdempotencyKey.Fresh();
        await _fixture.RaiseFloorsAsync(tree);

        using (LatticeIdempotencyContext.With(key))
        {
            await router.SetAsync("k", Utf8("v"));
            await router.SetAsync("k", Utf8("v"));
        }

        Assert.That(await router.GetAsync("k"), Is.EqualTo(Utf8("v")));
    }

    [Test]
    public async Task A_range_delete_refused_mid_fan_out_reissues_a_fresh_stamp_and_deletes_every_key_once()
    {
        var tree = $"floor-range-{Guid.NewGuid():N}";
        var router = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        const int keys = 40;
        for (var i = 0; i < keys; i++)
        {
            await router.SetAsync($"k{i:D3}", Utf8("v"));
        }

        FloorRefusalInjectingCommitLogWriter.ArmRangeDeleteRefusals(tree, 1);
        var deleted = await router.DeleteRangeAsync("k", "l");

        var stamps = FloorRefusalInjectingCommitLogWriter.RangeDeleteStampsFor(tree);
        var remaining = 0;
        for (var i = 0; i < keys; i++)
        {
            if (await router.GetAsync($"k{i:D3}") is not null)
            {
                remaining++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.EqualTo(keys), "every key is counted exactly once across the re-issue");
            Assert.That(remaining, Is.Zero, "the re-issued walk finishes the remainder");
            Assert.That(stamps.Distinct().Count(), Is.GreaterThanOrEqualTo(2), "the refusal re-issued a second stamp");
            Assert.That(stamps[^1], Is.GreaterThan(stamps[0]), "the re-issued stamp dominates the refused one");
        });
    }

    [Test]
    public async Task A_nested_range_delete_keeps_its_owners_stamp_and_fails_typed()
    {
        var tree = $"floor-range-nested-{Guid.NewGuid():N}";
        var router = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await router.SetAsync("k1", Utf8("v"));
        FloorRefusalInjectingCommitLogWriter.ArmRangeDeleteRefusals(tree, 1);
        var key = LatticeIdempotencyKey.Fresh();

        using (LatticeIdempotencyContext.With(key))
        {
            Assert.ThrowsAsync<LatticeIdempotencyKeyExpiredException>(async () => await router.DeleteRangeAsync("k", "l"));
        }

        Assert.That(FloorRefusalInjectingCommitLogWriter.RangeDeleteStampsFor(tree).Distinct().Single(), Is.EqualTo(key.Timestamp),
            "an idempotency-keyed range delete is never re-issued under another stamp");
    }
}
