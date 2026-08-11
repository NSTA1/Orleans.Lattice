namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextValues"/>: the HLC-ordered last-writer-wins
/// scalar registers - authoring, reading, ordering, and merge convergence.
/// </summary>
[TestFixture]
public sealed class RepoContextValuesTests
{
    private static HybridLogicalClock Clock(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void Lww_string_round_trips_through_ReadString()
    {
        var register = RepoContextValues.Lww("hello", Clock(100));
        Assert.That(RepoContextValues.ReadString(register), Is.EqualTo("hello"));
    }

    [Test]
    public void Lww_integer_round_trips_through_ReadInt64()
    {
        var register = RepoContextValues.Lww(4096L, Clock(100));
        Assert.That(RepoContextValues.ReadInt64(register), Is.EqualTo(4096L));
    }

    [Test]
    public void ReadString_on_an_unwritten_register_is_null()
        => Assert.That(RepoContextValues.ReadString(new BoundedRegister()), Is.Null);

    [Test]
    public void ReadInt64_on_an_unwritten_register_is_null()
        => Assert.That(RepoContextValues.ReadInt64(new BoundedRegister()), Is.Null);

    [Test]
    public void HlcOrderKey_is_the_fixed_width()
        => Assert.That(RepoContextValues.HlcOrderKey(Clock(1)).Length,
            Is.EqualTo(RepoContextValues.HlcOrderKeyLength));

    [Test]
    public void The_later_hlc_wins_regardless_of_merge_order()
    {
        var earlier = RepoContextValues.Lww("old", Clock(100));
        var later = RepoContextValues.Lww("new", Clock(200));

        var forward = BoundedRegister.Merge(earlier, later);
        var backward = BoundedRegister.Merge(later, earlier);

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(forward), Is.EqualTo("new"));
            Assert.That(RepoContextValues.ReadString(backward), Is.EqualTo("new"));
        });
    }

    [Test]
    public void The_hlc_counter_breaks_a_wall_clock_tie()
    {
        var earlier = RepoContextValues.Lww("old", Clock(100, counter: 1));
        var later = RepoContextValues.Lww("new", Clock(100, counter: 2));

        var merged = BoundedRegister.Merge(earlier, later);
        Assert.That(RepoContextValues.ReadString(merged), Is.EqualTo("new"));
    }

    [Test]
    public void A_later_write_may_lower_the_value_because_ordering_is_by_hlc_not_value()
    {
        var high = RepoContextValues.Lww(999L, Clock(100));
        var lowerButLater = RepoContextValues.Lww(1L, Clock(200));

        var merged = BoundedRegister.Merge(high, lowerButLater);
        Assert.That(RepoContextValues.ReadInt64(merged), Is.EqualTo(1L));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var register = RepoContextValues.Lww("v", Clock(100));
        var merged = BoundedRegister.Merge(register, register);
        Assert.That(RepoContextValues.ReadString(merged), Is.EqualTo("v"));
    }

    [Test]
    public void Lww_rejects_a_null_string_value()
        => Assert.That(() => RepoContextValues.Lww(null!, Clock(1)), Throws.ArgumentNullException);
}
