using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="HistoryRetentionShaper"/>: the drain-time reshaping
/// of a maximal revision row per the active retention mode (value stripping and
/// age-bound expiry stamping).
/// </summary>
[TestFixture]
public sealed class HistoryRetentionShaperTests
{
    private static readonly long Now = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static HistoryRow SetRow(long wallTicks, byte[]? value) => new()
    {
        Timestamp = new HybridLogicalClock { WallClockTicks = wallTicks, Counter = 0 },
        Kind = HistoryRowKind.Set,
        SourceKey = "k",
        Value = value,
        ValueHash = 123,
        ValueLength = value?.Length ?? 0,
    };

    [Test]
    public void Shape_metadata_only_strips_value_but_keeps_fingerprint()
    {
        var row = SetRow(Now, new byte[] { 1, 2, 3 });
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.MetadataOnly, TimeSpan.Zero, TimeSpan.Zero);

        var (shaped, expiresAtTicks) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Value, Is.Null);
        Assert.That(shaped.ValueHash, Is.EqualTo(123));
        Assert.That(shaped.ValueLength, Is.EqualTo(3));
        Assert.That(shaped.RetentionShape, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        Assert.That(expiresAtTicks, Is.Zero);
    }

    [Test]
    public void Shape_full_value_keeps_value()
    {
        var row = SetRow(Now, new byte[] { 1, 2, 3 });
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.FullValue, TimeSpan.Zero, TimeSpan.Zero);

        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(shaped.RetentionShape, Is.EqualTo(HistoryRetentionMode.FullValue));
    }

    [Test]
    public void Shape_with_window_stamps_age_bound_expiry()
    {
        var row = SetRow(Now, new byte[] { 1 });
        var window = TimeSpan.FromHours(1);
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.FullValue, window, TimeSpan.Zero);

        var (_, expiresAtTicks) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(expiresAtTicks, Is.EqualTo(Now + window.Ticks));
    }

    [Test]
    public void Shape_saturates_an_overflowing_window_instead_of_wrapping_negative()
    {
        // The durable-history retention window is validated only as strictly
        // positive (no upper bound), so an extreme value can drive drainNowTicks
        // + Window.Ticks past long.MaxValue. It must saturate at
        // DateTime.MaxValue.Ticks, never wrap to a negative expiry that the next
        // TTL sweep would treat as already-expired and silently drop - the exact
        // opposite of the operator's retain-nearly-forever intent.
        var row = SetRow(Now, new byte[] { 1 });
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.FullValue, TimeSpan.MaxValue, TimeSpan.Zero);

        var (_, expiresAtTicks) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(expiresAtTicks, Is.EqualTo(DateTime.MaxValue.Ticks));
        Assert.That(expiresAtTicks, Is.GreaterThan(Now), "a huge window must not expire the row immediately");
    }

    [Test]
    public void Shape_hybrid_keeps_recent_value()
    {
        var row = SetRow(Now - TimeSpan.FromMinutes(1).Ticks, new byte[] { 5 });
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.Hybrid, TimeSpan.Zero, TimeSpan.FromMinutes(5));

        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Value, Is.EqualTo(new byte[] { 5 }));
        Assert.That(shaped.RetentionShape, Is.EqualTo(HistoryRetentionMode.Hybrid));
    }

    [Test]
    public void Shape_hybrid_strips_old_value()
    {
        var row = SetRow(Now - TimeSpan.FromMinutes(10).Ticks, new byte[] { 5 });
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.Hybrid, TimeSpan.Zero, TimeSpan.FromMinutes(5));

        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Value, Is.Null);
        Assert.That(shaped.ValueLength, Is.EqualTo(1), "metadata fingerprint survives even when bytes are stripped");
    }

    [Test]
    public void Shape_hybrid_with_nonpositive_window_degrades_to_metadata()
    {
        var row = SetRow(Now, new byte[] { 5 });
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.Hybrid, TimeSpan.Zero, TimeSpan.Zero);

        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Value, Is.Null);
    }

    [Test]
    public void Shape_crdt_delta_row_keeps_delta_regardless_of_mode()
    {
        var row = new HistoryRow
        {
            Timestamp = new HybridLogicalClock { WallClockTicks = Now, Counter = 0 },
            Kind = HistoryRowKind.CrdtDelta,
            SourceKey = "k",
            Delta = new byte[] { 7, 7 },
        };
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.MetadataOnly, TimeSpan.FromHours(2), TimeSpan.Zero);

        var (shaped, expiresAtTicks) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Delta, Is.EqualTo(new byte[] { 7, 7 }));
        Assert.That(shaped.RetentionShape, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        Assert.That(expiresAtTicks, Is.EqualTo(Now + TimeSpan.FromHours(2).Ticks));
    }

    [Test]
    public void Shape_range_tombstone_marker_keeps_bounds()
    {
        var row = new HistoryRow
        {
            Timestamp = new HybridLogicalClock { WallClockTicks = Now, Counter = 0 },
            Kind = HistoryRowKind.RangeTombstone,
            SourceKey = "a",
            EndKey = "m",
        };
        var policy = new HistoryRetentionPolicy(HistoryRetentionMode.FullValue, TimeSpan.Zero, TimeSpan.Zero);

        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, Now);

        Assert.That(shaped.Kind, Is.EqualTo(HistoryRowKind.RangeTombstone));
        Assert.That(shaped.SourceKey, Is.EqualTo("a"));
        Assert.That(shaped.EndKey, Is.EqualTo("m"));
    }
}
