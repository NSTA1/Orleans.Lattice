using NSubstitute;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="DurableAuthAuditTrailSink"/>: the disabled-by-default
/// zero-cost no-op (it must not touch the grain factory when the durable trail is
/// off) and the shape of the roughly-time-ordered, unique audit-row key. The
/// enabled write path (records + TTL) is exercised end-to-end by the
/// observability integration tests.
/// </summary>
[TestFixture]
public sealed class DurableAuthAuditTrailSinkTests
{
    private static LatticeAuthDecisionEvent Event() =>
        new("alice", LatticeOperation.Read, "app", LatticeEffect.Deny, 1, DateTimeOffset.UtcNow, key: "k");

    [Test]
    public async Task WriteAsync_with_the_durable_trail_disabled_never_touches_the_grain_factory()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var options = new StubOptionsMonitor<LatticeAuthOptions>(
            new LatticeAuthOptions { EnableDurableAuditTrail = false });
        var sink = new DurableAuthAuditTrailSink(grainFactory, options);

        await sink.WriteAsync(Event());

        Assert.That(grainFactory.ReceivedCalls(), Is.Empty,
            "the durable trail is opt-in: with it off the sink must do no work at all");
    }

    [Test]
    public void BuildKey_is_prefixed_by_the_zero_padded_utc_ticks()
    {
        var ts = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);

        var key = DurableAuthAuditTrailSink.BuildKey(ts);

        var prefix = key.Split(DurableAuthAuditTrailSink.KeySeparator)[0];
        Assert.That(prefix, Has.Length.EqualTo(19));
        Assert.That(prefix, Is.EqualTo(ts.UtcTicks.ToString("D19")));
    }

    [Test]
    public void BuildKey_is_unique_for_the_same_timestamp()
    {
        var ts = DateTimeOffset.UtcNow;

        var a = DurableAuthAuditTrailSink.BuildKey(ts);
        var b = DurableAuthAuditTrailSink.BuildKey(ts);

        Assert.That(a, Is.Not.EqualTo(b), "the GUID suffix must keep concurrent same-instant rows distinct");
    }

    [Test]
    public void BuildKey_orders_earlier_timestamps_before_later_ones()
    {
        var earlier = DurableAuthAuditTrailSink.BuildKey(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var later = DurableAuthAuditTrailSink.BuildKey(new DateTimeOffset(2026, 1, 1, 0, 0, 1, TimeSpan.Zero));

        Assert.That(string.CompareOrdinal(earlier, later), Is.LessThan(0),
            "the ticks prefix must sort earlier events before later ones");
    }

    private sealed class StubOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
