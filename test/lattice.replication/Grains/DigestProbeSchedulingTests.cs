using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the pure digest-probe jitter helper.
/// </summary>
[TestFixture]
public class DigestProbeSchedulingTests
{
    [Test]
    public void ApplyJitter_returns_interval_unchanged_when_jitter_is_zero()
    {
        var interval = TimeSpan.FromMinutes(5);
        var result = DigestProbeScheduling.ApplyJitter(interval, 0.0, new Random(1));
        Assert.That(result, Is.EqualTo(interval));
    }

    [Test]
    public void ApplyJitter_returns_interval_unchanged_when_interval_is_zero()
    {
        var result = DigestProbeScheduling.ApplyJitter(TimeSpan.Zero, 0.5, new Random(1));
        Assert.That(result, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void ApplyJitter_stays_within_symmetric_band()
    {
        var interval = TimeSpan.FromMinutes(5);
        const double jitter = 0.2;
        var rng = new Random(12345);
        for (var i = 0; i < 1000; i++)
        {
            var result = DigestProbeScheduling.ApplyJitter(interval, jitter, rng);
            Assert.That(result.Ticks, Is.InRange(
                (long)(interval.Ticks * (1.0 - jitter)),
                (long)(interval.Ticks * (1.0 + jitter))));
        }
    }

    [Test]
    public void ApplyJitter_throws_when_random_is_null()
    {
        Assert.That(
            () => DigestProbeScheduling.ApplyJitter(TimeSpan.FromMinutes(5), 0.2, null!),
            Throws.ArgumentNullException);
    }
}
