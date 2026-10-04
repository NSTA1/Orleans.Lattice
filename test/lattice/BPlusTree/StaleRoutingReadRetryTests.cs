using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="StaleRoutingReadRetry"/>, the pacing and budget of
/// the read paths' stale-routing retry loops (issue #4545).
/// </summary>
[TestFixture]
public sealed class StaleRoutingReadRetryTests
{
    private static readonly TimeSpan Ceiling = TimeSpan.FromSeconds(60);

    [Test]
    public void Backoff_runs_the_first_retries_immediately()
    {
        for (var retry = 0; retry < StaleRoutingReadRetry.ImmediateRetries; retry++)
        {
            Assert.That(StaleRoutingReadRetry.Backoff(retry), Is.EqualTo(TimeSpan.Zero), $"retry {retry}");
        }
    }

    [Test]
    public void Backoff_doubles_from_the_initial_delay_up_to_the_ceiling()
    {
        var first = StaleRoutingReadRetry.ImmediateRetries;
        Assert.Multiple(() =>
        {
            Assert.That(StaleRoutingReadRetry.Backoff(first), Is.EqualTo(StaleRoutingReadRetry.InitialBackoff));
            Assert.That(StaleRoutingReadRetry.Backoff(first + 1), Is.EqualTo(StaleRoutingReadRetry.InitialBackoff * 2));
            Assert.That(StaleRoutingReadRetry.Backoff(first + 3), Is.EqualTo(StaleRoutingReadRetry.InitialBackoff * 8));
            Assert.That(StaleRoutingReadRetry.Backoff(first + 10), Is.EqualTo(StaleRoutingReadRetry.MaxBackoff));
            Assert.That(StaleRoutingReadRetry.Backoff(int.MaxValue), Is.EqualTo(StaleRoutingReadRetry.MaxBackoff),
                "a long retry run must not overflow the shift");
        });
    }

    [Test]
    public void Backoff_never_decreases_and_never_exceeds_the_ceiling()
    {
        var previous = TimeSpan.Zero;
        for (var retry = 0; retry < 64; retry++)
        {
            var delay = StaleRoutingReadRetry.Backoff(retry);
            Assert.That(delay, Is.GreaterThanOrEqualTo(previous), $"retry {retry}");
            Assert.That(delay, Is.LessThanOrEqualTo(StaleRoutingReadRetry.MaxBackoff), $"retry {retry}");
            previous = delay;
        }
    }

    [Test]
    public void Budget_is_five_sixths_of_the_response_timeout_so_the_typed_fault_arrives_first()
    {
        Assert.Multiple(() =>
        {
            Assert.That(StaleRoutingReadRetry.Budget(TimeSpan.FromSeconds(30), Ceiling), Is.EqualTo(TimeSpan.FromSeconds(25)));
            Assert.That(StaleRoutingReadRetry.Budget(TimeSpan.FromSeconds(6), Ceiling), Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(StaleRoutingReadRetry.Budget(TimeSpan.FromSeconds(30), Ceiling), Is.LessThan(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public void Budget_keeps_the_ceiling_for_a_long_infinite_or_unset_response_timeout()
    {
        Assert.Multiple(() =>
        {
            Assert.That(StaleRoutingReadRetry.Budget(TimeSpan.FromMinutes(30), Ceiling), Is.EqualTo(Ceiling));
            Assert.That(StaleRoutingReadRetry.Budget(Timeout.InfiniteTimeSpan, Ceiling), Is.EqualTo(Ceiling));
            Assert.That(StaleRoutingReadRetry.Budget(TimeSpan.Zero, Ceiling), Is.EqualTo(Ceiling));
        });
    }
}
