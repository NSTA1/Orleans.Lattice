using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSagaDecisionDeadlineContext"/>, the
/// ambient decide-by deadline a routing grain stamps around an atomic-write
/// saga call.
/// </summary>
[TestFixture]
public class LatticeSagaDecisionDeadlineContextTests
{
    [TearDown]
    public void ClearAmbient() => RequestContext.Clear();

    [Test]
    public void DeadlineFor_subtracts_a_tenth_of_the_response_timeout()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var deadline = LatticeSagaDecisionDeadlineContext.DeadlineFor(TimeSpan.FromSeconds(30), now);

        Assert.That(new DateTime(deadline, DateTimeKind.Utc), Is.EqualTo(now.AddSeconds(27)));
    }

    [Test]
    public void DeadlineFor_keeps_at_least_a_one_second_margin()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var deadline = LatticeSagaDecisionDeadlineContext.DeadlineFor(TimeSpan.FromSeconds(5), now);

        Assert.That(new DateTime(deadline, DateTimeKind.Utc), Is.EqualTo(now.AddSeconds(4)));
    }

    [Test]
    public void DeadlineFor_caps_the_margin_at_half_a_short_timeout()
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var deadline = LatticeSagaDecisionDeadlineContext.DeadlineFor(TimeSpan.FromSeconds(1), now);

        Assert.That(new DateTime(deadline, DateTimeKind.Utc), Is.EqualTo(now.AddMilliseconds(500)));
    }

    [Test]
    public void DeadlineFor_imposes_none_without_a_finite_timeout()
    {
        var now = DateTime.UtcNow;

        Assert.Multiple(() =>
        {
            Assert.That(LatticeSagaDecisionDeadlineContext.DeadlineFor(Timeout.InfiniteTimeSpan, now), Is.EqualTo(0));
            Assert.That(LatticeSagaDecisionDeadlineContext.DeadlineFor(TimeSpan.Zero, now), Is.EqualTo(0));
        });
    }

    [Test]
    public void With_stamps_the_deadline_and_restores_the_previous_one()
    {
        using (LatticeSagaDecisionDeadlineContext.With(100))
        {
            using (LatticeSagaDecisionDeadlineContext.With(200))
            {
                Assert.That(LatticeSagaDecisionDeadlineContext.Current, Is.EqualTo(200));
            }

            Assert.That(LatticeSagaDecisionDeadlineContext.Current, Is.EqualTo(100));
        }

        Assert.That(LatticeSagaDecisionDeadlineContext.Current, Is.Null);
    }

    [Test]
    public void Current_treats_a_non_positive_deadline_as_none()
    {
        LatticeSagaDecisionDeadlineContext.Current = 100;
        LatticeSagaDecisionDeadlineContext.Current = 0;

        Assert.That(LatticeSagaDecisionDeadlineContext.Current, Is.Null);
    }
}
