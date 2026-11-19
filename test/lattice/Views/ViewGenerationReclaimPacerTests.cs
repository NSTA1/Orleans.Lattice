using Orleans.Lattice.Testing;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Coverage for <see cref="ViewGenerationReclaimPacer"/>, the silo-wide
/// admission that keeps superseded-generation reclamation gradual (issue #6097).
/// </summary>
[TestFixture]
public class ViewGenerationReclaimPacerTests
{
    [Test]
    public void TryAdmit_admits_once_per_interval()
    {
        var clock = new ManualTimeProvider();
        var pacer = new ViewGenerationReclaimPacer { TimeProvider = clock, Interval = TimeSpan.FromSeconds(2) };

        Assert.Multiple(() =>
        {
            Assert.That(pacer.TryAdmit(), Is.True, "the first pass is admitted at once");
            Assert.That(pacer.TryAdmit(), Is.False);
        });

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.That(pacer.TryAdmit(), Is.False, "half an interval later");

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Multiple(() =>
        {
            Assert.That(pacer.TryAdmit(), Is.True);
            Assert.That(pacer.TryAdmit(), Is.False);
        });
    }

    [Test]
    public void TryAdmit_admits_exactly_one_of_many_concurrent_callers()
    {
        var pacer = new ViewGenerationReclaimPacer { TimeProvider = new ManualTimeProvider() };
        var admitted = 0;

        Parallel.For(0, 64, _ =>
        {
            if (pacer.TryAdmit())
            {
                Interlocked.Increment(ref admitted);
            }
        });

        Assert.That(admitted, Is.EqualTo(1));
    }
}
