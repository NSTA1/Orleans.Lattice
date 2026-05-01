using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationMaintenanceStateTests
{
    [Test]
    public void New_instance_has_zero_last_gc_ticks()
    {
        var state = new ReplicationMaintenanceState();
        Assert.That(state.LastGcTicks, Is.EqualTo(0L));
    }

    [Test]
    public void New_instance_has_zero_last_fall_off_check_ticks()
    {
        var state = new ReplicationMaintenanceState();
        Assert.That(state.LastFallOffCheckTicks, Is.EqualTo(0L));
    }

    [Test]
    public void LastGcTicks_is_settable()
    {
        var ticks = DateTime.UtcNow.Ticks;
        var state = new ReplicationMaintenanceState { LastGcTicks = ticks };
        Assert.That(state.LastGcTicks, Is.EqualTo(ticks));
    }

    [Test]
    public void LastFallOffCheckTicks_is_settable()
    {
        var ticks = DateTime.UtcNow.Ticks;
        var state = new ReplicationMaintenanceState { LastFallOffCheckTicks = ticks };
        Assert.That(state.LastFallOffCheckTicks, Is.EqualTo(ticks));
    }
}
