using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplogShardStateTests
{
    [Test]
    public void New_instance_has_empty_entries_list()
    {
        var state = new ReplogShardState();
        Assert.That(state.Entries, Is.Empty);
    }

    [Test]
    public void Entries_is_settable_for_state_round_trip()
    {
        var state = new ReplogShardState
        {
            Entries = new List<ReplogShardEntry> { new() { Sequence = 0 } },
        };

        Assert.That(state.Entries, Has.Count.EqualTo(1));
    }
}
