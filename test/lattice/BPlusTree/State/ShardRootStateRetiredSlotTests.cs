using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Issue #4700 retired <c>ShardRootState</c>'s shard-wide <c>LeafClearsBegun</c>
/// flag (<c>[Id(25)]</c>). A shard root persisted by an older silo still carries it,
/// with the flag set if that silo's purge was interrupted, and the current build must
/// read that state cleanly: skipping the retired slot without misaligning the fields
/// either side of it.
/// </summary>
[TestFixture]
public sealed class ShardRootStateRetiredSlotTests
{
    [Test]
    public void State_persisted_with_the_retired_LeafClearsBegun_slot_reads_cleanly_and_ignores_it()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var bytes = serializer.SerializeToArray(new LegacyHolder
        {
            State = new LegacyShardRootState { IsDeleted = true, IsPurged = false, BootstrapReadFenced = true, LeafClearsBegun = true },
        });

        var current = serializer.Deserialize<CurrentHolder>(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(current.State, Is.Not.Null);
            Assert.That(current.State!.IsDeleted, Is.True);
            Assert.That(current.State.IsPurged, Is.False);
            Assert.That(current.State.BootstrapReadFenced, Is.True, "the field before the retired slot survives");
            Assert.That(current.State.PurgeClearedLeafRecords, Is.Null, "the retired flag is not read into the new slot");
            Assert.That(current.Tail, Is.EqualTo("after-the-state"), "skipping the retired slot must not misalign later fields");
        });
    }

    /// <summary>Carries a pre-#4700 shard root, followed by a field after it.</summary>
    [GenerateSerializer]
    internal sealed class LegacyHolder
    {
        [Id(0)] public LegacyShardRootState? State { get; set; }
        [Id(1)] public string Tail { get; set; } = "after-the-state";
    }

    /// <summary>The same holder as the current build reads it.</summary>
    [GenerateSerializer]
    internal sealed class CurrentHolder
    {
        [Id(0)] public ShardRootState? State { get; set; }
        [Id(1)] public string Tail { get; set; } = "";
    }

    /// <summary>The slots of <see cref="ShardRootState"/> around the retired one, as a pre-#4700 silo wrote them.</summary>
    [GenerateSerializer]
    internal sealed class LegacyShardRootState
    {
        [Id(6)] public bool IsDeleted { get; set; }
        [Id(23)] public bool IsPurged { get; set; }
        [Id(24)] public bool BootstrapReadFenced { get; set; }
        [Id(25)] public bool LeafClearsBegun { get; set; }
    }
}
