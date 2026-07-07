using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Wire-compatibility pins for <see cref="TreeRegistryEntry"/>. Orleans
/// deserialization decodes any missing <c>[Id(n)]</c> slot to its
/// <c>default</c> value, so legacy persisted registry rows from before
/// new slots are introduced must round-trip with the slot reading
/// <see langword="null"/>.
/// </summary>
public class TreeRegistryEntryTests
{
    [Test]
    public void Default_entry_has_null_WalPartitions_pin()
    {
        // Legacy decode contract: a registry row whose persisted state
        // pre-dates the per-tree WAL-partition pin slot must observe
        // null so the resolver falls back to the live
        // IOptionsMonitor<LatticeOptions> value. The next register
        // stamps the pin and subsequent resolves read from it.
        var entry = new TreeRegistryEntry();
        Assert.That(entry.WalPartitions, Is.Null);
    }

    [Test]
    public void WalPartitions_round_trips_through_record_with_expression()
    {
        var entry = new TreeRegistryEntry { WalPartitions = 16 };
        Assert.That(entry.WalPartitions, Is.EqualTo(16));

        var updated = entry with { WalPartitions = 32 };
        Assert.That(updated.WalPartitions, Is.EqualTo(32));
        // Original record is immutable; the with-expression returned a
        // fresh instance.
        Assert.That(entry.WalPartitions, Is.EqualTo(16));
    }

    [Test]
    public void Default_entry_has_null_RestoreShadowOfTreeId()
    {
        // Legacy decode contract: a registry row persisted before the
        // restore-shadow provenance slot must observe null so it is
        // classified as an ordinary tree, not a restore shadow.
        var entry = new TreeRegistryEntry();
        Assert.That(entry.RestoreShadowOfTreeId, Is.Null);
    }

    [Test]
    public void RestoreShadowOfTreeId_round_trips_through_record_with_expression()
    {
        var entry = new TreeRegistryEntry { RestoreShadowOfTreeId = "mfg-facts" };
        Assert.That(entry.RestoreShadowOfTreeId, Is.EqualTo("mfg-facts"));

        var cleared = entry with { RestoreShadowOfTreeId = null };
        Assert.That(cleared.RestoreShadowOfTreeId, Is.Null);
        Assert.That(entry.RestoreShadowOfTreeId, Is.EqualTo("mfg-facts"));
    }
}
