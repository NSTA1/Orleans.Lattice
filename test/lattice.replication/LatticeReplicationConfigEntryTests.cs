using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeReplicationConfigEntry"/> - the composite
/// CRDT record stored per target tree in the <c>sys-replication-config</c>
/// OR-Map. Verifies the field composition (disable-wins enablement, multi-value
/// merge mode), the <see cref="ICrdt{TSelf}"/> contract (merge / bottom / clone),
/// mode encode/decode, and that the record round-trips as an
/// <see cref="OrMap{TKey, TValue}"/> value.
/// </summary>
[TestFixture]
public class LatticeReplicationConfigEntryTests
{
    [Test]
    public void New_entry_is_bottom_and_not_enabled_with_no_mode()
    {
        var entry = new LatticeReplicationConfigEntry();

        Assert.Multiple(() =>
        {
            Assert.That(entry.IsBottom, Is.True);
            Assert.That(entry.IsEnabled, Is.False);
            Assert.That(entry.HasAmbiguousMode, Is.False);
            Assert.That(entry.Modes, Is.Empty);
            Assert.That(entry.TryGetMode(out _), Is.False);
        });
    }

    [Test]
    public void Enable_marks_entry_enabled_and_not_bottom()
    {
        var entry = new LatticeReplicationConfigEntry();

        entry.Enable("site-a", 1);

        Assert.Multiple(() =>
        {
            Assert.That(entry.IsEnabled, Is.True);
            Assert.That(entry.IsBottom, Is.False);
        });
    }

    [Test]
    public void SetMode_records_single_unambiguous_mode()
    {
        var entry = new LatticeReplicationConfigEntry();

        entry.SetMode("site-a", LatticeMergeMode.OrSet);

        Assert.Multiple(() =>
        {
            Assert.That(entry.HasAmbiguousMode, Is.False);
            Assert.That(entry.TryGetMode(out var mode), Is.True);
            Assert.That(mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(entry.Modes, Is.EqualTo(new[] { LatticeMergeMode.OrSet }));
        });
    }

    [Test]
    public void SetMode_twice_on_same_replica_supersedes_earlier_mode()
    {
        var entry = new LatticeReplicationConfigEntry();

        entry.SetMode("site-a", LatticeMergeMode.OrSet);
        entry.SetMode("site-a", LatticeMergeMode.PnCounter);

        Assert.Multiple(() =>
        {
            Assert.That(entry.HasAmbiguousMode, Is.False);
            Assert.That(entry.TryGetMode(out var mode), Is.True);
            Assert.That(mode, Is.EqualTo(LatticeMergeMode.PnCounter));
        });
    }

    [Test]
    public void MergeFrom_concurrent_divergent_modes_both_survive_and_are_ambiguous()
    {
        // Two clusters concurrently assign different modes (neither observed the
        // other): the multi-value register must keep both so the ambiguity is
        // detectable rather than silently dropping one.
        var a = new LatticeReplicationConfigEntry();
        a.SetMode("site-a", LatticeMergeMode.LwwRegister);

        var b = new LatticeReplicationConfigEntry();
        b.SetMode("site-b", LatticeMergeMode.OrSet);

        a.MergeFrom(b);

        Assert.Multiple(() =>
        {
            Assert.That(a.HasAmbiguousMode, Is.True);
            Assert.That(a.TryGetMode(out _), Is.False);
            Assert.That(a.Modes, Has.Count.EqualTo(2));
            Assert.That(a.Modes, Does.Contain(LatticeMergeMode.LwwRegister));
            Assert.That(a.Modes, Does.Contain(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public void MergeFrom_disable_wins_over_concurrent_enable()
    {
        // site-a enables; site-b concurrently disables without observing the
        // enable. The RwFlag disable-wins bias must leave the flag off.
        var enabled = new LatticeReplicationConfigEntry();
        enabled.Enable("site-a", 1);

        var disabled = new LatticeReplicationConfigEntry();
        disabled.Disable("site-b", 1);

        enabled.MergeFrom(disabled);

        Assert.That(enabled.IsEnabled, Is.False);
    }

    [Test]
    public void MergeFrom_is_commutative_for_enablement_and_mode()
    {
        var a = new LatticeReplicationConfigEntry();
        a.Enable("site-a", 1);
        a.SetMode("site-a", LatticeMergeMode.OrSet);

        var b = new LatticeReplicationConfigEntry();
        b.Enable("site-b", 1);
        b.SetMode("site-b", LatticeMergeMode.OrSet);

        var ab = a.Clone();
        ab.MergeFrom(b);
        var ba = b.Clone();
        ba.MergeFrom(a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.IsEnabled, Is.EqualTo(ba.IsEnabled));
            Assert.That(ab.Modes, Is.EquivalentTo(ba.Modes));
        });
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        ICrdt<LatticeReplicationConfigEntry> entry = new LatticeReplicationConfigEntry();

        Assert.That(() => entry.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Disabled_but_mode_set_entry_is_not_bottom()
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("site-a", 1);
        entry.SetMode("site-a", LatticeMergeMode.OrSet);
        entry.Disable("site-a", 2);

        Assert.Multiple(() =>
        {
            Assert.That(entry.IsEnabled, Is.False);
            // The mode register still carries a live value, so the OR-Map must
            // retain the slot rather than dropping the tree.
            Assert.That(entry.IsBottom, Is.False);
        });
    }

    [Test]
    public void Clone_is_independent_of_the_source()
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("site-a", 1);
        entry.SetMode("site-a", LatticeMergeMode.OrSet);

        var clone = entry.Clone();
        // Mutate the original after cloning.
        entry.Disable("site-a", 2);
        entry.SetMode("site-a", LatticeMergeMode.PnCounter);

        Assert.Multiple(() =>
        {
            Assert.That(clone.IsEnabled, Is.True);
            Assert.That(clone.TryGetMode(out var mode), Is.True);
            Assert.That(mode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public void EncodeMode_and_DecodeMode_round_trip_every_mode()
    {
        foreach (LatticeMergeMode mode in Enum.GetValues<LatticeMergeMode>())
        {
            var encoded = LatticeReplicationConfigEntry.EncodeMode(mode);

            Assert.Multiple(() =>
            {
                Assert.That(encoded, Has.Length.EqualTo(1));
                Assert.That(LatticeReplicationConfigEntry.DecodeMode(encoded), Is.EqualTo(mode));
            });
        }
    }

    [Test]
    public void DecodeMode_throws_on_null()
    {
        Assert.That(() => LatticeReplicationConfigEntry.DecodeMode(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeMode_throws_on_empty()
    {
        Assert.That(
            () => LatticeReplicationConfigEntry.DecodeMode(Array.Empty<byte>()),
            Throws.ArgumentException);
    }

    [Test]
    public void OrMap_value_round_trips_through_json_serializer()
    {
        // Exercises the exact (de)serialisation lane the OR-Map shape descriptor
        // uses (JsonLatticeSerializer<OrMap<string, LatticeReplicationConfigEntry>>).
        var map = new OrMap<string, LatticeReplicationConfigEntry>();
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("site-a", 1);
        entry.SetMode("site-a", LatticeMergeMode.OrSet);
        map.Set("orders", "site-a", entry);

        var serializer = JsonLatticeSerializer<OrMap<string, LatticeReplicationConfigEntry>>.Default;
        var round = serializer.Deserialize(serializer.Serialize(map));

        var restored = round.Get("orders");
        Assert.Multiple(() =>
        {
            Assert.That(restored, Is.Not.Null);
            Assert.That(restored!.IsEnabled, Is.True);
            Assert.That(restored.TryGetMode(out var mode), Is.True);
            Assert.That(mode, Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }

    [Test]
    public void OrMap_merge_recurses_into_config_entry_value()
    {
        // Two OR-Maps carry the same key written concurrently on different
        // replicas with divergent modes; the map merge must fold the per-key
        // values through LatticeReplicationConfigEntry.MergeFrom, preserving the
        // ambiguity rather than collapsing to one value.
        var left = new OrMap<string, LatticeReplicationConfigEntry>();
        var leftEntry = new LatticeReplicationConfigEntry();
        leftEntry.Enable("site-a", 1);
        leftEntry.SetMode("site-a", LatticeMergeMode.LwwRegister);
        left.Set("orders", "site-a", leftEntry);

        var right = new OrMap<string, LatticeReplicationConfigEntry>();
        var rightEntry = new LatticeReplicationConfigEntry();
        rightEntry.Enable("site-b", 1);
        rightEntry.SetMode("site-b", LatticeMergeMode.OrSet);
        right.Set("orders", "site-b", rightEntry);

        left.MergeFrom(right);

        var merged = left.Get("orders");
        Assert.Multiple(() =>
        {
            Assert.That(merged, Is.Not.Null);
            Assert.That(merged!.IsEnabled, Is.True);
            Assert.That(merged.HasAmbiguousMode, Is.True);
            Assert.That(merged.Modes, Has.Count.EqualTo(2));
        });
    }
}
