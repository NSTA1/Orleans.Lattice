using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Guards <see cref="DurableVectorIndexOptions.ResolveMaxLeafKeys(long)"/>, the
/// leaf key bound a tree holding durable vector-index records is registered with
/// (issue #2829).
/// <para>
/// A Lattice leaf splits on whichever of two bounds it crosses first: its key
/// count, or its byte size. The core default key bound of 128 is sized for small
/// values. A durable index's records are byte-bounded chunks of up to
/// <see cref="DurableVectorIndexOptions.MaxChunkBytes"/>, so at 128 keys a leaf of
/// full chunks holds about 8 MiB against a 64 MiB byte bound: the key bound fires
/// at an eighth of the admitted size, the byte bound is dead, and the tree holds
/// several times the leaves it needs. The derivation divides the byte bound by the
/// largest record so the two bounds cross together.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexLeafBoundTests
{
    /// <summary>The width a current embedding model produces, and the one issue #2829 was observed at.</summary>
    private const int WideDimensions = 768;

    /// <summary>
    /// How close to the byte bound a leaf of full-size records must come when it
    /// reaches the key bound. A full chunk is a little under the chunk ceiling,
    /// because the ceiling is not an exact multiple of an item's stride, so the
    /// two bounds cross at nearly, not exactly, the same leaf size.
    /// </summary>
    private const double MinimumByteBoundReach = 0.9;

    [Test]
    public void At_the_default_byte_bound_the_leaf_key_bound_is_1024()
        => Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes), Is.EqualTo(1024),
            "64 MiB of 64 KiB records is 1024 of them; the core default of 128 splits the leaf at about 8 MiB");

    [Test]
    public void The_bound_tracks_a_non_default_byte_bound()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(128L * 1024 * 1024), Is.EqualTo(2048));
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(16L * 1024 * 1024), Is.EqualTo(256));
            Assert.That(
                DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes),
                Is.EqualTo(LatticeOptions.DefaultMaxLeafBytes / DurableVectorIndexOptions.MaxChunkBytes),
                "the bound is derived from the live chunk ceiling, not a literal fitted to it");
        });
    }

    [Test]
    public void The_bound_tracks_a_non_default_record_size()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes, 32 * 1024), Is.EqualTo(2048));
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes, 4 * 1024), Is.EqualTo(16_384));
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes, 128 * 1024), Is.EqualTo(512));
        });
    }

    [Test]
    public async Task A_leaf_of_full_size_records_reaches_both_bounds_together()
    {
        // Measured from a real wide build rather than asserted from the constants,
        // so the arm stays honest if the record layout grows.
        var store = new InMemoryVectorIndexStore();
        await DurableIndexHarness.BuiltAsync(
            store,
            DurableIndexHarness.Source(1_600, dimensions: WideDimensions),
            DurableIndexHarness.Options(dimensions: WideDimensions, maxItemsPerChunk: 1_024, ingestBatchSize: 128));

        var keys = DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes);
        var leafAtKeyBound = (long)keys * store.LargestRecordBytes;

        Assert.Multiple(() =>
        {
            Assert.That(store.LargestRecordBytes, Is.GreaterThan(DurableVectorIndexOptions.MaxChunkBytes / 2),
                "the arm is vacuous unless the build wrote chunks near the ceiling");
            Assert.That(leafAtKeyBound, Is.GreaterThanOrEqualTo((long)(LatticeOptions.DefaultMaxLeafBytes * MinimumByteBoundReach)),
                $"a leaf of {keys} full records holds {leafAtKeyBound} bytes: the key bound must not fire far "
                + $"short of the {LatticeOptions.DefaultMaxLeafBytes}-byte bound, or the byte bound is dead");
            Assert.That((long)keys * DurableVectorIndexOptions.MaxChunkBytes, Is.LessThanOrEqualTo(LatticeOptions.DefaultMaxLeafBytes),
                "nor may the key bound admit more full-ceiling records than the byte bound holds");
        });
    }

    [Test]
    public void A_disabled_byte_bound_is_sized_against_the_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(0), Is.EqualTo(1024));
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(-1), Is.EqualTo(1024),
                "with the byte bound off, the key bound is the only bound a leaf has");
        });
    }

    [Test]
    public void The_bound_never_falls_below_two()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(1), Is.EqualTo(DurableVectorIndexOptions.MinMaxLeafKeys));
            Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(100, 65_536), Is.EqualTo(2),
                "a leaf holding one key has nothing to split");
        });
    }

    [Test]
    public void A_vast_byte_bound_is_clamped_to_an_int()
        => Assert.That(DurableVectorIndexOptions.ResolveMaxLeafKeys(long.MaxValue, 1), Is.EqualTo(int.MaxValue));

    [Test]
    public void A_non_positive_record_size_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes, 0),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes, -1),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }
}
