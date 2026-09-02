using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexManifestTests
{
    private static VectorIndexHeader Header(int partitions = 4, int count = 100) => new(
        VectorIndexFormat.Version,
        Dimensions: 8,
        VectorDistanceMetric.Cosine,
        PartitionCount: partitions,
        Probes: 2,
        Seed: 1234,
        Count: count,
        ChunkCount: 9,
        CentroidChunkCount: 1,
        IndexVersion: 77);

    private static VectorIndexManifest Manifest(int partitions = 4, int count = 100) =>
        new(Generation: 3, CentroidEpoch: 11, IndexedCount: count, Header(partitions, count));

    [Test]
    public void A_manifest_round_trips_through_a_record()
    {
        var manifest = Manifest();

        Assert.That(VectorIndexManifest.TryReadRecord(manifest.ToRecord(), out var read), Is.True);
        Assert.That(read, Is.EqualTo(manifest));
    }

    [Test]
    public void Write_reports_exactly_the_declared_size()
    {
        var buffer = new byte[VectorIndexManifest.Size + 4];

        Assert.That(Manifest().Write(buffer), Is.EqualTo(VectorIndexManifest.Size));
    }

    [Test]
    public void A_short_buffer_is_refused()
    {
        Assert.That(() => Manifest().Write(new byte[4]), Throws.ArgumentException);
    }

    [Test]
    public void A_corrupt_manifest_is_refused_rather_than_partially_believed()
    {
        var record = Manifest().ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 2] ^= 0xFF;

        Assert.That(VectorIndexManifest.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_truncated_manifest_is_refused()
    {
        var record = Manifest().ToRecord();

        Assert.That(VectorIndexManifest.TryReadRecord(record.AsSpan(0, record.Length - 1), out _), Is.False);
    }

    [Test]
    public void A_payload_of_the_wrong_length_is_refused()
    {
        Assert.That(VectorIndexManifest.TryReadRecord(VectorIndexRecord.Wrap([1, 2, 3]), out _), Is.False);
    }

    [Test]
    public void A_manifest_carrying_a_snapshot_version_this_build_cannot_read_is_refused()
    {
        var manifest = Manifest() with
        {
            Header = Header() with { FormatVersion = VectorIndexFormat.Version + 1 },
        };

        Assert.That(VectorIndexManifest.TryReadRecord(manifest.ToRecord(), out _), Is.False,
            "An unreadable snapshot version must be a rebuild branch, not a decoded manifest.");
    }

    [Test]
    public void A_manifest_whose_two_partition_counts_disagree_is_refused()
    {
        // The partition count is written twice: once by the manifest and once
        // inside the snapshot header. A record assembled from two generations
        // passes its checksum but fails this cross-check.
        var record = Manifest().ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 20] = 9;
        VectorIndexRecord.Seal(record, VectorIndexManifest.Size);

        Assert.That(VectorIndexManifest.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_manifest_whose_count_disagrees_with_its_header_is_refused()
    {
        var record = Manifest().ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 16] = 99;
        VectorIndexRecord.Seal(record, VectorIndexManifest.Size);

        Assert.That(VectorIndexManifest.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void A_manifest_declaring_a_negative_generation_is_refused()
    {
        var record = Manifest().ToRecord();
        record[VectorIndexPersistenceFormat.RecordHeaderSize + 7] = 0x80;
        VectorIndexRecord.Seal(record, VectorIndexManifest.Size);

        Assert.That(VectorIndexManifest.TryReadRecord(record, out _), Is.False);
    }

    [Test]
    public void An_untrained_manifest_round_trips()
    {
        var manifest = new VectorIndexManifest(
            Generation: 0,
            CentroidEpoch: 0,
            IndexedCount: 42,
            new VectorIndexHeader(
                VectorIndexFormat.Version, 8, VectorDistanceMetric.DotProduct, 0, 0, 5, 42, 3, 0, 12));

        Assert.That(VectorIndexManifest.TryReadRecord(manifest.ToRecord(), out var read), Is.True);
        Assert.That(read, Is.EqualTo(manifest));
    }
}
