using System.Buffers.Binary;

namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// The arms of <see cref="VectorIndex.ApplyChunk"/> and
/// <see cref="VectorIndex.Restore"/> that refuse a chunk or header no snapshot
/// this build wrote could contain.
/// </summary>
/// <remarks>
/// <para>
/// These guards form one family - the same "this is not a snapshot I wrote"
/// decision, copied into each decode step - and the family was tested
/// unevenly. Chunk-level rejection (truncated, wrong marker, wrong version,
/// unknown kind) had tests; the per-field range checks inside the two chunk
/// appliers, and the two guards that read a field the header's own validation
/// does not bound, had none. A reader generalising from the covered members
/// would conclude the whole family was pinned, so deleting any of the
/// uncovered ones left the suite green.
/// </para>
/// <para>
/// Every input here is a chunk the index must reject rather than decode. That
/// matters beyond coverage: a chunk is untrusted durable input, and each of
/// these fields indexes into an array. Accepting one would be an out-of-range
/// read or a silently wrong index, not merely a bad error message.
/// </para>
/// </remarks>
[TestFixture]
public sealed class VectorIndexChunkDecodeTests
{
    private const int Dimensions = 12;
    private const int Count = 240;
    private const int Partitions = 16;

    private static VectorIndexOptions Options() => new()
    {
        Dimensions = Dimensions,
        PartitionCount = Partitions,
        Probes = 4,
        MinimumTrainingCount = 16,
        TrainingSampleSize = 512,
    };

    private static VectorIndex Build(out float[][] corpus, bool train = true)
    {
        corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: Partitions, seed: 23);
        var index = new VectorIndex(Options());
        index.EnsureCapacity(Count);
        for (var i = 0; i < Count; i++)
        {
            index.Add(i, corpus[i]);
        }

        if (train)
        {
            index.Train();
        }

        return index;
    }

    private static VectorIndex Restored(out VectorIndexSnapshot snapshot)
    {
        snapshot = Build(out _).CreateSnapshot(4);
        return VectorIndex.Restore(snapshot.Header, Options());
    }

    /// <summary>
    /// A chunk carrying only its fixed preamble, so every test states the field
    /// it is perturbing and nothing else.
    /// </summary>
    private static byte[] Chunk(
        VectorIndexChunkKind kind, int partitionId, int sequence, int itemCount, int payloadBytes = 0)
    {
        var chunk = new byte[VectorIndexFormat.ChunkHeaderSize + payloadBytes];
        var span = chunk.AsSpan();
        BinaryPrimitives.WriteUInt32LittleEndian(span[..4], VectorIndexFormat.ChunkMagic);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4, 4), VectorIndexFormat.Version);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(8, 4), (int)kind);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(12, 4), partitionId);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(16, 4), sequence);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(20, 4), itemCount);
        return chunk;
    }

    [Test]
    public void ApplyChunk_rejects_a_chunk_declaring_a_negative_item_count()
    {
        var restored = Restored(out _);

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(Chunk(VectorIndexChunkKind.Vectors, partitionId: 0, sequence: 0, itemCount: -1)));

        Assert.That(thrown!.Message, Does.Contain("negative item count or sequence"));
    }

    [Test]
    public void ApplyChunk_rejects_a_chunk_declaring_a_negative_sequence()
    {
        // The second disjunct of the same guard. Tested separately because a
        // compound condition proved through one of its terms leaves the other
        // free to be deleted.
        var restored = Restored(out _);

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(Chunk(VectorIndexChunkKind.Vectors, partitionId: 0, sequence: -1, itemCount: 1)));

        Assert.That(thrown!.Message, Does.Contain("negative item count or sequence"));
    }

    [Test]
    public void ApplyChunk_rejects_a_negative_count_before_it_dispatches_on_kind()
    {
        // Ordering is the point: the range check must run before the switch, so
        // an undefined kind cannot shadow it and a defined one cannot act on the
        // negative field. Driving both kinds proves the guard sits above the
        // dispatch rather than being duplicated inside one applier.
        var restored = Restored(out _);

        Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(Chunk(VectorIndexChunkKind.Centroids, partitionId: 0, sequence: 0, itemCount: -1)));
        Assert.That(restored.Count, Is.Zero);
        Assert.That(restored.CentroidsComplete, Is.False);
    }

    [Test]
    public void Restore_rejects_a_header_whose_centroid_block_cannot_be_allocated()
    {
        // PartitionCount is durable, untrusted, and - unlike Dimensions and
        // Metric - is not checked against the options. The header reader only
        // refuses it when negative, so the allocation guard in PrepareForRestore
        // is the single thing standing between a corrupt header and a request for
        // a block the runtime cannot produce.
        var header = Build(out _).CreateSnapshot(4).Header with
        {
            PartitionCount = int.MaxValue,
            CentroidChunkCount = 1,
        };

        var thrown = Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, Options()));

        Assert.That(thrown!.Message, Does.Contain("centroid block"));
        Assert.That(thrown.Message, Does.Contain("largest array"));
    }

    [Test]
    public void Restore_reports_an_unallocatable_centroid_block_as_bad_data_not_as_a_fault()
    {
        // The exception type carries the operational decision. A restore is a
        // derived projection, so "this header is not one I wrote" must present as
        // a format failure the caller answers by rebuilding - not as an
        // InvalidOperationException, which reads as a bug in the caller and is
        // what the same guard throws on the training path where the value is the
        // caller's own.
        var header = Build(out _).CreateSnapshot(4).Header with
        {
            PartitionCount = int.MaxValue,
            CentroidChunkCount = 1,
        };

        Assert.That(
            Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, Options())),
            Is.Not.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ApplyChunk_rejects_a_centroid_chunk_that_runs_past_the_last_partition()
    {
        var restored = Restored(out _);

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Centroids, partitionId: Partitions - 1, sequence: 0, itemCount: 4)));

        Assert.That(thrown!.Message, Does.Contain($"the index has {Partitions}"));
        Assert.That(restored.CentroidsComplete, Is.False);
    }

    [Test]
    public void ApplyChunk_rejects_a_centroid_chunk_naming_a_negative_first_partition()
    {
        // The other disjunct of the range guard, and the one that would index
        // backwards out of the centroid block rather than past its end.
        var restored = Restored(out _);

        Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Centroids, partitionId: -1, sequence: 0, itemCount: 1)));

        Assert.That(restored.CentroidsComplete, Is.False);
    }

    [Test]
    public void ApplyChunk_rejects_a_centroid_chunk_whose_sequence_exceeds_the_headers_count()
    {
        var restored = Restored(out var snapshot);
        var declared = snapshot.Header.CentroidChunkCount;
        Assert.That(declared, Is.GreaterThan(0));

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Centroids, partitionId: 0, sequence: declared, itemCount: 1)));

        Assert.That(thrown!.Message, Does.Contain($"only {declared} centroid chunks"));
    }

    [Test]
    public void ApplyChunk_rejects_an_unassigned_vector_chunk_when_the_index_is_partitioned()
    {
        // Partition -1 is the legal way an untrained snapshot names its single
        // cell, so this is not a malformed field - it is a well-formed chunk from
        // the wrong snapshot. Applying it would file partitioned vectors into
        // cell 0, where no query that probes by centroid affinity would find them.
        var restored = Restored(out _);

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Vectors, partitionId: -1, sequence: 0, itemCount: 1)));

        Assert.That(thrown!.Message, Does.Contain("unassigned vectors"));
        Assert.That(restored.Count, Is.Zero);
    }

    [Test]
    public void ApplyChunk_rejects_a_centroid_chunk_whose_payload_is_short()
    {
        var restored = Restored(out _);

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Centroids, partitionId: 0, sequence: 0, itemCount: 2)));

        Assert.That(thrown!.Message, Does.Contain("centroid chunk declares a payload"));
    }

    [Test]
    public void ApplyChunk_rejects_a_vector_chunk_whose_payload_is_short()
    {
        // The same helper reached through the other applier, which computes a
        // different expected length (a key plus a vector per record, not a vector
        // alone). Both call sites matter because the length arithmetic differs.
        var restored = Restored(out _);

        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Vectors, partitionId: 0, sequence: 0, itemCount: 2)));

        Assert.That(thrown!.Message, Does.Contain("vector chunk declares a payload"));
    }

    [Test]
    public void ApplyChunk_rejects_a_payload_one_byte_short_of_the_declared_length()
    {
        // The boundary, not merely an empty payload: the guard is `actual <
        // expected`, so an off-by-one that relaxed it to `<=` or dropped the
        // comparison entirely would still refuse a zero-length payload and only
        // start over-reading on a nearly-complete one.
        var restored = Restored(out _);
        var exact = Dimensions * sizeof(float);

        Assert.Throws<VectorIndexFormatException>(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Centroids, 0, 0, itemCount: 1, payloadBytes: exact - 1)));

        Assert.DoesNotThrow(
            () => restored.ApplyChunk(
                Chunk(VectorIndexChunkKind.Centroids, 0, 0, itemCount: 1, payloadBytes: exact)));
    }

    [Test]
    public void A_vector_deleted_while_a_restore_streams_is_not_resurrected_when_it_completes()
    {
        // A write taken while the centroid block is incomplete is parked in cell
        // 0 and remembered, so it can be re-placed once the partitioning arrives.
        // Deleting it in that window leaves the key remembered but no longer
        // stored, and the re-placement pass has to skip it. Without that skip the
        // pass reads a location for a key that has none.
        var index = Build(out var corpus);
        var snapshot = index.CreateSnapshot(4);
        var chunks = new List<byte[]>(snapshot.ChunkCount);
        for (var i = 0; i < snapshot.ChunkCount; i++)
        {
            var buffer = new byte[snapshot.MeasureChunk(i)];
            snapshot.WriteChunk(i, buffer);
            chunks.Add(buffer);
        }

        var centroidChunks = snapshot.Header.CentroidChunkCount;
        Assert.That(centroidChunks, Is.GreaterThan(1), "The test needs a window in which centroids are incomplete.");

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        restored.ApplyChunk(chunks[0]);
        Assert.That(restored.CentroidsComplete, Is.False);

        const long Parked = 900_001;
        const long Kept = 900_002;
        restored.Add(Parked, corpus[7]);
        restored.Add(Kept, corpus[9]);
        Assert.That(restored.Remove(Parked), Is.True);

        for (var i = 1; i < centroidChunks; i++)
        {
            restored.ApplyChunk(chunks[i]);
        }

        Assert.That(restored.CentroidsComplete, Is.True);
        Assert.That(restored.Contains(Parked), Is.False, "A key deleted in the window must not come back.");
        Assert.That(restored.Count, Is.EqualTo(1));

        // And the survivor really was re-placed rather than left parked: it is
        // findable by the approximate path, which only probes cells chosen by
        // centroid affinity.
        var results = new VectorSearchResult[4];
        var found = restored.Search(corpus[9], results, out var mode);
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
        Assert.That(found, Is.EqualTo(1));
        Assert.That(results[0].Key, Is.EqualTo(Kept));
    }

    [Test]
    public void A_restore_whose_every_parked_vector_was_deleted_still_completes()
    {
        // The degenerate shape of the same arm: the re-placement pass runs with a
        // non-empty remembered set and places nothing at all.
        var index = Build(out var corpus);
        var snapshot = index.CreateSnapshot(4);
        var chunks = new List<byte[]>(snapshot.ChunkCount);
        for (var i = 0; i < snapshot.ChunkCount; i++)
        {
            var buffer = new byte[snapshot.MeasureChunk(i)];
            snapshot.WriteChunk(i, buffer);
            chunks.Add(buffer);
        }

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        restored.ApplyChunk(chunks[0]);

        for (var i = 0; i < 4; i++)
        {
            restored.Add(800_000 + i, corpus[i]);
            Assert.That(restored.Remove(800_000 + i), Is.True);
        }

        for (var i = 1; i < snapshot.Header.CentroidChunkCount; i++)
        {
            restored.ApplyChunk(chunks[i]);
        }

        Assert.That(restored.CentroidsComplete, Is.True);
        Assert.That(restored.Count, Is.Zero);

        // Ready, not Empty: readiness is a property of the partitioning, and this
        // index has a complete one. Empty is reserved for an index that has
        // neither vectors nor a partitioning, so a restore that completed its
        // centroids is ready to place the next write even with nothing in it.
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Ready));

        // And it genuinely is: a write taken now is placed by centroid affinity
        // rather than parked, so the approximate path finds it.
        restored.Add(700_001, corpus[2]);
        var results = new VectorSearchResult[2];
        Assert.That(restored.Search(corpus[2], results, out var mode), Is.EqualTo(1));
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
        Assert.That(results[0].Key, Is.EqualTo(700_001));
    }
}
