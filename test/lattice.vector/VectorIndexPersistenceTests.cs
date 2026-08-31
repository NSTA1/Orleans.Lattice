using System.Buffers.Binary;

namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the chunked, version-stamped persistence seam: snapshot
/// planning, chunk rendering, restore, partial restore, idempotent re-apply, and
/// format rejection.
/// </summary>
[TestFixture]
public sealed class VectorIndexPersistenceTests
{
    private const int Dimensions = 12;
    private const int Count = 1_200;
    private const int Partitions = 16;

    private static VectorIndexOptions Options(int partitionCount = Partitions, int probes = 6) => new()
    {
        Dimensions = Dimensions,
        PartitionCount = partitionCount,
        Probes = probes,
        MinimumTrainingCount = 16,
        TrainingSampleSize = 1_024,
    };

    private static VectorIndex Build(out float[][] corpus, bool train = true)
    {
        corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: Partitions, seed: 31);
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

    private static List<byte[]> RenderChunks(VectorIndexSnapshot snapshot)
    {
        var chunks = new List<byte[]>(snapshot.ChunkCount);
        for (var i = 0; i < snapshot.ChunkCount; i++)
        {
            var buffer = new byte[snapshot.MeasureChunk(i)];
            var written = snapshot.WriteChunk(i, buffer);
            Assert.That(written, Is.EqualTo(buffer.Length));
            chunks.Add(buffer);
        }

        return chunks;
    }

    [Test]
    public void CreateSnapshot_rejects_a_non_positive_chunk_size()
    {
        var index = Build(out _);

        Assert.Throws<ArgumentOutOfRangeException>(() => index.CreateSnapshot(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => index.CreateSnapshot(-1));
    }

    [Test]
    public void A_snapshot_header_describes_the_index_it_was_taken_from()
    {
        var index = Build(out _);

        var header = index.CreateSnapshot(128).Header;

        Assert.That(header.FormatVersion, Is.EqualTo(VectorIndexFormat.Version));
        Assert.That(header.Dimensions, Is.EqualTo(Dimensions));
        Assert.That(header.Metric, Is.EqualTo(VectorDistanceMetric.Cosine));
        Assert.That(header.PartitionCount, Is.EqualTo(Partitions));
        Assert.That(header.Probes, Is.EqualTo(6));
        Assert.That(header.Seed, Is.EqualTo(index.Seed));
        Assert.That(header.Count, Is.EqualTo(Count));
        Assert.That(header.IndexVersion, Is.EqualTo(index.Version));
        Assert.That(header.CentroidChunkCount, Is.EqualTo(1));
        Assert.That(header.ChunkCount, Is.GreaterThan(header.CentroidChunkCount));
    }

    [Test]
    public void No_chunk_exceeds_the_requested_item_bound()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(32);

        for (var i = 0; i < snapshot.ChunkCount; i++)
        {
            Assert.That(snapshot.Describe(i).ItemCount, Is.LessThanOrEqualTo(32));
        }
    }

    [Test]
    public void Centroid_chunks_come_first_so_a_reader_can_rank_before_it_fetches()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(4);

        for (var i = 0; i < snapshot.Header.CentroidChunkCount; i++)
        {
            Assert.That(snapshot.Describe(i).Kind, Is.EqualTo(VectorIndexChunkKind.Centroids));
        }

        for (var i = snapshot.Header.CentroidChunkCount; i < snapshot.ChunkCount; i++)
        {
            Assert.That(snapshot.Describe(i).Kind, Is.EqualTo(VectorIndexChunkKind.Vectors));
        }
    }

    [Test]
    public void The_chunks_of_a_snapshot_cover_every_vector_exactly_once()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(64);

        var vectors = 0;
        var centroids = 0;
        for (var i = 0; i < snapshot.ChunkCount; i++)
        {
            var descriptor = snapshot.Describe(i);
            if (descriptor.Kind == VectorIndexChunkKind.Vectors)
            {
                vectors += descriptor.ItemCount;
            }
            else
            {
                centroids += descriptor.ItemCount;
            }
        }

        Assert.That(vectors, Is.EqualTo(Count));
        Assert.That(centroids, Is.EqualTo(Partitions));
    }

    [Test]
    public void Describe_rejects_a_chunk_index_out_of_range()
    {
        var snapshot = Build(out _).CreateSnapshot(64);

        Assert.Throws<ArgumentOutOfRangeException>(() => snapshot.Describe(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => snapshot.Describe(snapshot.ChunkCount));
        Assert.Throws<ArgumentOutOfRangeException>(() => snapshot.MeasureChunk(snapshot.ChunkCount));
        Assert.Throws<ArgumentOutOfRangeException>(() => snapshot.WriteChunk(snapshot.ChunkCount, new byte[8]));
    }

    [Test]
    public void WriteChunk_rejects_a_destination_that_is_too_small()
    {
        var snapshot = Build(out _).CreateSnapshot(64);

        Assert.Throws<ArgumentException>(() => snapshot.WriteChunk(0, new byte[4]));
    }

    [Test]
    public void WriteChunk_refuses_to_render_a_torn_snapshot()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(64);

        index.Remove(0);

        var thrown = Assert.Throws<InvalidOperationException>(
            () => snapshot.WriteChunk(0, new byte[snapshot.MeasureChunk(0)]));

        Assert.That(thrown!.Message, Does.Contain("moved from version"));
    }

    [Test]
    public void A_restored_index_answers_identically_to_the_one_it_came_from()
    {
        var index = Build(out var corpus);
        var snapshot = index.CreateSnapshot(97);
        var header = snapshot.Header;
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(header, Options());
        foreach (var chunk in chunks)
        {
            restored.ApplyChunk(chunk);
        }

        Assert.That(restored.Count, Is.EqualTo(index.Count));
        Assert.That(restored.PartitionCount, Is.EqualTo(index.PartitionCount));
        Assert.That(restored.Probes, Is.EqualTo(index.Probes));
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Ready));

        var original = new VectorSearchResult[10];
        var replica = new VectorSearchResult[10];
        for (var q = 0; q < 30; q++)
        {
            var foundOriginal = index.Search(corpus[q], original, out var originalMode);
            var foundReplica = restored.Search(corpus[q], replica, out var replicaMode);

            Assert.That(replicaMode, Is.EqualTo(originalMode));
            Assert.That(foundReplica, Is.EqualTo(foundOriginal));
            Assert.That(replica, Is.EqualTo(original));
        }
    }

    [Test]
    public void A_restored_index_preserves_partition_membership_exactly()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(97);
        var restored = VectorIndex.Restore(snapshot.Header, Options());
        foreach (var chunk in RenderChunks(snapshot))
        {
            restored.ApplyChunk(chunk);
        }

        for (var p = 0; p < index.PartitionCount; p++)
        {
            Assert.That(restored.PartitionSize(p), Is.EqualTo(index.PartitionSize(p)));
        }
    }

    [Test]
    public void Chunks_may_be_applied_in_any_order()
    {
        var index = Build(out var corpus);
        var snapshot = index.CreateSnapshot(53);
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        for (var i = chunks.Count - 1; i >= 0; i--)
        {
            restored.ApplyChunk(chunks[i]);
        }

        Assert.That(restored.Count, Is.EqualTo(index.Count));
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Ready));

        var original = new VectorSearchResult[10];
        var replica = new VectorSearchResult[10];
        index.Search(corpus[5], original);
        restored.Search(corpus[5], replica);
        Assert.That(replica, Is.EqualTo(original));
    }

    [Test]
    public void Re_applying_a_chunk_is_idempotent_so_a_restore_can_resume()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(53);
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        foreach (var chunk in chunks)
        {
            restored.ApplyChunk(chunk);
            restored.ApplyChunk(chunk);
        }

        Assert.That(restored.Count, Is.EqualTo(index.Count));
        for (var p = 0; p < index.PartitionCount; p++)
        {
            Assert.That(restored.PartitionSize(p), Is.EqualTo(index.PartitionSize(p)));
        }
    }

    [Test]
    public void A_partially_restored_index_reports_that_it_is_still_building()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(4);
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(snapshot.Header, Options(partitionCount: Partitions));

        Assert.That(restored.CentroidsComplete, Is.False);
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Building));
        Assert.That(restored.IsReady, Is.False);

        for (var i = 0; i < snapshot.Header.CentroidChunkCount; i++)
        {
            restored.ApplyChunk(chunks[i]);
        }

        Assert.That(restored.CentroidsComplete, Is.True);
    }

    [Test]
    public void Centroids_alone_are_enough_to_rank_partitions_and_page_a_query_in_lazily()
    {
        var index = Build(out var corpus);
        var snapshot = index.CreateSnapshot(4);
        var chunks = RenderChunks(snapshot);

        var lazy = VectorIndex.Restore(snapshot.Header, Options());
        for (var i = 0; i < snapshot.Header.CentroidChunkCount; i++)
        {
            lazy.ApplyChunk(chunks[i]);
        }

        // With only the centroids loaded the index can already say which posting
        // lists a query needs, which is the whole point of the seam.
        var wanted = new int[index.Probes];
        var selected = lazy.SelectPartitions(corpus[9], wanted);
        Assert.That(selected, Is.EqualTo(index.Probes));

        var needed = new HashSet<int>(wanted[..selected]);
        var loaded = 0;
        for (var i = snapshot.Header.CentroidChunkCount; i < snapshot.ChunkCount; i++)
        {
            if (needed.Contains(snapshot.Describe(i).PartitionId))
            {
                lazy.ApplyChunk(chunks[i]);
                loaded++;
            }
        }

        Assert.That(loaded, Is.GreaterThan(0));
        Assert.That(loaded, Is.LessThan(snapshot.ChunkCount - snapshot.Header.CentroidChunkCount),
            "A lazy load that fetched every chunk would not have saved anything.");
        Assert.That(lazy.Count, Is.LessThan(index.Count));

        var full = new VectorSearchResult[10];
        var partial = new VectorSearchResult[10];
        var foundFull = index.Search(corpus[9], full);
        var foundPartial = lazy.Search(corpus[9], partial);

        Assert.That(foundPartial, Is.EqualTo(foundFull));
        Assert.That(partial, Is.EqualTo(full),
            "Probing the selected partitions must give the same answer whether the other partitions are resident or not.");
    }

    [Test]
    public void An_untrained_index_round_trips_through_unassigned_vector_chunks()
    {
        var index = Build(out var corpus, train: false);
        Assert.That(index.PartitionCount, Is.EqualTo(0));

        var snapshot = index.CreateSnapshot(100);
        Assert.That(snapshot.Header.CentroidChunkCount, Is.EqualTo(0));
        for (var i = 0; i < snapshot.ChunkCount; i++)
        {
            Assert.That(snapshot.Describe(i).PartitionId, Is.EqualTo(-1));
        }

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        foreach (var chunk in RenderChunks(snapshot))
        {
            restored.ApplyChunk(chunk);
        }

        Assert.That(restored.Count, Is.EqualTo(Count));
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Building));

        var original = new VectorSearchResult[10];
        var replica = new VectorSearchResult[10];
        index.Search(corpus[1], original);
        restored.Search(corpus[1], replica);
        Assert.That(replica, Is.EqualTo(original));
    }

    [Test]
    public void A_restored_index_can_be_retrained_from_its_restored_contents()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(97);
        var restored = VectorIndex.Restore(snapshot.Header, Options());
        foreach (var chunk in RenderChunks(snapshot))
        {
            restored.ApplyChunk(chunk);
        }

        Assert.That(restored.Train(), Is.True);
        Assert.That(restored.Count, Is.EqualTo(Count));

        var total = 0;
        for (var p = 0; p < restored.PartitionCount; p++)
        {
            total += restored.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(Count));
    }

    [Test]
    public void Restore_rejects_null_options()
    {
        var header = Build(out _).CreateSnapshot(64).Header;

        Assert.Throws<ArgumentNullException>(() => VectorIndex.Restore(header, null!));
    }

    [Test]
    public void Restore_rejects_a_format_version_it_does_not_understand()
    {
        var header = Build(out _).CreateSnapshot(64).Header with { FormatVersion = VectorIndexFormat.Version + 1 };

        var thrown = Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, Options()));

        Assert.That(thrown!.Message, Does.Contain("not supported by this build"));
    }

    [Test]
    public void Restore_rejects_a_header_that_contradicts_the_options_on_dimensionality()
    {
        var header = Build(out _).CreateSnapshot(64).Header;
        var mismatched = Options();
        mismatched.Dimensions = Dimensions + 1;

        Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, mismatched));
    }

    [Test]
    public void Restore_rejects_a_header_that_contradicts_the_options_on_metric()
    {
        var header = Build(out _).CreateSnapshot(64).Header;
        var mismatched = Options();
        mismatched.Metric = VectorDistanceMetric.DotProduct;

        Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, mismatched));
    }

    [Test]
    public void Restore_rejects_a_header_declaring_partitions_with_no_centroid_chunks()
    {
        var header = Build(out _).CreateSnapshot(64).Header with { CentroidChunkCount = 0 };

        Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, Options()));
    }

    [Test]
    public void Restore_rejects_a_header_declaring_centroid_chunks_with_no_partitions()
    {
        var header = Build(out _).CreateSnapshot(64).Header with { PartitionCount = 0, CentroidChunkCount = 1 };

        Assert.Throws<VectorIndexFormatException>(() => VectorIndex.Restore(header, Options()));
    }

    [Test]
    public void ApplyChunk_rejects_a_truncated_chunk()
    {
        var snapshot = Build(out _).CreateSnapshot(64);
        var restored = VectorIndex.Restore(snapshot.Header, Options());

        Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(new byte[8]));
    }

    [Test]
    public void ApplyChunk_rejects_bytes_that_are_not_a_chunk()
    {
        var snapshot = Build(out _).CreateSnapshot(64);
        var restored = VectorIndex.Restore(snapshot.Header, Options());

        Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(new byte[64]));
    }

    [Test]
    public void ApplyChunk_rejects_a_chunk_whose_format_version_is_unsupported()
    {
        var snapshot = Build(out _).CreateSnapshot(64);
        var chunk = new byte[snapshot.MeasureChunk(0)];
        snapshot.WriteChunk(0, chunk);
        chunk[4] = 99;

        var restored = VectorIndex.Restore(snapshot.Header, Options());

        var thrown = Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(chunk));
        Assert.That(thrown!.Message, Does.Contain("chunk format version"));
    }

    [Test]
    public void ApplyChunk_rejects_a_chunk_kind_it_does_not_know()
    {
        var snapshot = Build(out _).CreateSnapshot(64);
        var chunk = new byte[snapshot.MeasureChunk(0)];
        snapshot.WriteChunk(0, chunk);
        chunk[8] = 77;

        var restored = VectorIndex.Restore(snapshot.Header, Options());

        Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(chunk));
    }

    [Test]
    public void ApplyChunk_rejects_a_vector_chunk_naming_a_partition_the_index_does_not_have()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(64);
        var vectorChunkIndex = snapshot.Header.CentroidChunkCount;
        var chunk = new byte[snapshot.MeasureChunk(vectorChunkIndex)];
        snapshot.WriteChunk(vectorChunkIndex, chunk);
        chunk[12] = 200;

        var restored = VectorIndex.Restore(snapshot.Header, Options());

        Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(chunk));
    }

    [Test]
    public void ApplyChunk_rejects_a_centroid_chunk_when_the_index_was_restored_untrained()
    {
        var trained = Build(out _);
        var trainedSnapshot = trained.CreateSnapshot(64);
        var centroidChunk = new byte[trainedSnapshot.MeasureChunk(0)];
        trainedSnapshot.WriteChunk(0, centroidChunk);

        var untrained = Build(out _, train: false);
        var untrainedSnapshot = untrained.CreateSnapshot(64);
        var restored = VectorIndex.Restore(untrainedSnapshot.Header, Options());

        Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(centroidChunk));
    }

    [Test]
    public void ApplyChunk_reports_what_it_applied()
    {
        var snapshot = Build(out _).CreateSnapshot(64);
        var chunk = new byte[snapshot.MeasureChunk(0)];
        snapshot.WriteChunk(0, chunk);

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        var applied = restored.ApplyChunk(chunk);

        Assert.That(applied, Is.EqualTo(snapshot.Describe(0)));
    }

    [Test]
    public void A_vector_written_while_a_restore_is_streaming_is_findable_once_it_completes()
    {
        // Regression: with the centroid block only partly filled there is no
        // honest nearest cell, so a write placed by centroid affinity would land
        // arbitrarily and become unreachable the moment the index went Ready.
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(4);
        var chunks = RenderChunks(snapshot);
        var centroidChunks = snapshot.Header.CentroidChunkCount;
        Assert.That(centroidChunks, Is.GreaterThan(1), "This test needs the centroids split over several chunks.");

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        for (var i = centroidChunks; i < chunks.Count; i++)
        {
            restored.ApplyChunk(chunks[i]);
        }

        for (var i = 0; i < centroidChunks - 1; i++)
        {
            restored.ApplyChunk(chunks[i]);
        }

        Assert.That(restored.CentroidsComplete, Is.False);
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Building));

        var written = VectorCorpus.Clustered(200, Dimensions, clusters: Partitions, seed: 555);
        for (var i = 0; i < written.Length; i++)
        {
            restored.Add(500_000 + i, written[i]);
        }

        restored.ApplyChunk(chunks[centroidChunks - 1]);

        Assert.That(restored.CentroidsComplete, Is.True);
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Ready));
        Assert.That(restored.Count, Is.EqualTo(Count + written.Length));

        var total = 0;
        for (var p = 0; p < restored.PartitionCount; p++)
        {
            total += restored.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(restored.Count));

        var results = new VectorSearchResult[1];
        var missed = 0;
        for (var i = 0; i < written.Length; i++)
        {
            var found = restored.Search(written[i], results, out var mode);
            Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
            if (found == 0 || results[0].Key != 500_000 + i)
            {
                missed++;
            }
        }

        Assert.That(missed, Is.EqualTo(0),
            $"{missed} of {written.Length} vectors written during the restore window could not find themselves once "
            + "the index went Ready, so they were placed against an incomplete centroid block.");
    }

    [Test]
    public void A_vector_written_while_a_restore_is_streaming_is_findable_even_if_it_never_completes()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(4);
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        restored.ApplyChunk(chunks[0]);

        var vector = VectorCorpus.Clustered(1, Dimensions, clusters: Partitions, seed: 556)[0];
        restored.Add(900_001, vector);

        Assert.That(restored.CentroidsComplete, Is.False);

        var results = new VectorSearchResult[1];
        var found = restored.Search(vector, results, out var mode);

        Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
        Assert.That(found, Is.EqualTo(1));
        Assert.That(results[0].Key, Is.EqualTo(900_001));
    }

    [Test]
    public void ApplyChunk_rejects_a_centroid_chunk_that_carries_no_centroids()
    {
        // Regression: an empty centroid chunk would tick the readiness counter
        // without filling any centroid, so enough of them could promote an index
        // to Ready over an all-zero centroid block.
        var snapshot = Build(out _).CreateSnapshot(4);
        var restored = VectorIndex.Restore(snapshot.Header, Options());

        var empty = new byte[VectorIndexFormat.ChunkHeaderSize];
        WriteChunkHeader(empty, VectorIndexChunkKind.Centroids, partitionId: Partitions, sequence: 0, itemCount: 0);

        Assert.Throws<VectorIndexFormatException>(() => restored.ApplyChunk(empty));
        Assert.That(restored.CentroidsComplete, Is.False);
    }

    [Test]
    public void Re_applying_one_centroid_chunk_never_completes_a_partitioning_it_does_not_cover()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(4);
        var chunks = RenderChunks(snapshot);
        var centroidChunks = snapshot.Header.CentroidChunkCount;
        Assert.That(centroidChunks, Is.GreaterThan(1));

        var restored = VectorIndex.Restore(snapshot.Header, Options());
        for (var i = 0; i < centroidChunks; i++)
        {
            restored.ApplyChunk(chunks[0]);
        }

        Assert.That(restored.CentroidsComplete, Is.False,
            "Readiness must track the partitions actually covered, not how many chunks arrived.");
        Assert.That(restored.State, Is.EqualTo(VectorIndexState.Building));
    }

    [Test]
    public void A_chunk_size_larger_than_the_corpus_still_plans_chunks_covering_every_vector()
    {
        var index = Build(out _, train: false);

        foreach (var chunkSize in new[] { Count, Count + 1, int.MaxValue - 1, int.MaxValue })
        {
            var snapshot = index.CreateSnapshot(chunkSize);
            var vectors = 0;
            for (var i = 0; i < snapshot.ChunkCount; i++)
            {
                vectors += snapshot.Describe(i).ItemCount;
            }

            Assert.That(vectors, Is.EqualTo(Count),
                $"A chunk size of {chunkSize} planned {vectors} vectors for a corpus of {Count}.");
            Assert.That(snapshot.Header.Count, Is.EqualTo(Count));
        }
    }

    private static void WriteChunkHeader(
        Span<byte> destination, VectorIndexChunkKind kind, int partitionId, int sequence, int itemCount)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], VectorIndexFormat.ChunkMagic);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(4, 4), VectorIndexFormat.Version);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), (int)kind);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12, 4), partitionId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), sequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), itemCount);
    }
}
