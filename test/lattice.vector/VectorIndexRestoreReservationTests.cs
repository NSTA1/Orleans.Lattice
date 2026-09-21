namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the one allocation in <see cref="VectorIndex.Restore"/> that is
/// an optimisation rather than a requirement: the up-front capacity reservation.
/// </summary>
/// <remarks>
/// <para>
/// Issue #3130. A reservation the runtime declined used to propagate out of the
/// whole restore. The caller re-drove the phase on the next tick, re-read the same
/// durable header, computed the same capacity and failed at the same line - 44
/// consecutive times on the deployed container, after which the coordinator
/// reported that the phase machine had stopped advancing and every tick's work was
/// being discarded. Because <c>header.Count</c> is durable and the computation is
/// deterministic, nothing in that loop could ever break it.
/// </para>
/// <para>
/// <b>Why these tests cost nothing to run.</b> They drive the failure through the
/// cell-block guard rather than through real memory exhaustion: a header declaring
/// <see cref="int.MaxValue"/> vectors asks for a block larger than
/// <see cref="Array.MaxLength"/>, which the guard refuses <i>before</i> allocating
/// anything. That makes the same "cannot reserve that much" path deterministic,
/// instantaneous, and free of the flakiness an OOM-by-exhaustion test would have.
/// </para>
/// </remarks>
[TestFixture]
public sealed class VectorIndexRestoreReservationTests
{
    private const int Dimensions = 12;
    private const int Count = 240;
    private const int Partitions = 8;

    private static VectorIndexOptions Options(int partitionCount = Partitions) => new()
    {
        Dimensions = Dimensions,
        PartitionCount = partitionCount,
        Probes = 4,
        MinimumTrainingCount = 16,
        TrainingSampleSize = 256,
    };

    private static VectorIndex Build(out float[][] corpus, bool train = true)
    {
        corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: Partitions, seed: 17);
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
    public void Restore_tolerates_a_capacity_reservation_the_runtime_cannot_satisfy()
    {
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(64);

        // The one header field Restore does not validate, set beyond what any cell
        // block can hold. Before the fix this threw out of Restore entirely.
        var header = snapshot.Header with { Count = int.MaxValue };

        VectorIndex restored = null!;
        Assert.DoesNotThrow(() => restored = VectorIndex.Restore(header, Options()));
        Assert.That(restored, Is.Not.Null);
    }

    [Test]
    public void A_restore_that_could_not_reserve_still_rebuilds_the_index_exactly()
    {
        // The load-bearing test. Tolerating the failure is only correct if the
        // index that comes out is indistinguishable from one that reserved
        // successfully - otherwise the fix would trade a loud wedge for a quiet
        // wrong answer.
        var index = Build(out var corpus);
        var snapshot = index.CreateSnapshot(64);
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(snapshot.Header with { Count = int.MaxValue }, Options());
        foreach (var chunk in chunks)
        {
            restored.ApplyChunk(chunk);
        }

        Assert.That(restored.Count, Is.EqualTo(index.Count));
        Assert.That(restored.State, Is.EqualTo(index.State));
        Assert.That(restored.PartitionCount, Is.EqualTo(index.PartitionCount));

        for (var p = 0; p < index.PartitionCount; p++)
        {
            Assert.That(restored.PartitionSize(p), Is.EqualTo(index.PartitionSize(p)));
        }

        var original = new VectorSearchResult[8];
        var replica = new VectorSearchResult[8];
        for (var q = 0; q < Count; q += 17)
        {
            var foundOriginal = index.Search(corpus[q], original, out var originalMode);
            var foundReplica = restored.Search(corpus[q], replica, out var replicaMode);

            Assert.That(replicaMode, Is.EqualTo(originalMode));
            Assert.That(foundReplica, Is.EqualTo(foundOriginal));
            Assert.That(replica, Is.EqualTo(original));
        }
    }

    [Test]
    public void An_untrained_restore_tolerates_the_same_reservation_failure()
    {
        // The untrained shape reserves into a single cell rather than spreading
        // over partitions, so it reaches the guard by a different arithmetic path
        // and is worth covering separately.
        var index = Build(out var corpus, train: false);
        var snapshot = index.CreateSnapshot(64);
        var chunks = RenderChunks(snapshot);

        var restored = VectorIndex.Restore(
            snapshot.Header with { Count = int.MaxValue }, Options(partitionCount: Partitions));
        foreach (var chunk in chunks)
        {
            restored.ApplyChunk(chunk);
        }

        Assert.That(restored.Count, Is.EqualTo(index.Count));

        var original = new VectorSearchResult[8];
        var replica = new VectorSearchResult[8];
        index.Search(corpus[3], original);
        restored.Search(corpus[3], replica);
        Assert.That(replica, Is.EqualTo(original));
    }

    [Test]
    public void Restore_still_rejects_a_header_it_cannot_interpret()
    {
        // Scope discipline: only the reservation is optional. A header whose shape
        // contradicts the options is still a hard failure, so the tolerance cannot
        // have been written wide enough to swallow a real one.
        var index = Build(out _);
        var snapshot = index.CreateSnapshot(64);

        Assert.Throws<VectorIndexFormatException>(
            () => VectorIndex.Restore(
                snapshot.Header with { Count = int.MaxValue, Dimensions = Dimensions + 1 },
                Options()));

        Assert.Throws<VectorIndexFormatException>(
            () => VectorIndex.Restore(
                snapshot.Header with { Count = int.MaxValue, PartitionCount = 4, CentroidChunkCount = 0 },
                Options()));
    }

    [Test]
    public void EnsureCapacity_still_throws_for_a_caller_that_asked_for_the_reservation()
    {
        // The tolerance belongs to the restore call site, not to the public
        // reservation API of a released package. A caller that reserves explicitly
        // must still be told the reservation did not happen.
        var index = Build(out _);
        Assert.Throws<InvalidOperationException>(() => index.EnsureCapacity(int.MaxValue));
    }

    [Test]
    public void EnsureCapacity_refuses_an_over_large_capacity_at_the_cell_guard()
    {
        // Regression for an overflow this fix's own test run exposed. The
        // per-cell share was computed as `capacity + _segmentCount - 1` in 32-bit
        // arithmetic, which wraps NEGATIVE for a capacity near int.MaxValue. Every
        // ReserveSegment call then took its `capacity <= current` early return, so
        // the request silently reserved NOTHING and surfaced much later as an
        // OutOfMemoryException from the location map instead.
        //
        // The exception type is the witness: reaching the cell-block guard proves
        // the division produced a positive share and the segments were actually
        // considered. Restoring the 32-bit arithmetic turns this back into an
        // OutOfMemoryException raised from Dictionary.Resize.
        var index = Build(out _);

        var thrown = Assert.Throws<InvalidOperationException>(
            () => index.EnsureCapacity(int.MaxValue));

        Assert.That(thrown!.Message, Does.Contain("cell block"));
        Assert.That(thrown, Is.Not.InstanceOf<OutOfMemoryException>());
    }

    [Test]
    public void A_reservation_that_fails_leaves_every_stored_vector_readable()
    {
        // The atomicity half. A reservation grows three parallel arrays per cell,
        // and publishing any of them before all three exist would leave the cell
        // torn - a vectors block sized for the new capacity beside norms and keys
        // still sized for the old one. That state was unreachable while the
        // exception destroyed the whole index; making the failure survivable is
        // what makes it reachable, so the two changes belong together.
        var index = Build(out var corpus);

        var before = new VectorSearchResult[8];
        index.Search(corpus[11], before);
        var countBefore = index.Count;

        Assert.Throws<InvalidOperationException>(() => index.EnsureCapacity(int.MaxValue));

        Assert.That(index.Count, Is.EqualTo(countBefore));
        for (var i = 0; i < Count; i++)
        {
            var destination = new float[Dimensions];
            Assert.That(index.Contains(i), Is.True);
            Assert.That(index.TryGetVector(i, destination), Is.True);
            Assert.That(destination, Is.EqualTo(corpus[i]));
        }

        var after = new VectorSearchResult[8];
        index.Search(corpus[11], after);
        Assert.That(after, Is.EqualTo(before));
    }

    [Test]
    public void A_reservation_that_succeeds_keeps_every_stored_vector_readable()
    {
        // The positive half of the same invariant: growing the cells must preserve
        // what they already held, which is what the copy in the rebuilt reservation
        // is responsible for.
        var index = Build(out var corpus);

        index.EnsureCapacity(Count * 4);

        for (var i = 0; i < Count; i++)
        {
            var destination = new float[Dimensions];
            Assert.That(index.TryGetVector(i, destination), Is.True);
            Assert.That(destination, Is.EqualTo(corpus[i]));
        }

        // And the grown index still accepts writes into the reserved room.
        var extra = VectorCorpus.Clustered(4, Dimensions, clusters: 2, seed: 91);
        for (var i = 0; i < extra.Length; i++)
        {
            index.Add(Count + i, extra[i]);
        }

        Assert.That(index.Count, Is.EqualTo(Count + extra.Length));
        for (var i = 0; i < extra.Length; i++)
        {
            var destination = new float[Dimensions];
            Assert.That(index.TryGetVector(Count + i, destination), Is.True);
            Assert.That(destination, Is.EqualTo(extra[i]));
        }
    }
}
