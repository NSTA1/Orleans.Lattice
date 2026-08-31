using System.Buffers;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Vector;

public sealed partial class VectorIndex
{
    // Probe scratch up to this many partitions is stack-allocated, so the
    // overwhelmingly common query allocates nothing anywhere - not even from a
    // pool. Beyond it the scratch is rented, which is allocation-free once the
    // pool is warm.
    private const int StackProbeLimit = 128;

    /// <summary>
    /// Writes the identifiers of the partitions this query would probe into
    /// <paramref name="destination"/>, best first, and returns how many were
    /// written (the lesser of the destination's length and
    /// <see cref="PartitionCount"/>).
    /// <para>
    /// This is the seam a durable consumer uses to page an index in lazily: rank
    /// the partitions from the centroids alone, fetch only those cells, and never
    /// touch the rest of the corpus. The ordering is total - by descending
    /// affinity with ascending partition identifier breaking ties - so two calls
    /// with the same query and the same trained state select the same partitions
    /// in the same order.
    /// </para>
    /// </summary>
    /// <param name="query">The query vector, of exactly <see cref="Dimensions"/> components.</param>
    /// <param name="destination">The span to write partition identifiers into.</param>
    /// <returns>The number of partition identifiers written. <c>0</c> when the index is untrained or its centroids are incomplete.</returns>
    /// <exception cref="ArgumentException"><paramref name="query"/> has the wrong length.</exception>
    public int SelectPartitions(ReadOnlySpan<float> query, Span<int> destination)
    {
        RequireDimensions(query.Length, nameof(query));
        if (_partitionCount == 0 || destination.IsEmpty || _missingCentroids > 0)
        {
            return 0;
        }

        var wanted = Math.Min(destination.Length, _partitionCount);
        var scale = ScaleFor(TensorPrimitives.Norm(query));

        float[]? rented = null;
        Span<float> affinity = wanted <= StackProbeLimit
            ? stackalloc float[StackProbeLimit]
            : (rented = ArrayPool<float>.Shared.Rent(wanted));
        try
        {
            return SelectPartitionsCore(query, scale, destination[..wanted], affinity[..wanted]);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<float>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Finds the best matches for a query, writing them into
    /// <paramref name="results"/> in descending score order and returning how many
    /// were written. The span's length <i>is</i> k, so a caller asking for more
    /// results than the index holds simply receives fewer.
    /// </summary>
    /// <param name="query">The query vector, of exactly <see cref="Dimensions"/> components. Every component must be finite.</param>
    /// <param name="results">The caller-owned span the ranked hits are written into.</param>
    /// <returns>The number of results written, never more than <paramref name="results"/>'s length.</returns>
    /// <exception cref="ArgumentException"><paramref name="query"/> has the wrong length.</exception>
    public int Search(ReadOnlySpan<float> query, Span<VectorSearchResult> results) =>
        Search(query, results, out _);

    /// <summary>
    /// Finds the best matches for a query and reports which retrieval path
    /// answered, so a consumer never presents an approximate answer as an exact
    /// one.
    /// </summary>
    /// <param name="query">The query vector, of exactly <see cref="Dimensions"/> components. Every component must be finite.</param>
    /// <param name="results">The caller-owned span the ranked hits are written into.</param>
    /// <param name="mode">
    /// Set to <see cref="VectorSearchMode.Approximate"/> when the trained
    /// partitioning answered, and <see cref="VectorSearchMode.Exhaustive"/> when
    /// every live vector was scored and the answer is therefore exact.
    /// </param>
    /// <returns>The number of results written, never more than <paramref name="results"/>'s length.</returns>
    /// <exception cref="ArgumentException"><paramref name="query"/> has the wrong length.</exception>
    public int Search(ReadOnlySpan<float> query, Span<VectorSearchResult> results, out VectorSearchMode mode)
    {
        RequireDimensions(query.Length, nameof(query));

        if (results.IsEmpty || _count == 0)
        {
            mode = VectorSearchMode.Exhaustive;
            return 0;
        }

        var queryNorm = _metric == VectorDistanceMetric.Cosine ? TensorPrimitives.Norm(query) : 1f;

        if (_partitionCount == 0 || _missingCentroids > 0)
        {
            mode = VectorSearchMode.Exhaustive;
            return SearchExhaustive(query, queryNorm, results);
        }

        mode = VectorSearchMode.Approximate;
        return SearchPartitions(query, queryNorm, results);
    }

    private int SearchExhaustive(ReadOnlySpan<float> query, float queryNorm, Span<VectorSearchResult> results)
    {
        var found = 0;
        for (var segment = 0; segment < _segmentCount; segment++)
        {
            ScoreSegment(segment, query, queryNorm, results, ref found);
        }

        return found;
    }

    private int SearchPartitions(ReadOnlySpan<float> query, float queryNorm, Span<VectorSearchResult> results)
    {
        var wanted = Math.Min(_probes, _partitionCount);
        var scale = _metric == VectorDistanceMetric.Cosine
            ? queryNorm == 0f ? 0f : 1f / queryNorm
            : 1f;

        int[]? rentedIds = null;
        float[]? rentedAffinity = null;
        Span<int> probeIds = wanted <= StackProbeLimit
            ? stackalloc int[StackProbeLimit]
            : (rentedIds = ArrayPool<int>.Shared.Rent(wanted));
        Span<float> affinity = wanted <= StackProbeLimit
            ? stackalloc float[StackProbeLimit]
            : (rentedAffinity = ArrayPool<float>.Shared.Rent(wanted));

        try
        {
            var selected = SelectPartitionsCore(query, scale, probeIds[..wanted], affinity[..wanted]);
            var found = 0;
            for (var i = 0; i < selected; i++)
            {
                ScoreSegment(probeIds[i], query, queryNorm, results, ref found);
            }

            return found;
        }
        finally
        {
            if (rentedIds is not null)
            {
                ArrayPool<int>.Shared.Return(rentedIds);
            }

            if (rentedAffinity is not null)
            {
                ArrayPool<float>.Shared.Return(rentedAffinity);
            }
        }
    }

    // One cell, scored as a straight streaming scan of its contiguous block. The
    // metric test is hoisted out of the loop so the body is a dot product, a
    // multiply, and a bounded insertion that almost always rejects on its first
    // comparison.
    private void ScoreSegment(
        int segment,
        ReadOnlySpan<float> query,
        float queryNorm,
        Span<VectorSearchResult> results,
        ref int found)
    {
        var vectors = _segmentVectors[segment];
        var norms = _segmentNorms[segment];
        var keys = _segmentKeys[segment];
        var count = _segmentCounts[segment];
        var dimensions = _dimensions;

        if (_metric == VectorDistanceMetric.Cosine)
        {
            for (var i = 0; i < count; i++)
            {
                var dot = TensorPrimitives.Dot(query, new ReadOnlySpan<float>(vectors, i * dimensions, dimensions));
                Offer(results, ref found, keys[i], VectorSimilarity.Scale(dot, norms[i], queryNorm));
            }

            return;
        }

        for (var i = 0; i < count; i++)
        {
            var dot = TensorPrimitives.Dot(query, new ReadOnlySpan<float>(vectors, i * dimensions, dimensions));
            Offer(results, ref found, keys[i], dot);
        }
    }

    private int SelectPartitionsCore(
        ReadOnlySpan<float> query, float scale, Span<int> destination, Span<float> affinity)
    {
        var wanted = destination.Length;
        var found = 0;
        var centroids = _centroids;
        for (var partition = 0; partition < _partitionCount; partition++)
        {
            var centroid = new ReadOnlySpan<float>(centroids, partition * _dimensions, _dimensions);
            var score = (2f * TensorPrimitives.Dot(query, centroid) * scale) - _centroidSquaredNorms[partition];

            if (found == wanted &&
                (score < affinity[wanted - 1] ||
                 (score == affinity[wanted - 1] && partition > destination[wanted - 1])))
            {
                continue;
            }

            var i = found < wanted ? found : wanted - 1;
            while (i > 0 && (score > affinity[i - 1] || (score == affinity[i - 1] && partition < destination[i - 1])))
            {
                affinity[i] = affinity[i - 1];
                destination[i] = destination[i - 1];
                i--;
            }

            affinity[i] = score;
            destination[i] = partition;
            if (found < wanted)
            {
                found++;
            }
        }

        return found;
    }

    /// <summary>
    /// Returns the partition whose centroid is nearest a vector, in the
    /// clustering space of the index's metric. The affinity
    /// <c>2 * dot(v, c) * scale - |c|^2</c> is monotone in the negative squared
    /// distance, so ranking by it ranks by proximity without ever forming a
    /// normalised copy of the vector.
    /// </summary>
    private int NearestPartition(ReadOnlySpan<float> vector, float scale) =>
        NearestCentroid(vector, scale, _centroids, _centroidSquaredNorms, _partitionCount, _dimensions);

    private static int NearestCentroid(
        ReadOnlySpan<float> vector,
        float scale,
        float[] centroids,
        float[] centroidSquaredNorms,
        int partitionCount,
        int dimensions)
    {
        var best = 0;
        var bestAffinity = float.NegativeInfinity;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var centroid = new ReadOnlySpan<float>(centroids, partition * dimensions, dimensions);
            var affinity = (2f * TensorPrimitives.Dot(vector, centroid) * scale) - centroidSquaredNorms[partition];
            if (affinity > bestAffinity)
            {
                bestAffinity = affinity;
                best = partition;
            }
        }

        return best;
    }

    // Bounded insertion into the caller's span. k is small and most candidates
    // fail the first comparison, so this beats a heap and, unlike a sort, needs
    // no scratch at all.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Offer(Span<VectorSearchResult> results, ref int found, long key, float score)
    {
        var k = results.Length;
        if (found == k)
        {
            var worst = results[k - 1];
            if (score < worst.Score || (score == worst.Score && key > worst.Key))
            {
                return;
            }
        }

        var i = found < k ? found : k - 1;
        while (i > 0 && IsBetter(score, key, results[i - 1]))
        {
            results[i] = results[i - 1];
            i--;
        }

        results[i] = new VectorSearchResult(key, score);
        if (found < k)
        {
            found++;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsBetter(float score, long key, in VectorSearchResult other) =>
        score > other.Score || (score == other.Score && key < other.Key);
}
