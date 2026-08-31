using System.Buffers;
using System.Numerics.Tensors;

namespace Orleans.Lattice.Vector;

public sealed partial class VectorIndex
{
    // Above this many (vector, centroid) comparisons the assignment pass is worth
    // handing to the thread pool. Assignment is a pure per-vector argmin written
    // to a distinct index, so parallelising it cannot change the result; the mean
    // recomputation that follows stays serial and in ascending order precisely so
    // that float addition order - and therefore the trained centroids - remain
    // reproducible.
    private const long ParallelAssignmentThreshold = 2_000_000;

    // A partitioning of one cell is not a partitioning: it would scan the whole
    // corpus while reporting an approximate answer, which is both slower than the
    // exhaustive path and less honest.
    private const int MinimumUsefulPartitionCount = 2;

    /// <summary>
    /// (Re)builds the partitioning from the vectors currently held, moving the
    /// index to <see cref="VectorIndexState.Ready"/>.
    /// <para>
    /// Training is a bounded, seeded k-means pass: it samples at most
    /// <see cref="VectorIndexOptions.TrainingSampleSize"/> vectors in key order,
    /// refines <see cref="VectorIndexOptions.MaxTrainingIterations"/> times or
    /// until the assignment stops moving, then re-lays the whole corpus out so
    /// each cell owns its members contiguously. It is deterministic in the set of
    /// key / vector pairs and the seed, not in the order they were inserted.
    /// </para>
    /// <para>
    /// Training does not have to be repeated for correctness: inserts and deletes
    /// maintain the cells in place. Retrain when the corpus has grown or churned
    /// enough that the cells no longer reflect it. The re-layout builds the new
    /// cells before releasing the old ones, so a retrain transiently holds two
    /// copies of the corpus; a host that must stay responsive runs it off the
    /// request path, during which the index keeps answering exhaustively and
    /// honestly reports <see cref="VectorIndexState.Building"/>.
    /// </para>
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the index is now trained;
    /// <see langword="false"/> when the corpus is too small to partition usefully
    /// (below <see cref="VectorIndexOptions.MinimumTrainingCount"/> or resolving
    /// to fewer than two partitions), in which case any previous partitioning is
    /// dropped and searches stay exhaustive and exact.
    /// </returns>
    public bool Train()
    {
        var partitionCount = ResolvePartitionCount();
        var count = _count;

        var orderedKeys = ArrayPool<long>.Shared.Rent(Math.Max(1, count));
        var orderedLocations = ArrayPool<long>.Shared.Rent(Math.Max(1, count));
        try
        {
            CollectLiveInKeyOrder(orderedKeys, orderedLocations, count);

            if (partitionCount < MinimumUsefulPartitionCount)
            {
                DropPartitioning(orderedLocations, count);
                return false;
            }

            var sampleCount = Math.Min(count, Math.Max(_options.TrainingSampleSize, partitionCount));
            var sampleLocations = ArrayPool<long>.Shared.Rent(sampleCount);
            try
            {
                DrawSample(orderedLocations, count, sampleLocations, sampleCount);

                var centroidLength = (long)partitionCount * _dimensions;
                if (centroidLength > Array.MaxLength)
                {
                    throw new InvalidOperationException(
                        $"A centroid block of {partitionCount} partitions by {_dimensions} dimensions exceeds the largest array the runtime can allocate.");
                }

                var centroids = new float[(int)centroidLength];
                var centroidSquaredNorms = new float[partitionCount];

                SeedCentroids(sampleLocations, sampleCount, centroids, centroidSquaredNorms, partitionCount);
                Refine(sampleLocations, sampleCount, centroids, centroidSquaredNorms, partitionCount);
                Commit(orderedLocations, count, centroids, centroidSquaredNorms, partitionCount);
                return true;
            }
            finally
            {
                ArrayPool<long>.Shared.Return(sampleLocations);
            }
        }
        finally
        {
            ArrayPool<long>.Shared.Return(orderedKeys);
            ArrayPool<long>.Shared.Return(orderedLocations);
        }
    }

    private int ResolvePartitionCount()
    {
        if (_count < _options.MinimumTrainingCount)
        {
            return 0;
        }

        var requested = _options.PartitionCount > 0
            ? _options.PartitionCount
            : VectorIndexOptions.AutoPartitionCount(_count);
        return Math.Min(requested, _count);
    }

    private void DropPartitioning(long[] orderedLocations, int count)
    {
        if (_partitionCount == 0)
        {
            return;
        }

        Relayout(1, orderedLocations, count, assignment: null);
        _partitionCount = 0;
        _probes = 0;
        _missingCentroids = 0;
        _centroids = [];
        _centroidSquaredNorms = [];
        _centroidsPresent = [];
    }

    // Ordering the live set by key makes every later step - sampling, centroid
    // seeding, and the re-laid-out cell order - a function of the index's contents
    // rather than of the sequence of calls that produced them.
    private void CollectLiveInKeyOrder(long[] keys, long[] locations, int count)
    {
        var written = 0;
        for (var segment = 0; segment < _segmentCount && written < count; segment++)
        {
            var segmentKeys = _segmentKeys[segment];
            var segmentCount = _segmentCounts[segment];
            for (var position = 0; position < segmentCount; position++)
            {
                keys[written] = segmentKeys[position];
                locations[written] = Pack(segment, position);
                written++;
            }
        }

        Array.Sort(keys, locations, 0, count);
    }

    private void DrawSample(long[] liveLocations, int count, long[] sampleLocations, int sampleCount)
    {
        if (sampleCount == count)
        {
            Array.Copy(liveLocations, sampleLocations, count);
            return;
        }

        var pool = ArrayPool<long>.Shared.Rent(count);
        try
        {
            Array.Copy(liveLocations, pool, count);
            var random = new VectorRandom(_options.Seed);
            for (var i = 0; i < sampleCount; i++)
            {
                var pick = i + random.NextInt32(count - i);
                (pool[i], pool[pick]) = (pool[pick], pool[i]);
                sampleLocations[i] = pool[i];
            }
        }
        finally
        {
            ArrayPool<long>.Shared.Return(pool);
        }
    }

    private void SeedCentroids(
        long[] sampleLocations, int sampleCount, float[] centroids, float[] centroidSquaredNorms, int partitionCount)
    {
        var pool = ArrayPool<long>.Shared.Rent(sampleCount);
        try
        {
            Array.Copy(sampleLocations, pool, sampleCount);

            // A second, independently mixed stream so the seeding draw is not a
            // prefix of the sampling draw when the whole corpus is the sample.
            var random = new VectorRandom(_options.Seed ^ 0xA5A5A5A5A5A5A5A5UL);
            for (var partition = 0; partition < partitionCount; partition++)
            {
                var pick = partition + random.NextInt32(sampleCount - partition);
                (pool[partition], pool[pick]) = (pool[pick], pool[partition]);
                WriteMetricSpaceVector(pool[partition], centroids, partition);
            }

            RecomputeSquaredNorms(centroids, centroidSquaredNorms, partitionCount);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(pool);
        }
    }

    private void Refine(
        long[] sampleLocations, int sampleCount, float[] centroids, float[] centroidSquaredNorms, int partitionCount)
    {
        var assignment = ArrayPool<int>.Shared.Rent(sampleCount);
        var previous = ArrayPool<int>.Shared.Rent(sampleCount);
        var affinity = ArrayPool<float>.Shared.Rent(sampleCount);
        var counts = ArrayPool<int>.Shared.Rent(partitionCount);
        try
        {
            previous.AsSpan(0, sampleCount).Fill(-1);
            for (var iteration = 0; iteration < _options.MaxTrainingIterations; iteration++)
            {
                Assign(sampleLocations, sampleCount, centroids, centroidSquaredNorms, partitionCount, assignment, affinity);
                if (assignment.AsSpan(0, sampleCount).SequenceEqual(previous.AsSpan(0, sampleCount)))
                {
                    return;
                }

                assignment.AsSpan(0, sampleCount).CopyTo(previous);
                RecomputeCentroids(
                    sampleLocations, sampleCount, assignment, affinity, centroids, centroidSquaredNorms, counts, partitionCount);
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(assignment);
            ArrayPool<int>.Shared.Return(previous);
            ArrayPool<float>.Shared.Return(affinity);
            ArrayPool<int>.Shared.Return(counts);
        }
    }

    private void RecomputeCentroids(
        long[] sampleLocations,
        int sampleCount,
        int[] assignment,
        float[] affinity,
        float[] centroids,
        float[] centroidSquaredNorms,
        int[] counts,
        int partitionCount)
    {
        Array.Clear(centroids, 0, partitionCount * _dimensions);
        Array.Clear(counts, 0, partitionCount);

        for (var i = 0; i < sampleCount; i++)
        {
            var location = sampleLocations[i];
            var segment = SegmentOf(location);
            var position = PositionOf(location);
            var partition = assignment[i];
            var accumulator = new Span<float>(centroids, partition * _dimensions, _dimensions);
            TensorPrimitives.MultiplyAdd(
                VectorAt(segment, position), ScaleFor(_segmentNorms[segment][position]), accumulator, accumulator);
            counts[partition]++;
        }

        for (var partition = 0; partition < partitionCount; partition++)
        {
            var accumulator = new Span<float>(centroids, partition * _dimensions, _dimensions);
            if (counts[partition] > 0)
            {
                TensorPrimitives.Divide(accumulator, counts[partition], accumulator);
                continue;
            }

            // An empty cell is re-seeded from the sample point the current
            // centroids serve worst, which both refills the cell and reduces the
            // largest quantisation error. Marking that point as taken keeps a
            // second empty cell from claiming it too.
            var worst = ArgMin(affinity, sampleCount);
            affinity[worst] = float.PositiveInfinity;
            WriteMetricSpaceVector(sampleLocations[worst], centroids, partition);
        }

        RecomputeSquaredNorms(centroids, centroidSquaredNorms, partitionCount);
    }

    private void Commit(
        long[] orderedLocations,
        int count,
        float[] centroids,
        float[] centroidSquaredNorms,
        int partitionCount)
    {
        var assignment = ArrayPool<int>.Shared.Rent(Math.Max(1, count));
        var affinity = ArrayPool<float>.Shared.Rent(Math.Max(1, count));
        try
        {
            Assign(orderedLocations, count, centroids, centroidSquaredNorms, partitionCount, assignment, affinity);

            Relayout(partitionCount, orderedLocations, count, assignment);

            _centroids = centroids;
            _centroidSquaredNorms = centroidSquaredNorms;
            _centroidsPresent = [];
            _missingCentroids = 0;
            _partitionCount = partitionCount;
            _probes = _options.Probes > 0
                ? Math.Min(_options.Probes, partitionCount)
                : VectorIndexOptions.AutoProbes(partitionCount);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(assignment);
            ArrayPool<float>.Shared.Return(affinity);
        }
    }

    /// <summary>
    /// Rebuilds the cell blocks so each new cell owns its members contiguously,
    /// copying every live vector from its current position into its new one. The
    /// copy runs in key order, so the resulting layout - and therefore every
    /// snapshot taken from it - depends only on the index's contents.
    /// </summary>
    private void Relayout(int segmentCount, long[] orderedLocations, int count, int[]? assignment)
    {
        var sizes = ArrayPool<int>.Shared.Rent(segmentCount);
        try
        {
            Array.Clear(sizes, 0, segmentCount);
            for (var i = 0; i < count; i++)
            {
                sizes[assignment is null ? 0 : assignment[i]]++;
            }

            var oldVectors = _segmentVectors;
            var oldNorms = _segmentNorms;
            var oldKeys = _segmentKeys;

            var newVectors = new float[segmentCount][];
            var newNorms = new float[segmentCount][];
            var newKeys = new long[segmentCount][];
            var newCounts = new int[segmentCount];
            var newVersions = new long[segmentCount];

            _version++;
            for (var segment = 0; segment < segmentCount; segment++)
            {
                var size = sizes[segment];
                newVectors[segment] = NewBlock(size);
                newNorms[segment] = size == 0 ? [] : new float[size];
                newKeys[segment] = size == 0 ? [] : new long[size];
                newVersions[segment] = _version;
            }

            for (var i = 0; i < count; i++)
            {
                var location = orderedLocations[i];
                var fromSegment = SegmentOf(location);
                var fromPosition = PositionOf(location);
                var toSegment = assignment is null ? 0 : assignment[i];
                var toPosition = newCounts[toSegment]++;

                Array.Copy(
                    oldVectors[fromSegment],
                    (long)fromPosition * _dimensions,
                    newVectors[toSegment],
                    (long)toPosition * _dimensions,
                    _dimensions);

                var key = oldKeys[fromSegment][fromPosition];
                newNorms[toSegment][toPosition] = oldNorms[fromSegment][fromPosition];
                newKeys[toSegment][toPosition] = key;
                _location[key] = Pack(toSegment, toPosition);
            }

            _segmentVectors = newVectors;
            _segmentNorms = newNorms;
            _segmentKeys = newKeys;
            _segmentCounts = newCounts;
            _segmentVersions = newVersions;
            _segmentCount = segmentCount;
            _capacity = count;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(sizes);
        }
    }

    private float[] NewBlock(int size)
    {
        if (size == 0)
        {
            return [];
        }

        var length = (long)size * _dimensions;
        if (length > Array.MaxLength)
        {
            throw new InvalidOperationException(
                $"A cell block of {size} vectors by {_dimensions} dimensions exceeds the largest array the runtime can allocate.");
        }

        return new float[(int)length];
    }

    private void Assign(
        long[] locations,
        int count,
        float[] centroids,
        float[] centroidSquaredNorms,
        int partitionCount,
        int[] assignment,
        float[] affinity)
    {
        if ((long)count * partitionCount >= ParallelAssignmentThreshold)
        {
            Parallel.For(0, count, i =>
                AssignOne(i, locations, centroids, centroidSquaredNorms, partitionCount, assignment, affinity));
            return;
        }

        for (var i = 0; i < count; i++)
        {
            AssignOne(i, locations, centroids, centroidSquaredNorms, partitionCount, assignment, affinity);
        }
    }

    private void AssignOne(
        int i,
        long[] locations,
        float[] centroids,
        float[] centroidSquaredNorms,
        int partitionCount,
        int[] assignment,
        float[] affinity)
    {
        var location = locations[i];
        var segment = SegmentOf(location);
        var position = PositionOf(location);
        var vector = VectorAt(segment, position);
        var scale = ScaleFor(_segmentNorms[segment][position]);

        var best = 0;
        var bestAffinity = float.NegativeInfinity;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var centroid = new ReadOnlySpan<float>(centroids, partition * _dimensions, _dimensions);
            var value = (2f * TensorPrimitives.Dot(vector, centroid) * scale) - centroidSquaredNorms[partition];
            if (value > bestAffinity)
            {
                bestAffinity = value;
                best = partition;
            }
        }

        assignment[i] = best;
        affinity[i] = bestAffinity;
    }

    private void WriteMetricSpaceVector(long location, float[] centroids, int partition)
    {
        var segment = SegmentOf(location);
        var position = PositionOf(location);
        var destination = new Span<float>(centroids, partition * _dimensions, _dimensions);
        TensorPrimitives.Multiply(
            VectorAt(segment, position), ScaleFor(_segmentNorms[segment][position]), destination);
    }

    private void RecomputeSquaredNorms(float[] centroids, float[] centroidSquaredNorms, int partitionCount)
    {
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var centroid = new ReadOnlySpan<float>(centroids, partition * _dimensions, _dimensions);
            centroidSquaredNorms[partition] = TensorPrimitives.Dot(centroid, centroid);
        }
    }

    private static int ArgMin(float[] values, int count)
    {
        var best = 0;
        var bestValue = float.PositiveInfinity;
        for (var i = 0; i < count; i++)
        {
            if (values[i] < bestValue)
            {
                bestValue = values[i];
                best = i;
            }
        }

        return best;
    }
}
