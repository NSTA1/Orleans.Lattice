using System.Numerics.Tensors;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Vector;

/// <summary>
/// An allocation-lean approximate nearest-neighbour index over dense
/// <see cref="float"/> vectors, keyed by a caller-supplied <see cref="long"/>.
/// <para>
/// <b>Structure.</b> The index is an inverted file (IVF): a seeded k-means pass
/// partitions the corpus into roughly <c>sqrt(n)</c> cells, each cell owns the
/// vectors assigned to it, and a search ranks the cell centroids and scores only
/// the vectors in the best few cells. Query cost is therefore sub-linear in the
/// corpus - the fraction scanned falls as the corpus grows - which is the
/// property that makes retrieval over a very large corpus bounded rather than
/// proportional to it. The accuracy / latency trade-off is the single
/// <see cref="VectorIndexOptions.Probes"/> dial.
/// </para>
/// <para>
/// <b>Storage.</b> Each cell holds its members' vectors in one contiguous
/// <see cref="float"/> block, never as a per-vector object graph and never as an
/// index into a shared block. That layout is what makes a probe fast: scoring a
/// cell is a straight streaming scan the hardware prefetcher can follow, rather
/// than a scattered walk over the whole corpus. An untrained index is the same
/// structure with a single cell, so the exhaustive path streams too.
/// </para>
/// <para>
/// <b>Allocation.</b> The steady-state query path allocates nothing: results are
/// written into a caller-owned span, the probe scratch is stack-allocated for
/// ordinary probe counts and pooled beyond them, and no metric needs a normalised
/// copy of the query. The insert path allocates only the cell blocks (nothing at
/// all after <see cref="EnsureCapacity"/>), and training rents its scratch from
/// the array pool.
/// </para>
/// <para>
/// <b>Determinism.</b> A result set is totally ordered by descending score with
/// ascending key breaking ties, and training samples the corpus in key order from
/// an explicitly seeded generator. The same set of key / vector pairs with the
/// same options therefore produces byte-identical results irrespective of the
/// order in which they were inserted or deleted.
/// </para>
/// <para>
/// <b>Threading.</b> An index is safe for concurrent readers <i>or</i> a single
/// writer, not both. A host that mutates concurrently with searches must
/// serialise access itself; the natural home is a single-threaded grain.
/// </para>
/// </summary>
public sealed partial class VectorIndex
{
    private readonly VectorIndexOptions _options;
    private readonly int _dimensions;
    private readonly VectorDistanceMetric _metric;

    // key -> packed (segment, position). A long value keeps the map primitive, so
    // a lookup neither boxes nor chases a reference.
    private readonly Dictionary<long, long> _location;

    private float[][] _segmentVectors = [];
    private float[][] _segmentNorms = [];
    private long[][] _segmentKeys = [];
    private int[] _segmentCounts = [];
    private long[] _segmentVersions = [];
    private int _segmentCount;
    private int _capacity;

    private float[] _centroids = [];
    private float[] _centroidSquaredNorms = [];
    private bool[] _centroidsPresent = [];
    private HashSet<long>? _provisional;
    private int _partitionCount;
    private int _centroidChunkCount;
    private int _missingCentroids;
    private int _probes;
    private int _count;
    private long _version;

    /// <summary>
    /// Creates an empty index from the supplied options. The options are
    /// validated and copied, so later mutation of the instance passed in does not
    /// affect the index.
    /// </summary>
    /// <param name="options">The index configuration. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">The options are not usable; see <see cref="VectorIndexOptions.Validate"/>.</exception>
    public VectorIndex(VectorIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options.Clone();
        _dimensions = _options.Dimensions;
        _metric = _options.Metric;
        _location = new Dictionary<long, long>();
    }

    /// <summary>The fixed dimensionality every vector and query must have.</summary>
    public int Dimensions => _dimensions;

    /// <summary>The similarity kernel this index ranks by.</summary>
    public VectorDistanceMetric Metric => _metric;

    /// <summary>The number of live vectors the index holds.</summary>
    public int Count => _count;

    /// <summary>
    /// The total number of vector slots reserved across the index's cells. Never
    /// shrinks on delete; a vacated position is refilled rather than released.
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>The number of trained partitions, or <c>0</c> when the index is untrained.</summary>
    public int PartitionCount => _partitionCount;

    /// <summary>The number of partitions a search probes, or <c>0</c> when the index is untrained.</summary>
    public int Probes => _probes;

    /// <summary>
    /// A monotonically increasing counter bumped by every mutation. A consumer
    /// caches against it to detect that the index moved.
    /// </summary>
    public long Version => _version;

    /// <summary>
    /// Whether the index can answer from its partitioning. While this is
    /// <see langword="false"/> the index is still building and searches fall back
    /// to an exact exhaustive scan.
    /// </summary>
    public bool IsReady => State == VectorIndexState.Ready;

    /// <summary>
    /// Whether the index is empty, still building its partitioning, or ready.
    /// </summary>
    public VectorIndexState State =>
        _count == 0 && _partitionCount == 0 ? VectorIndexState.Empty
        : _partitionCount > 0 && _missingCentroids == 0 ? VectorIndexState.Ready
        : VectorIndexState.Building;

    /// <summary>
    /// Returns an immutable snapshot of the index's shape and readiness, cheap
    /// enough to read on every request.
    /// </summary>
    public VectorIndexStatus Status => new(
        State, _count, Capacity, _dimensions, _metric, _partitionCount, _probes, _version);

    /// <summary>
    /// Returns the version stamp of one partition, bumped whenever a vector
    /// enters or leaves it. A durable consumer persists only the partitions whose
    /// stamp has moved since it last wrote them.
    /// </summary>
    /// <param name="partitionId">The zero-based partition identifier.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionId"/> is outside <c>[0, PartitionCount)</c>.</exception>
    public long PartitionVersion(int partitionId)
    {
        RequirePartition(partitionId);
        return _segmentVersions[partitionId];
    }

    /// <summary>
    /// Returns the number of live vectors held by one partition.
    /// </summary>
    /// <param name="partitionId">The zero-based partition identifier.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionId"/> is outside <c>[0, PartitionCount)</c>.</exception>
    public int PartitionSize(int partitionId)
    {
        RequirePartition(partitionId);
        return _segmentCounts[partitionId];
    }

    /// <summary>
    /// Reserves room for at least <paramref name="capacity"/> vectors so a
    /// subsequent insert run does not reallocate. On an untrained index the whole
    /// reservation goes to its single cell, which is why filling a reserved index
    /// allocates nothing at all; on a trained one it is spread evenly over the
    /// cells, which is a hint rather than a guarantee because a cell's true size
    /// depends on the data.
    /// </summary>
    /// <param name="capacity">The number of vectors to reserve room for.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
    public void EnsureCapacity(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        EnsureSegmentsExist();

        if (_segmentCount == 1)
        {
            ReserveSegment(0, capacity);
        }
        else
        {
            var perSegment = (capacity + _segmentCount - 1) / _segmentCount;
            for (var segment = 0; segment < _segmentCount; segment++)
            {
                ReserveSegment(segment, perSegment);
            }
        }

        _location.EnsureCapacity(capacity);
    }

    /// <summary>
    /// Whether the index currently holds a vector under the given key.
    /// </summary>
    /// <param name="key">The caller-supplied key.</param>
    public bool Contains(long key) => _location.ContainsKey(key);

    /// <summary>
    /// Copies the stored vector for a key into <paramref name="destination"/>.
    /// </summary>
    /// <param name="key">The caller-supplied key.</param>
    /// <param name="destination">A span of at least <see cref="Dimensions"/> floats to copy into.</param>
    /// <returns><see langword="true"/> when the key was present and the vector was copied.</returns>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is shorter than <see cref="Dimensions"/>.</exception>
    public bool TryGetVector(long key, Span<float> destination)
    {
        if (destination.Length < _dimensions)
        {
            throw new ArgumentException(
                $"Destination must hold at least {_dimensions} floats to receive a vector, but holds {destination.Length}.",
                nameof(destination));
        }

        if (!_location.TryGetValue(key, out var location))
        {
            return false;
        }

        VectorAt(SegmentOf(location), PositionOf(location)).CopyTo(destination);
        return true;
    }

    /// <summary>
    /// Adds a vector under a key not already present.
    /// </summary>
    /// <param name="key">The caller-supplied key. Must not already be in the index.</param>
    /// <param name="vector">The vector, of exactly <see cref="Dimensions"/> components.</param>
    /// <exception cref="ArgumentException"><paramref name="vector"/> has the wrong length, or <paramref name="key"/> is already present.</exception>
    public void Add(long key, ReadOnlySpan<float> vector)
    {
        RequireDimensions(vector.Length, nameof(vector));
        if (_location.ContainsKey(key))
        {
            throw new ArgumentException(
                $"Key {key} is already present in the index. Use Upsert to replace an existing vector.",
                nameof(key));
        }

        Insert(key, vector);
    }

    /// <summary>
    /// Adds a vector, replacing any vector already stored under the key. This is
    /// the operation a maintenance loop uses when a source is re-embedded.
    /// </summary>
    /// <param name="key">The caller-supplied key.</param>
    /// <param name="vector">The vector, of exactly <see cref="Dimensions"/> components.</param>
    /// <returns><see langword="true"/> when an existing vector was replaced.</returns>
    /// <exception cref="ArgumentException"><paramref name="vector"/> has the wrong length.</exception>
    public bool Upsert(long key, ReadOnlySpan<float> vector)
    {
        RequireDimensions(vector.Length, nameof(vector));
        var replaced = Remove(key);
        Insert(key, vector);
        return replaced;
    }

    /// <summary>
    /// Removes the vector stored under a key. Removing an absent key is a no-op,
    /// so repeated deletion is idempotent. A removed vector can never appear in a
    /// later result: it leaves its cell immediately rather than being tombstoned
    /// and filtered at query time.
    /// </summary>
    /// <param name="key">The caller-supplied key.</param>
    /// <returns><see langword="true"/> when a vector was removed.</returns>
    public bool Remove(long key)
    {
        if (!_location.Remove(key, out var location))
        {
            return false;
        }

        var segment = SegmentOf(location);
        var position = PositionOf(location);
        var last = _segmentCounts[segment] - 1;

        // Backfill the hole with the cell's final member so the block stays dense
        // and a probe never has to test liveness per vector.
        if (position != last)
        {
            var vectors = _segmentVectors[segment];
            Array.Copy(vectors, last * _dimensions, vectors, position * _dimensions, _dimensions);
            _segmentNorms[segment][position] = _segmentNorms[segment][last];
            var moved = _segmentKeys[segment][last];
            _segmentKeys[segment][position] = moved;
            _location[moved] = Pack(segment, position);
        }

        _segmentCounts[segment] = last;
        _count--;
        _version++;
        _segmentVersions[segment] = _version;
        return true;
    }

    /// <summary>
    /// Drops every vector and the trained partitioning, returning the index to
    /// <see cref="VectorIndexState.Empty"/>. The first cell's block is retained,
    /// so refilling an index that was never partitioned allocates nothing.
    /// </summary>
    public void Clear()
    {
        _location.Clear();
        _count = 0;
        _partitionCount = 0;
        _probes = 0;
        _centroidChunkCount = 0;
        _missingCentroids = 0;
        _provisional = null;
        _centroids = [];
        _centroidSquaredNorms = [];
        _centroidsPresent = [];

        if (_segmentCount == 0)
        {
            _version++;
            return;
        }

        var vectors = _segmentVectors[0];
        var norms = _segmentNorms[0];
        var keys = _segmentKeys[0];
        AllocateSegments(1);
        _segmentVectors[0] = vectors;
        _segmentNorms[0] = norms;
        _segmentKeys[0] = keys;
        _capacity = keys.Length;
        _version++;
    }

    private void Insert(long key, ReadOnlySpan<float> vector)
    {
        EnsureSegmentsExist();
        var norm = TensorPrimitives.Norm(vector);
        int segment;

        if (_partitionCount == 0)
        {
            segment = 0;
        }
        else if (_missingCentroids > 0)
        {
            // A restore is still streaming its centroids, so there is no honest
            // nearest cell yet - ranking against a partly zeroed centroid block
            // would place the vector arbitrarily and leave it in a cell its own
            // query never probes once the index went Ready. Park it instead: the
            // index is Building meanwhile, so an exhaustive search still finds it,
            // and it is re-placed the moment the partitioning completes.
            segment = 0;
            (_provisional ??= new HashSet<long>()).Add(key);
        }
        else
        {
            segment = NearestPartition(vector, ScaleFor(norm));
        }

        Append(segment, key, vector, norm);
        _count++;
    }

    private void Append(int segment, long key, ReadOnlySpan<float> vector, float norm)
    {
        var position = _segmentCounts[segment];
        ReserveSegment(segment, position + 1);
        vector.CopyTo(_segmentVectors[segment].AsSpan(position * _dimensions, _dimensions));
        _segmentNorms[segment][position] = norm;
        _segmentKeys[segment][position] = key;
        _segmentCounts[segment] = position + 1;
        _location[key] = Pack(segment, position);
        _version++;
        _segmentVersions[segment] = _version;
    }

    private void EnsureSegmentsExist()
    {
        if (_segmentCount == 0)
        {
            AllocateSegments(1);
        }
    }

    private void AllocateSegments(int segmentCount)
    {
        _segmentVectors = new float[segmentCount][];
        _segmentNorms = new float[segmentCount][];
        _segmentKeys = new long[segmentCount][];
        _segmentCounts = new int[segmentCount];
        _segmentVersions = new long[segmentCount];
        for (var segment = 0; segment < segmentCount; segment++)
        {
            _segmentVectors[segment] = [];
            _segmentNorms[segment] = [];
            _segmentKeys[segment] = [];
        }

        _segmentCount = segmentCount;
        _capacity = 0;
    }

    private void ReserveSegment(int segment, int capacity)
    {
        var current = _segmentKeys[segment].Length;
        if (capacity <= current)
        {
            return;
        }

        var target = Math.Max(capacity, current * 2);
        var blockLength = (long)target * _dimensions;
        if (blockLength > Array.MaxLength)
        {
            throw new InvalidOperationException(
                $"A cell block of {target} vectors by {_dimensions} dimensions exceeds the largest array the runtime can allocate.");
        }

        Array.Resize(ref _segmentVectors[segment], (int)blockLength);
        Array.Resize(ref _segmentNorms[segment], target);
        Array.Resize(ref _segmentKeys[segment], target);
        _capacity += target - current;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<float> VectorAt(int segment, int position) =>
        new(_segmentVectors[segment], position * _dimensions, _dimensions);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private float ScaleFor(float norm) =>
        _metric != VectorDistanceMetric.Cosine ? 1f
        : norm == 0f ? 0f
        : 1f / norm;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Pack(int segment, int position) => ((long)segment << 32) | (uint)position;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SegmentOf(long location) => (int)(location >> 32);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int PositionOf(long location) => (int)(uint)location;

    private void RequirePartition(int partitionId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitionId);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(partitionId, _partitionCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RequireDimensions(int length, string parameterName)
    {
        if (length != _dimensions)
        {
            throw new ArgumentException(
                $"This index stores {_dimensions}-dimensional vectors, but a vector of {length} components was supplied.",
                parameterName);
        }
    }
}
