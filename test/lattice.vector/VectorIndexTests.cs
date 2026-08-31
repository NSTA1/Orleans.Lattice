namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the index's storage core: construction, insertion, replacement,
/// lookup, capacity, and the edge cases a caller can reach without training.
/// </summary>
[TestFixture]
public sealed partial class VectorIndexTests
{
    private const int Dimensions = 8;

    private static VectorIndex CreateIndex(
        int dimensions = Dimensions,
        VectorDistanceMetric metric = VectorDistanceMetric.Cosine,
        int partitionCount = 0,
        int probes = 0,
        int minimumTrainingCount = 1_024,
        ulong seed = 0x9E3779B97F4A7C15UL) =>
        new(new VectorIndexOptions
        {
            Dimensions = dimensions,
            Metric = metric,
            PartitionCount = partitionCount,
            Probes = probes,
            MinimumTrainingCount = minimumTrainingCount,
            Seed = seed,
        });

    private static float[] Vector(int dimensions, params float[] leading)
    {
        var vector = new float[dimensions];
        leading.CopyTo(vector, 0);
        return vector;
    }

    [Test]
    public void Constructor_rejects_null_options()
    {
        Assert.Throws<ArgumentNullException>(() => new VectorIndex(null!));
    }

    [Test]
    public void Constructor_rejects_options_that_do_not_validate()
    {
        Assert.Throws<ArgumentException>(() => new VectorIndex(new VectorIndexOptions()));
    }

    [Test]
    public void A_new_index_is_empty_and_reports_its_configuration()
    {
        var index = CreateIndex(metric: VectorDistanceMetric.DotProduct);

        Assert.That(index.Count, Is.EqualTo(0));
        Assert.That(index.Capacity, Is.EqualTo(0));
        Assert.That(index.Dimensions, Is.EqualTo(Dimensions));
        Assert.That(index.Metric, Is.EqualTo(VectorDistanceMetric.DotProduct));
        Assert.That(index.PartitionCount, Is.EqualTo(0));
        Assert.That(index.Probes, Is.EqualTo(0));
        Assert.That(index.Version, Is.EqualTo(0));
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Empty));
        Assert.That(index.IsReady, Is.False);
        Assert.That(index.Seed, Is.EqualTo(0x9E3779B97F4A7C15UL));
        Assert.That(index.CentroidsComplete, Is.True);
    }

    [Test]
    public void Status_mirrors_the_index_properties()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));

        var status = index.Status;

        Assert.That(status.State, Is.EqualTo(VectorIndexState.Building));
        Assert.That(status.Count, Is.EqualTo(1));
        Assert.That(status.Capacity, Is.EqualTo(index.Capacity));
        Assert.That(status.Dimensions, Is.EqualTo(Dimensions));
        Assert.That(status.Metric, Is.EqualTo(VectorDistanceMetric.Cosine));
        Assert.That(status.PartitionCount, Is.EqualTo(0));
        Assert.That(status.Probes, Is.EqualTo(0));
        Assert.That(status.Version, Is.EqualTo(index.Version));
        Assert.That(status.IsReady, Is.False);
        Assert.That(status.BytesPerVector, Is.GreaterThan(0));
    }

    [Test]
    public void Add_stores_a_vector_under_its_key()
    {
        var index = CreateIndex();

        index.Add(7, Vector(Dimensions, 1f, 2f));

        Assert.That(index.Count, Is.EqualTo(1));
        Assert.That(index.Contains(7), Is.True);
        Assert.That(index.Contains(8), Is.False);
        Assert.That(index.Version, Is.GreaterThan(0));
    }

    [Test]
    public void Add_rejects_a_vector_of_the_wrong_dimensionality()
    {
        var index = CreateIndex();

        var thrown = Assert.Throws<ArgumentException>(() => index.Add(1, new float[Dimensions + 1]));

        Assert.That(thrown!.Message, Does.Contain("8-dimensional"));
    }

    [Test]
    public void Add_rejects_a_key_that_is_already_present()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));

        var thrown = Assert.Throws<ArgumentException>(() => index.Add(1, Vector(Dimensions, 2f)));

        Assert.That(thrown!.Message, Does.Contain("Upsert"));
    }

    [Test]
    public void Upsert_adds_a_new_key_and_reports_that_nothing_was_replaced()
    {
        var index = CreateIndex();

        Assert.That(index.Upsert(3, Vector(Dimensions, 1f)), Is.False);
        Assert.That(index.Count, Is.EqualTo(1));
    }

    [Test]
    public void Upsert_replaces_an_existing_vector_in_place()
    {
        var index = CreateIndex();
        index.Add(3, Vector(Dimensions, 1f));

        Assert.That(index.Upsert(3, Vector(Dimensions, 0f, 5f)), Is.True);
        Assert.That(index.Count, Is.EqualTo(1));

        var stored = new float[Dimensions];
        Assert.That(index.TryGetVector(3, stored), Is.True);
        Assert.That(stored[0], Is.EqualTo(0f));
        Assert.That(stored[1], Is.EqualTo(5f));
    }

    [Test]
    public void Upsert_rejects_a_vector_of_the_wrong_dimensionality()
    {
        var index = CreateIndex();

        Assert.Throws<ArgumentException>(() => index.Upsert(1, new float[3]));
    }

    [Test]
    public void TryGetVector_copies_the_stored_vector()
    {
        var index = CreateIndex();
        var original = Vector(Dimensions, 1f, 2f, 3f);
        index.Add(9, original);

        var destination = new float[Dimensions];

        Assert.That(index.TryGetVector(9, destination), Is.True);
        Assert.That(destination, Is.EqualTo(original));
    }

    [Test]
    public void TryGetVector_returns_false_for_an_absent_key()
    {
        var index = CreateIndex();

        Assert.That(index.TryGetVector(9, new float[Dimensions]), Is.False);
    }

    [Test]
    public void TryGetVector_rejects_a_destination_that_is_too_short()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));

        Assert.Throws<ArgumentException>(() => index.TryGetVector(1, new float[Dimensions - 1]));
    }

    [Test]
    public void EnsureCapacity_reserves_slots_up_front()
    {
        var index = CreateIndex();

        index.EnsureCapacity(500);

        Assert.That(index.Capacity, Is.EqualTo(500));
    }

    [Test]
    public void EnsureCapacity_never_shrinks_the_backing_block()
    {
        var index = CreateIndex();
        index.EnsureCapacity(500);

        index.EnsureCapacity(10);

        Assert.That(index.Capacity, Is.EqualTo(500));
    }

    [Test]
    public void EnsureCapacity_rejects_a_negative_capacity()
    {
        var index = CreateIndex();

        Assert.Throws<ArgumentOutOfRangeException>(() => index.EnsureCapacity(-1));
    }

    [Test]
    public void Capacity_grows_by_doubling_as_vectors_arrive()
    {
        var index = CreateIndex();

        for (var i = 0; i < 40; i++)
        {
            index.Add(i, Vector(Dimensions, i));
        }

        Assert.That(index.Count, Is.EqualTo(40));
        Assert.That(index.Capacity, Is.GreaterThanOrEqualTo(40));
    }

    [Test]
    public void Clear_drops_every_vector_but_keeps_the_backing_block()
    {
        var index = CreateIndex();
        index.EnsureCapacity(64);
        index.Add(1, Vector(Dimensions, 1f));
        index.Add(2, Vector(Dimensions, 2f));

        index.Clear();

        Assert.That(index.Count, Is.EqualTo(0));
        Assert.That(index.Contains(1), Is.False);
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Empty));
        Assert.That(index.Capacity, Is.EqualTo(64));
    }

    [Test]
    public void Clear_drops_a_trained_partitioning()
    {
        var index = BuildTrainedIndex(count: 2_000, dimensions: Dimensions, partitionCount: 8);

        index.Clear();

        Assert.That(index.PartitionCount, Is.EqualTo(0));
        Assert.That(index.Probes, Is.EqualTo(0));
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Empty));
    }

    [Test]
    public void Clear_lets_the_index_be_refilled()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));
        index.Clear();

        index.Add(1, Vector(Dimensions, 2f));

        Assert.That(index.Count, Is.EqualTo(1));
        Assert.That(index.Contains(1), Is.True);
    }

    [Test]
    public void PartitionVersion_and_PartitionSize_reject_an_identifier_outside_the_partitioning()
    {
        var index = CreateIndex();

        Assert.Throws<ArgumentOutOfRangeException>(() => index.PartitionVersion(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => index.PartitionSize(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => index.PartitionVersion(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => index.PartitionSize(-1));
    }

    /// <summary>
    /// Builds a trained index over a deterministic clustered corpus keyed
    /// <c>0 .. count - 1</c>, the shared fixture for every test that needs the
    /// approximate path.
    /// </summary>
    private static VectorIndex BuildTrainedIndex(
        int count,
        int dimensions,
        int partitionCount,
        int probes = 0,
        VectorDistanceMetric metric = VectorDistanceMetric.Cosine,
        ulong seed = 0x9E3779B97F4A7C15UL)
    {
        var corpus = VectorCorpus.Clustered(count, dimensions, clusters: partitionCount, seed: 17);
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = dimensions,
            Metric = metric,
            PartitionCount = partitionCount,
            Probes = probes,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 4_096,
            Seed = seed,
        });

        index.EnsureCapacity(count);
        for (var i = 0; i < count; i++)
        {
            index.Add(i, corpus[i]);
        }

        index.Train();
        return index;
    }
}
