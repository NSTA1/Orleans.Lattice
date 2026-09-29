namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Two arms of <see cref="VectorIndex"/> that every other fixture walks past:
/// clearing an index that has not yet allocated a cell, and the partition
/// selection that outgrows its stack buffer and rents instead.
/// </summary>
/// <remarks>
/// Both are forks whose cold side needs a differently-shaped subject rather
/// than a different call. A fixture reaches <c>Clear</c> only after adding
/// something, which is exactly what allocates the cell the early return exists
/// for; and the rented path needs more partitions than any other fixture in
/// this project configures. Neither is reachable by varying the arguments of an
/// existing test, which is why both stayed uncovered while the methods
/// themselves read as well tested.
/// </remarks>
[TestFixture]
public sealed class VectorIndexUnallocatedAndPooledPathTests
{
    private const int Dimensions = 8;

    private static VectorIndexOptions Options(int partitionCount, int probes = 0) => new()
    {
        Dimensions = Dimensions,
        PartitionCount = partitionCount,
        Probes = probes,
        MinimumTrainingCount = 16,
        TrainingSampleSize = 1_024,
    };

    [Test]
    public void Clear_on_an_index_that_never_allocated_a_cell_still_moves_the_version()
    {
        // A fresh index holds no cell block at all, so Clear has nothing to
        // preserve and returns before the block-retaining path. The version bump
        // still has to happen on this arm: it is what invalidates a snapshot
        // taken against the index, and a Clear that skipped it would let a
        // snapshot plan captured beforehand still render.
        var index = new VectorIndex(Options(partitionCount: 4));
        var before = index.Version;

        index.Clear();

        Assert.Multiple(() =>
        {
            Assert.That(index.Version, Is.GreaterThan(before), "Clear must invalidate any captured snapshot.");
            Assert.That(index.Count, Is.Zero);
            Assert.That(index.PartitionCount, Is.Zero);
            Assert.That(index.State, Is.EqualTo(VectorIndexState.Empty));
        });
    }

    [Test]
    public void An_index_cleared_before_it_allocated_anything_is_still_usable()
    {
        // The early return skips the code that re-seats the retained cell block,
        // so the index must still be able to allocate one on demand afterwards.
        var index = new VectorIndex(Options(partitionCount: 4));
        index.Clear();

        var corpus = VectorCorpus.Clustered(32, Dimensions, clusters: 2, seed: 5);
        for (var i = 0; i < corpus.Length; i++)
        {
            index.Add(i, corpus[i]);
        }

        Assert.That(index.Count, Is.EqualTo(corpus.Length));

        var results = new VectorSearchResult[1];
        Assert.That(index.Search(corpus[3], results, out var mode), Is.EqualTo(1));
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
        Assert.That(results[0].Key, Is.EqualTo(3));
    }

    [Test]
    public void Clearing_twice_before_any_allocation_is_a_no_op_beyond_the_version()
    {
        var index = new VectorIndex(Options(partitionCount: 4));

        index.Clear();
        var afterFirst = index.Version;
        index.Clear();

        Assert.That(index.Version, Is.GreaterThan(afterFirst));
        Assert.That(index.Count, Is.Zero);
    }

    [Test]
    public void SelectPartitions_ranks_the_same_way_whether_it_uses_the_stack_or_the_pool()
    {
        // Above its stack-probe limit SelectPartitions rents its affinity
        // scratch. The rented arm is the one no other fixture reaches, and the
        // property that makes renting safe is that it is invisible: the ordering
        // is documented as total, so the shorter request must be a strict prefix
        // of the longer one across the boundary between the two allocations.
        const int StackProbeLimit = 128;
        const int Partitions = 160;
        const int Corpus = 640;

        var corpus = VectorCorpus.Clustered(Corpus, Dimensions, clusters: 16, seed: 41);
        var index = new VectorIndex(Options(Partitions, probes: Partitions));
        index.EnsureCapacity(Corpus);
        for (var i = 0; i < Corpus; i++)
        {
            index.Add(i, corpus[i]);
        }

        Assert.That(index.Train(), Is.True);
        Assert.That(index.PartitionCount, Is.EqualTo(Partitions),
            "The rented arm is only reached above the stack limit, so the partitioning must be large enough.");

        var query = corpus[17];

        var stackBound = new int[StackProbeLimit];
        var viaStack = index.SelectPartitions(query, stackBound);

        var pooled = new int[Partitions];
        var viaPool = index.SelectPartitions(query, pooled);

        Assert.Multiple(() =>
        {
            Assert.That(viaStack, Is.EqualTo(StackProbeLimit));
            Assert.That(viaPool, Is.EqualTo(Partitions),
                "A destination longer than the stack limit must still be filled to the partition count.");
            Assert.That(pooled.Take(StackProbeLimit), Is.EqualTo(stackBound),
                "The ordering is total, so the stack-sized answer must be a prefix of the rented one.");
            Assert.That(pooled.Distinct().Count(), Is.EqualTo(Partitions),
                "A partition must not be selected twice.");
            Assert.That(pooled, Is.All.InRange(0, Partitions - 1));
        });
    }

    [Test]
    public void The_rented_partition_selection_is_repeatable()
    {
        // Renting is only invisible if the buffer is returned in a state the next
        // caller can rely on. A scratch array handed back dirty, or returned
        // twice and later handed to two callers at once, shows up here as a
        // second call that disagrees with the first.
        const int Partitions = 160;
        const int Corpus = 640;

        var corpus = VectorCorpus.Clustered(Corpus, Dimensions, clusters: 16, seed: 43);
        var index = new VectorIndex(Options(Partitions, probes: Partitions));
        index.EnsureCapacity(Corpus);
        for (var i = 0; i < Corpus; i++)
        {
            index.Add(i, corpus[i]);
        }

        Assert.That(index.Train(), Is.True);

        var first = new int[Partitions];
        index.SelectPartitions(corpus[5], first);

        for (var repeat = 0; repeat < 8; repeat++)
        {
            var again = new int[Partitions];
            index.SelectPartitions(corpus[5], again);
            Assert.That(again, Is.EqualTo(first), $"Repeat {repeat} disagreed with the first selection.");
        }
    }

    [Test]
    public void SelectPartitions_fills_a_destination_of_exactly_the_stack_limit_from_the_stack()
    {
        // The boundary itself. The fork is `wanted <= StackProbeLimit`, so the
        // at-the-limit case must stay on the stack; without this the limit could
        // be lowered by one and only the rented arm would notice.
        const int Partitions = 160;
        const int Corpus = 640;

        var corpus = VectorCorpus.Clustered(Corpus, Dimensions, clusters: 16, seed: 47);
        var index = new VectorIndex(Options(Partitions, probes: Partitions));
        index.EnsureCapacity(Corpus);
        for (var i = 0; i < Corpus; i++)
        {
            index.Add(i, corpus[i]);
        }

        Assert.That(index.Train(), Is.True);

        var exact = new int[128];
        Assert.That(index.SelectPartitions(corpus[9], exact), Is.EqualTo(128));
        Assert.That(exact.Distinct().Count(), Is.EqualTo(128));
    }
}
