using Orleans.Lattice.GrainIndex.Tests.Enrollment;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="IndexedGrain{TState}"/>: the optional convenience facade
/// over an <see cref="IndexedAttribute"/> state object.
/// </summary>
/// <remarks>
/// The fixture is deliberately about forwarding and nothing else. The base class
/// holds no enrolment logic - the attribute does all of it - so proving it
/// forwards is proving the whole of its contract.
/// </remarks>
[TestFixture]
public sealed class IndexedGrainTests
{
    private sealed class ProbeGrain(IPersistentState<IndexedTestState> state)
        : IndexedGrain<IndexedTestState>(state)
    {
        public IndexedTestState Current
        {
            get => State;
            set => State = value;
        }

        public bool Stored => RecordExists;

        public string? Tag => Etag;

        public IPersistentState<IndexedTestState> Underlying => PersistentState;

        public Task WriteAsync() => WriteStateAsync();

        public Task ReadAsync() => ReadStateAsync();

        public Task ClearAsync() => ClearStateAsync();
    }

    private static RecordingPersistentState<IndexedTestState> StateOf(bool recordExists = true) =>
        new(new IndexedTestState { Age = 30, Country = "GB" }, recordExists);

    [Test]
    public void The_state_property_reads_and_writes_the_underlying_state_object()
    {
        var inner = StateOf();
        var grain = new ProbeGrain(inner);
        var replacement = new IndexedTestState { Age = 41 };

        grain.Current = replacement;

        Assert.Multiple(() =>
        {
            Assert.That(inner.State, Is.SameAs(replacement));
            Assert.That(grain.Current, Is.SameAs(replacement));
        });
    }

    [Test]
    public void The_record_flag_and_etag_come_straight_from_the_state_object()
    {
        var inner = StateOf(recordExists: false);
        var grain = new ProbeGrain(inner);

        Assert.Multiple(() =>
        {
            Assert.That(grain.Stored, Is.False,
                "Grain<TState> gives no way to tell a never-written grain from a default one, which "
                + "is the gap this facade closes.");
            Assert.That(grain.Tag, Is.EqualTo(inner.Etag));
        });
    }

    [Test]
    public void The_underlying_state_object_is_reachable_for_a_grain_that_needs_it()
    {
        var inner = StateOf();

        Assert.That(new ProbeGrain(inner).Underlying, Is.SameAs(inner));
    }

    [Test]
    public async Task Every_operation_forwards_to_the_state_object_that_does_the_indexing()
    {
        var inner = StateOf();
        var grain = new ProbeGrain(inner);

        await grain.WriteAsync();
        await grain.ReadAsync();
        await grain.ClearAsync();

        Assert.Multiple(() =>
        {
            Assert.That(inner.WriteCount, Is.EqualTo(1));
            Assert.That(inner.ReadCount, Is.EqualTo(1));
            Assert.That(inner.ClearCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_null_state_object_is_rejected_at_construction()
    {
        Assert.That(() => new ProbeGrain(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_facade_is_a_grain_so_it_composes_with_the_orleans_runtime_as_usual()
    {
        Assert.That(typeof(Grain).IsAssignableFrom(typeof(IndexedGrain<IndexedTestState>)), Is.True);
    }
}
