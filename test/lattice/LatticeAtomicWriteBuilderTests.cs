using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public cross-tree atomic-write authoring surface:
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite"/> and the
/// <see cref="LatticeAtomicWriteBuilder"/> it opens.
/// <para>
/// The builder is a pure accumulator - it only becomes observable at
/// <see cref="LatticeAtomicWriteBuilder.CommitAsync"/>, where each staged slice is
/// projected into a <see cref="LatticeTreeBatch"/> - so these tests substitute the
/// cross-tree coordinator grain and assert on the batch shape that actually reaches
/// the wire: the serializer-typed overloads, the at-most-one-predicate-per-tree rule,
/// and the null-carry optimisations that keep a value-only, upsert-only slice
/// byte-identical to the pre-builder path.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeAtomicWriteBuilderTests
{
    private sealed record Doc(string Name, int Score);

    /// <summary>A serializer distinguishable from the JSON default, to prove overload dispatch.</summary>
    private sealed class UpperCaseSerializer : ILatticeSerializer<string>
    {
        public int SerializeCalls { get; private set; }

        public byte[] Serialize(string value)
        {
            SerializeCalls++;
            return Encoding.UTF8.GetBytes(value.ToUpperInvariant());
        }

        public string Deserialize(byte[] bytes) => Encoding.UTF8.GetString(bytes).ToLowerInvariant();
    }

    private static IGrainFactory FactoryCapturing(out List<List<LatticeTreeBatch>> captured)
    {
        var batches = new List<List<LatticeTreeBatch>>();
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.CommitAsync(Arg.Any<List<LatticeTreeBatch>>()).Returns(call =>
        {
            batches.Add(call.ArgAt<List<LatticeTreeBatch>>(0));
            return Task.FromResult(CrossTreeAtomicWriteOutcome.Committed);
        });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeCrossTreeTxGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(coordinator);
        captured = batches;
        return factory;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // === BeginAtomicWrite operation-id validation ===

    [Test]
    public void BeginAtomicWrite_rejects_a_null_factory()
    {
        Assert.Throws<ArgumentNullException>(() => ((IGrainFactory)null!).BeginAtomicWrite("op"));
    }

    [Test]
    public void BeginAtomicWrite_rejects_an_operation_id_containing_the_grain_key_separator()
    {
        var factory = Substitute.For<IGrainFactory>();

        // '/' is the grain-key separator, so admitting it would let one caller's
        // operation id collide with another's tree-scoped key.
        var ex = Assert.Throws<ArgumentException>(() => factory.BeginAtomicWrite("tenant/op"));
        Assert.That(ex!.ParamName, Is.EqualTo("operationId"));
        Assert.That(ex.Message, Does.Contain("'/'"));
    }

    [TestCase("")]
    [TestCase("   ")]
    public void BeginAtomicWrite_rejects_a_blank_operation_id(string operationId)
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.Throws<ArgumentException>(() => factory.BeginAtomicWrite(operationId));
    }

    [Test]
    public void BeginAtomicWrite_rejects_a_null_operation_id()
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.Throws<ArgumentNullException>(() => factory.BeginAtomicWrite(null!));
    }

    [Test]
    public void SetManyAtomicAsync_rejects_an_operation_id_containing_the_grain_key_separator()
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.Throws<ArgumentException>(() =>
            factory.SetManyAtomicAsync([], "tenant/op"));
    }

    // === Typed Set overloads ===

    [Test]
    public async Task Set_with_an_explicit_serializer_stages_that_serializer_s_bytes()
    {
        var factory = FactoryCapturing(out var captured);
        var serializer = new UpperCaseSerializer();

        await factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .Set("k", "hello", serializer)
            .CommitAsync();

        Assert.That(serializer.SerializeCalls, Is.EqualTo(1));
        var entries = captured.Single().Single().Entries;
        Assert.That(entries.Single().Key, Is.EqualTo("k"));
        Assert.That(Encoding.UTF8.GetString(entries.Single().Value), Is.EqualTo("HELLO"));
    }

    [Test]
    public async Task Set_without_a_serializer_falls_back_to_the_json_default()
    {
        var factory = FactoryCapturing(out var captured);

        await factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .Set("k", new Doc("ada", 7))
            .CommitAsync();

        var value = captured.Single().Single().Entries.Single().Value;
        Assert.That(JsonSerializer.Deserialize<Doc>(value), Is.EqualTo(new Doc("ada", 7)));
    }

    [Test]
    public void Set_with_a_serializer_rejects_a_null_key()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op").ForTree("orders");

        Assert.Throws<ArgumentNullException>(
            () => builder.Set(null!, "v", JsonLatticeSerializer<string>.Default));
    }

    [Test]
    public void Set_with_a_serializer_rejects_a_null_serializer()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op").ForTree("orders");

        Assert.Throws<ArgumentNullException>(() => builder.Set<string>("k", "v", null!));
    }

    [Test]
    public void Set_with_a_serializer_requires_a_selected_tree()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op");

        Assert.Throws<InvalidOperationException>(
            () => builder.Set("k", "v", JsonLatticeSerializer<string>.Default));
    }

    [Test]
    public async Task Set_with_a_serializer_forwards_no_delta_or_delete_carry()
    {
        var factory = FactoryCapturing(out var captured);

        await factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .Set("k", "v", JsonLatticeSerializer<string>.Default)
            .CommitAsync();

        // A value-only, upsert-only slice must stay byte-identical to the
        // pre-builder cross-tree write: both optional carries stay null.
        var batch = captured.Single().Single();
        Assert.That(batch.EntryDeltas, Is.Null);
        Assert.That(batch.EntryDeletes, Is.Null);
    }

    // === SetWhere predicate rules ===

    [Test]
    public async Task SetWhere_attaches_the_compiled_guard_predicate_to_the_slice()
    {
        var factory = FactoryCapturing(out var captured);

        await factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .SetWhere<Doc>("k", new Doc("ada", 7), d => d.Score > 3)
            .CommitAsync();

        Assert.That(captured.Single().Single().Predicate, Is.Not.Null);
    }

    [Test]
    public void SetWhere_rejects_a_second_different_predicate_on_the_same_tree()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op")
            .ForTree("orders")
            .SetWhere<Doc>("a", new Doc("ada", 7), d => d.Score > 3);

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.SetWhere<Doc>("b", new Doc("bob", 9), d => d.Score > 5));
        Assert.That(ex!.Message, Does.Contain("orders"));
        Assert.That(ex.Message, Does.Contain("at most one"));
    }

    [Test]
    public void SetWhere_admits_a_restatement_of_an_identical_predicate_on_the_same_tree()
    {
        var factory = Substitute.For<IGrainFactory>();
        var builder = factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .SetWhere<Doc>("a", new Doc("ada", 7), d => d.Score > 3);

        // The guard is `!existing.Equals(ir)`, which admits a restatement of the
        // same predicate for a second key in the slice. Since issue #1827 gave
        // LatticePredicateNode structural equality, two compilations of the same
        // predicate compare equal, so restating it is idempotent rather than a
        // conflict.
        Assert.DoesNotThrow(
            () => builder.SetWhere<Doc>("b", new Doc("bob", 9), d => d.Score > 3));
    }

    [Test]
    public void SetWhere_lets_distinct_trees_each_carry_their_own_predicate()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op")
            .ForTree("orders")
            .SetWhere<Doc>("a", new Doc("ada", 7), d => d.Score > 3);

        Assert.DoesNotThrow(() => builder
            .ForTree("customers")
            .SetWhere<Doc>("b", new Doc("bob", 9), d => d.Score > 5));
    }

    [Test]
    public void SetWhere_rejects_null_arguments()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op").ForTree("orders");
        Expression<Func<Doc, bool>> predicate = d => d.Score > 1;

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => builder.SetWhere(null!, new Doc("a", 1), predicate, JsonLatticeSerializer<Doc>.Default));
            Assert.Throws<ArgumentNullException>(
                () => builder.SetWhere<Doc>("k", new Doc("a", 1), null!, JsonLatticeSerializer<Doc>.Default));
            Assert.Throws<ArgumentNullException>(
                () => builder.SetWhere("k", new Doc("a", 1), predicate, null!));
        });
    }

    // === Slice accumulation ===

    [Test]
    public async Task ForTree_reselecting_a_tree_appends_to_the_same_slice()
    {
        var factory = FactoryCapturing(out var captured);

        await factory.BeginAtomicWrite("op")
            .ForTree("orders").Set("a", Bytes("1"))
            .ForTree("customers").Set("b", Bytes("2"))
            .ForTree("orders").Set("c", Bytes("3"))
            .CommitAsync();

        var batches = captured.Single();
        Assert.That(batches.Select(b => b.TreeId), Is.EqualTo(new[] { "orders", "customers" }));
        Assert.That(batches[0].Entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "c" }));
    }

    [Test]
    public void ForTree_rejects_a_blank_tree_id()
    {
        var builder = Substitute.For<IGrainFactory>().BeginAtomicWrite("op");

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => builder.ForTree(null!));
            Assert.Throws<ArgumentException>(() => builder.ForTree(string.Empty));
        });
    }

    [Test]
    public async Task Delete_sets_the_per_entry_delete_carry_for_the_slice()
    {
        var factory = FactoryCapturing(out var captured);

        await factory.BeginAtomicWrite("op")
            .ForTree("orders")
            .Set("new", Bytes("v"))
            .Delete("old")
            .CommitAsync();

        var batch = captured.Single().Single();
        Assert.That(batch.EntryDeletes, Is.EqualTo(new[] { false, true }));
    }

    [Test]
    public async Task CommitAsync_keys_the_coordinator_by_the_operation_id()
    {
        var factory = FactoryCapturing(out _);

        await factory.BeginAtomicWrite("op-42").ForTree("orders").Set("k", Bytes("v")).CommitAsync();

        factory.Received(1).GetGrain<ILatticeCrossTreeTxGrain>("op-42", Arg.Any<string?>());
    }

    [Test]
    public void CommitAsync_observes_cancellation_before_dispatch()
    {
        var factory = FactoryCapturing(out var captured);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(() =>
            factory.BeginAtomicWrite("op").ForTree("orders").Set("k", Bytes("v")).CommitAsync(cts.Token));
        Assert.That(captured, Is.Empty);
    }
}
