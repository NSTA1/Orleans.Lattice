using NSubstitute;
using System.Linq.Expressions;
using System.Text.Json;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Coverage for the predicate-pushdown scan overloads on
/// <see cref="TypedLatticeExtensions"/>: the typed <c>KeysAsync</c>,
/// <c>EntriesAsync</c>, and <c>ValuesAsync</c> families that compile a
/// <see cref="Expression"/> to a <c>LatticePredicateNode</c> and hand it to the
/// owning leaf, so non-matching rows are dropped server-side and never cross the
/// wire.
/// <para>
/// Each family ships two overloads - one taking an explicit
/// <see cref="ILatticeSerializer{T}"/> and one defaulting to
/// <c>JsonLatticeSerializer&lt;T&gt;.Default</c> - and every one of them is
/// public API surface, so the repository's "every public member has at least one
/// test" rule applies. These tests assert three things per overload: that the
/// predicate is actually pushed down (the unfiltered scan entry point is never
/// called), that the typed projection round-trips, and that the range, reverse,
/// and prefetch parameters are forwarded verbatim.
/// </para>
/// </summary>
public class TypedLatticeExtensionsPredicateScanTests
{
    private record TestItem(string Name, int Score);

    private static readonly ILatticeSerializer<TestItem> Serializer = JsonLatticeSerializer<TestItem>.Default;

    private static ILattice CreateMock() => Substitute.For<ILattice>();

    private static byte[] Bytes(TestItem item) => JsonSerializer.SerializeToUtf8Bytes(item);

    // ── KeysAsync (predicate) ───────────────────────────────────

    [Test]
    public async Task KeysAsync_with_predicate_and_explicit_serializer_streams_the_pushed_down_keys()
    {
        var lattice = CreateMock();
        lattice.KeysWherePredicateAsync(Arg.Any<LatticePredicateNode>(), null, null, false, null)
            .Returns(new[] { "k1", "k2" }.ToAsyncEnumerable());

        var result = new List<string>();
        await foreach (var key in lattice.KeysAsync<TestItem>(i => i.Score > 5, Serializer))
            result.Add(key);

        Assert.That(result, Is.EqualTo(new[] { "k1", "k2" }));

        // The unfiltered scan must never be consulted: that is what proves the
        // filter ran on the leaf rather than in the client.
        lattice.DidNotReceiveWithAnyArgs().KeysAsync(default, default, default, default);
    }

    [Test]
    public async Task KeysAsync_with_predicate_and_default_serializer_streams_the_pushed_down_keys()
    {
        var lattice = CreateMock();
        lattice.KeysWherePredicateAsync(Arg.Any<LatticePredicateNode>(), null, null, false, null)
            .Returns(new[] { "k1" }.ToAsyncEnumerable());

        var result = new List<string>();
        await foreach (var key in lattice.KeysAsync<TestItem>(i => i.Score > 5))
            result.Add(key);

        Assert.That(result, Is.EqualTo(new[] { "k1" }));
        lattice.DidNotReceiveWithAnyArgs().KeysAsync(default, default, default, default);
    }

    [Test]
    public async Task KeysAsync_with_predicate_forwards_range_reverse_and_prefetch()
    {
        var lattice = CreateMock();
        lattice.KeysWherePredicateAsync(Arg.Any<LatticePredicateNode>(), "k1", "k9", true, true)
            .Returns(new[] { "k2" }.ToAsyncEnumerable());

        var result = new List<string>();
        await foreach (var key in lattice.KeysAsync<TestItem>(
            i => i.Score > 5, Serializer, startInclusive: "k1", endExclusive: "k9", reverse: true, prefetch: true))
        {
            result.Add(key);
        }

        Assert.That(result, Is.EqualTo(new[] { "k2" }));
        lattice.Received(1).KeysWherePredicateAsync(Arg.Any<LatticePredicateNode>(), "k1", "k9", true, true);
    }

    [Test]
    public void KeysAsync_with_predicate_rejects_a_null_predicate_or_serializer()
    {
        var lattice = CreateMock();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () =>
                {
                    await foreach (var _ in lattice.KeysAsync<TestItem>(null!, Serializer)) { }
                },
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () =>
                {
                    await foreach (var _ in lattice.KeysAsync(
                        (Expression<Func<TestItem, bool>>)(i => i.Score > 5),
                        (ILatticeSerializer<TestItem>)null!))
                    { }
                },
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    // ── EntriesAsync (predicate) ────────────────────────────────

    [Test]
    public async Task EntriesAsync_with_predicate_and_explicit_serializer_deserializes_each_entry()
    {
        var lattice = CreateMock();
        var alice = new TestItem("alice", 10);
        var bob = new TestItem("bob", 20);
        lattice.EntriesWherePredicateAsync(Arg.Any<LatticePredicateNode>(), null, null, false, null)
            .Returns(new List<KeyValuePair<string, byte[]>>
            {
                new("k1", Bytes(alice)),
                new("k2", Bytes(bob)),
            }.ToAsyncEnumerable());

        var result = new List<KeyValuePair<string, TestItem>>();
        await foreach (var entry in lattice.EntriesAsync<TestItem>(i => i.Score > 5, Serializer))
            result.Add(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2" }));
            Assert.That(result.Select(e => e.Value), Is.EqualTo(new[] { alice, bob }));
        });
        lattice.DidNotReceiveWithAnyArgs().EntriesAsync(default, default, default, default);
    }

    [Test]
    public async Task EntriesAsync_with_predicate_and_default_serializer_deserializes_each_entry()
    {
        var lattice = CreateMock();
        var alice = new TestItem("alice", 10);
        lattice.EntriesWherePredicateAsync(Arg.Any<LatticePredicateNode>(), null, null, false, null)
            .Returns(new List<KeyValuePair<string, byte[]>> { new("k1", Bytes(alice)) }.ToAsyncEnumerable());

        var result = new List<KeyValuePair<string, TestItem>>();
        await foreach (var entry in lattice.EntriesAsync<TestItem>(i => i.Score > 5))
            result.Add(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(1));
            Assert.That(result[0].Value, Is.EqualTo(alice));
        });
        lattice.DidNotReceiveWithAnyArgs().EntriesAsync(default, default, default, default);
    }

    [Test]
    public async Task EntriesAsync_with_predicate_forwards_range_reverse_and_prefetch()
    {
        var lattice = CreateMock();
        var alice = new TestItem("alice", 10);
        lattice.EntriesWherePredicateAsync(Arg.Any<LatticePredicateNode>(), "k1", "k9", true, true)
            .Returns(new List<KeyValuePair<string, byte[]>> { new("k2", Bytes(alice)) }.ToAsyncEnumerable());

        var result = new List<KeyValuePair<string, TestItem>>();
        await foreach (var entry in lattice.EntriesAsync<TestItem>(
            i => i.Score > 5, Serializer, startInclusive: "k1", endExclusive: "k9", reverse: true, prefetch: true))
        {
            result.Add(entry);
        }

        Assert.That(result.Single().Key, Is.EqualTo("k2"));
        lattice.Received(1).EntriesWherePredicateAsync(Arg.Any<LatticePredicateNode>(), "k1", "k9", true, true);
    }

    [Test]
    public void EntriesAsync_with_predicate_rejects_a_null_predicate_or_serializer()
    {
        var lattice = CreateMock();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () =>
                {
                    await foreach (var _ in lattice.EntriesAsync<TestItem>(null!, Serializer)) { }
                },
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () =>
                {
                    await foreach (var _ in lattice.EntriesAsync(
                        (Expression<Func<TestItem, bool>>)(i => i.Score > 5),
                        (ILatticeSerializer<TestItem>)null!))
                    { }
                },
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    // ── ValuesAsync ─────────────────────────────────────────────

    [Test]
    public async Task ValuesAsync_without_a_predicate_projects_the_unfiltered_entry_scan()
    {
        var lattice = CreateMock();
        var alice = new TestItem("alice", 10);
        var bob = new TestItem("bob", 20);
        lattice.EntriesAsync(null, null, false, null)
            .Returns(new List<KeyValuePair<string, byte[]>>
            {
                new("k1", Bytes(alice)),
                new("k2", Bytes(bob)),
            }.ToAsyncEnumerable());

        var result = new List<TestItem>();
        await foreach (var value in lattice.ValuesAsync(Serializer))
            result.Add(value);

        Assert.That(result, Is.EqualTo(new[] { alice, bob }));

        // With no predicate there is nothing to push down, so the filtered entry
        // point must be left alone.
        lattice.DidNotReceiveWithAnyArgs().EntriesWherePredicateAsync(default, default, default, default, default);
    }

    [Test]
    public async Task ValuesAsync_with_a_predicate_projects_the_pushed_down_entry_scan()
    {
        var lattice = CreateMock();
        var bob = new TestItem("bob", 20);
        lattice.EntriesWherePredicateAsync(Arg.Any<LatticePredicateNode>(), null, null, false, null)
            .Returns(new List<KeyValuePair<string, byte[]>> { new("k2", Bytes(bob)) }.ToAsyncEnumerable());

        var result = new List<TestItem>();
        await foreach (var value in lattice.ValuesAsync(Serializer, i => i.Score > 15))
            result.Add(value);

        Assert.That(result, Is.EqualTo(new[] { bob }));
        lattice.DidNotReceiveWithAnyArgs().EntriesAsync(default, default, default, default);
    }

    [Test]
    public async Task ValuesAsync_with_the_default_serializer_projects_the_unfiltered_scan()
    {
        var lattice = CreateMock();
        var alice = new TestItem("alice", 10);
        lattice.EntriesAsync(null, null, false, null)
            .Returns(new List<KeyValuePair<string, byte[]>> { new("k1", Bytes(alice)) }.ToAsyncEnumerable());

        var result = new List<TestItem>();
        await foreach (var value in lattice.ValuesAsync<TestItem>())
            result.Add(value);

        Assert.That(result, Is.EqualTo(new[] { alice }));
    }

    [Test]
    public async Task ValuesAsync_with_the_default_serializer_pushes_a_predicate_down()
    {
        var lattice = CreateMock();
        var bob = new TestItem("bob", 20);
        lattice.EntriesWherePredicateAsync(Arg.Any<LatticePredicateNode>(), null, null, false, null)
            .Returns(new List<KeyValuePair<string, byte[]>> { new("k2", Bytes(bob)) }.ToAsyncEnumerable());

        var result = new List<TestItem>();
        await foreach (var value in lattice.ValuesAsync<TestItem>(i => i.Score > 15))
            result.Add(value);

        Assert.That(result, Is.EqualTo(new[] { bob }));
        lattice.DidNotReceiveWithAnyArgs().EntriesAsync(default, default, default, default);
    }

    [Test]
    public async Task ValuesAsync_forwards_range_reverse_and_prefetch()
    {
        var lattice = CreateMock();
        var alice = new TestItem("alice", 10);
        lattice.EntriesAsync("k1", "k9", true, true)
            .Returns(new List<KeyValuePair<string, byte[]>> { new("k2", Bytes(alice)) }.ToAsyncEnumerable());

        var result = new List<TestItem>();
        await foreach (var value in lattice.ValuesAsync(
            Serializer, predicate: null, startInclusive: "k1", endExclusive: "k9", reverse: true, prefetch: true))
        {
            result.Add(value);
        }

        Assert.That(result, Is.EqualTo(new[] { alice }));
        lattice.Received(1).EntriesAsync("k1", "k9", true, true);
    }

    [Test]
    public void ValuesAsync_rejects_a_null_serializer()
    {
        var lattice = CreateMock();

        Assert.That(
            async () =>
            {
                await foreach (var _ in lattice.ValuesAsync((ILatticeSerializer<TestItem>)null!)) { }
            },
            Throws.InstanceOf<ArgumentNullException>());
    }
}
