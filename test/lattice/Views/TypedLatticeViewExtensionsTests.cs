using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for the typed read helpers in <see cref="TypedLatticeViewExtensions"/>.</summary>
[TestFixture]
public class TypedLatticeViewExtensionsTests
{
    private sealed record Person(string Name, int Age);

    [Test]
    public async Task GetAsync_typed_deserializes_with_the_default_serializer()
    {
        var view = new StubView();
        await view.SeedAsync("alice", JsonLatticeSerializer<Person>.Default.Serialize(new Person("Alice", 30)));

        var person = await view.GetAsync<Person>("alice");

        Assert.That(person, Is.EqualTo(new Person("Alice", 30)));
    }

    [Test]
    public async Task GetAsync_typed_returns_default_for_a_missing_key()
    {
        var view = new StubView();

        var person = await view.GetAsync<Person>("absent");

        Assert.That(person, Is.Null);
    }

    [Test]
    public async Task GetAsync_typed_honours_a_custom_serializer()
    {
        var view = new StubView();
        var serializer = new PipePersonSerializer();
        await view.SeedAsync("bob", serializer.Serialize(new Person("Bob", 41)));

        var person = await view.GetAsync("bob", serializer);

        Assert.That(person, Is.EqualTo(new Person("Bob", 41)));
    }

    [Test]
    public async Task EntriesAsync_typed_streams_deserialized_values_in_key_order()
    {
        var view = new StubView();
        await view.SeedAsync("a", JsonLatticeSerializer<Person>.Default.Serialize(new Person("Amy", 20)));
        await view.SeedAsync("b", JsonLatticeSerializer<Person>.Default.Serialize(new Person("Ben", 25)));

        var entries = new List<KeyValuePair<string, Person>>();
        await foreach (var entry in view.EntriesAsync<Person>())
        {
            entries.Add(entry);
        }

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
        Assert.That(entries[0].Value, Is.EqualTo(new Person("Amy", 20)));
        Assert.That(entries[1].Value, Is.EqualTo(new Person("Ben", 25)));
    }

    [Test]
    public async Task GetAggregateDoubleAsync_decodes_a_double_aggregate()
    {
        var view = new StubView();
        await view.SeedAsync("Alice", LatticeAggregationValue.EncodeDouble(15.5));

        Assert.That(await view.GetAggregateDoubleAsync("Alice"), Is.EqualTo(15.5));
    }

    [Test]
    public async Task GetAggregateDoubleAsync_returns_null_for_an_empty_group()
    {
        var view = new StubView();

        Assert.That(await view.GetAggregateDoubleAsync("absent"), Is.Null);
    }

    [Test]
    public async Task GetAggregateInt64Async_decodes_an_int64_aggregate()
    {
        var view = new StubView();
        await view.SeedAsync("Bob", LatticeAggregationValue.EncodeInt64(7));

        Assert.That(await view.GetAggregateInt64Async("Bob"), Is.EqualTo(7));
    }

    private sealed class PipePersonSerializer : ILatticeSerializer<Person>
    {
        public byte[] Serialize(Person value) =>
            System.Text.Encoding.UTF8.GetBytes($"{value.Name}|{value.Age}");

        public Person Deserialize(byte[] bytes)
        {
            var parts = System.Text.Encoding.UTF8.GetString(bytes).Split('|');
            return new Person(parts[0], int.Parse(parts[1]));
        }
    }

    /// <summary>Minimal in-memory <see cref="ILatticeView"/> backing the read-helper tests.</summary>
    private sealed class StubView : ILatticeView
    {
        private readonly SortedDictionary<string, byte[]> _store = new(StringComparer.Ordinal);

        public string ViewName => "stub";

        public Task SeedAsync(string key, byte[] value)
        {
            _store[key] = value;
            return Task.CompletedTask;
        }

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(_store.TryGetValue(key, out var value) ? value : null);

        public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(_store.Count);

        public async IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var key in _store.Keys)
            {
                yield return key;
            }

            await Task.CompletedTask;
        }

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var entry in _store)
            {
                if (startInclusive is not null && string.CompareOrdinal(entry.Key, startInclusive) < 0)
                {
                    continue;
                }

                if (endExclusive is not null && string.CompareOrdinal(entry.Key, endExclusive) >= 0)
                {
                    continue;
                }

                yield return entry;
            }

            await Task.CompletedTask;
        }

        public Task<long> GetLagAsync(CancellationToken cancellationToken = default) => Task.FromResult(0L);

        public Task RebuildAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<bool> ReconcileAsync(CancellationToken cancellationToken = default) => Task.FromResult(false);

        public Task<ViewDigest> ComputeDigestAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
