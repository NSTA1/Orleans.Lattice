using NSubstitute;
using System.Text.Json;

namespace Orleans.Lattice.Tests;

public class TypedLatticeExtensionsTests
{
    private record TestItem(string Name, int Score);

    private static readonly ILatticeSerializer<TestItem> Serializer = JsonLatticeSerializer<TestItem>.Default;

    private static ILattice CreateMock() => Substitute.For<ILattice>();

    // ── GetAsync ────────────────────────────────────────────────

    [Test]
    public async Task GetAsync_deserializes_value()
    {
        var lattice = CreateMock();
        var item = new TestItem("alice", 10);
        lattice.GetAsync("k1").Returns(JsonSerializer.SerializeToUtf8Bytes(item));

        var result = await lattice.GetAsync<TestItem>("k1", Serializer);

        Assert.That(result, Is.EqualTo(item));
    }

    [Test]
    public async Task GetAsync_returns_default_for_missing_key()
    {
        var lattice = CreateMock();
        lattice.GetAsync("k1").Returns(Task.FromResult<byte[]?>(null));

        var result = await lattice.GetAsync<TestItem>("k1", Serializer);

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task GetAsync_returns_zero_for_missing_value_type_key()
    {
        var lattice = CreateMock();
        lattice.GetAsync("k1").Returns(Task.FromResult<byte[]?>(null));

        var result = await lattice.GetAsync<int>("k1");

        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public async Task GetAsync_default_serializer_roundtrips()
    {
        var lattice = CreateMock();
        var item = new TestItem("bob", 20);
        lattice.GetAsync("k1").Returns(JsonSerializer.SerializeToUtf8Bytes(item));

        var result = await lattice.GetAsync<TestItem>("k1");

        Assert.That(result, Is.EqualTo(item));
    }

    [Test]
    public void GetAsync_throws_for_null_serializer()
    {
        var lattice = CreateMock();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => lattice.GetAsync<TestItem>("k1", null!));
    }

    // ── SetAsync ────────────────────────────────────────────────

    [Test]
    public async Task SetAsync_serializes_value()
    {
        var lattice = CreateMock();
        var item = new TestItem("carol", 30);

        await lattice.SetAsync("k1", item, Serializer);

        await lattice.Received(1).SetAsync("k1",
            Arg.Is<byte[]>(b => JsonSerializer.Deserialize<TestItem>(b)! == item));
    }

    [Test]
    public async Task SetAsync_default_serializer_works()
    {
        var lattice = CreateMock();

        await lattice.SetAsync("k1", new TestItem("dave", 40));

        await lattice.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public void SetAsync_throws_for_null_serializer()
    {
        var lattice = CreateMock();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => lattice.SetAsync("k1", new TestItem("x", 0), null!));
    }

    // ── GetManyAsync ────────────────────────────────────────────

    [Test]
    public async Task GetManyAsync_deserializes_all_values()
    {
        var lattice = CreateMock();
        var items = new Dictionary<string, byte[]>
        {
            ["a"] = JsonSerializer.SerializeToUtf8Bytes(new TestItem("a", 1)),
            ["b"] = JsonSerializer.SerializeToUtf8Bytes(new TestItem("b", 2)),
        };
        lattice.GetManyAsync(Arg.Any<List<string>>()).Returns(items);

        var result = await lattice.GetManyAsync<TestItem>(["a", "b"], Serializer);

        Assert.That(result, Has.Count.EqualTo(2));
        Assert.That(result["a"], Is.EqualTo(new TestItem("a", 1)));
        Assert.That(result["b"], Is.EqualTo(new TestItem("b", 2)));
    }

    [Test]
    public async Task GetManyAsync_returns_empty_when_no_matches()
    {
        var lattice = CreateMock();
        lattice.GetManyAsync(Arg.Any<List<string>>()).Returns(new Dictionary<string, byte[]>());
        var result = await lattice.GetManyAsync<TestItem>(["x"], Serializer);

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void GetManyAsync_throws_for_null_serializer()
    {
        var lattice = CreateMock();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => lattice.GetManyAsync<TestItem>(["a"], null!));
    }

    // ── SetManyAsync ────────────────────────────────────────────

    [Test]
    public async Task SetManyAsync_serializes_all_entries()
    {
        var lattice = CreateMock();
        var entries = new List<KeyValuePair<string, TestItem>>
        {
            new("a", new TestItem("a", 1)),
            new("b", new TestItem("b", 2)),
        };

        await lattice.SetManyAsync(entries, Serializer);

        await lattice.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(l => l.Count == 2));
    }

    [Test]
    public async Task SetManyAsync_default_serializer_works()
    {
        var lattice = CreateMock();
        var entries = new List<KeyValuePair<string, TestItem>>
        {
            new("a", new TestItem("a", 1)),
        };

        await lattice.SetManyAsync(entries);

        await lattice.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(l => l.Count == 1));
    }

    [Test]
    public void SetManyAsync_throws_for_null_serializer()
    {
        var lattice = CreateMock();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => lattice.SetManyAsync(new List<KeyValuePair<string, TestItem>>(), null!));
    }

    [Test]
    public async Task SetManyAsync_empty_list_delegates_empty()
    {
        var lattice = CreateMock();

        await lattice.SetManyAsync(new List<KeyValuePair<string, TestItem>>(), Serializer);

        await lattice.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(l => l.Count == 0));
    }

    // ── BulkLoadAsync ───────────────────────────────────────────

    [Test]
    public async Task BulkLoadAsync_serializes_all_entries()
    {
        var lattice = CreateMock();
        var entries = new List<KeyValuePair<string, TestItem>>
        {
            new("a", new TestItem("a", 1)),
            new("b", new TestItem("b", 2)),
            new("c", new TestItem("c", 3)),
        };

        await lattice.BulkLoadAsync<TestItem>(entries, Serializer);

        await lattice.Received(1).BulkLoadAsync(
            Arg.Is<IReadOnlyList<KeyValuePair<string, byte[]>>>(l => l.Count == 3));
    }

    [Test]
    public async Task BulkLoadAsync_default_serializer_works()
    {
        var lattice = CreateMock();
        var entries = new List<KeyValuePair<string, TestItem>>
        {
            new("a", new TestItem("a", 1)),
        };

        await lattice.BulkLoadAsync<TestItem>(entries);

        await lattice.Received(1).BulkLoadAsync(
            Arg.Is<IReadOnlyList<KeyValuePair<string, byte[]>>>(l => l.Count == 1));
    }

    [Test]
    public void BulkLoadAsync_throws_for_null_serializer()
    {
        var lattice = CreateMock();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => lattice.BulkLoadAsync(new List<KeyValuePair<string, TestItem>>(), null!));
    }

    [Test]
    public async Task BulkLoadAsync_empty_list_delegates_empty()
    {
        var lattice = CreateMock();

        await lattice.BulkLoadAsync(new List<KeyValuePair<string, TestItem>>(), Serializer);

        await lattice.Received(1).BulkLoadAsync(
            Arg.Is<IReadOnlyList<KeyValuePair<string, byte[]>>>(l => l.Count == 0));
    }

    // ── EntriesAsync ────────────────────────────────────────────

    [Test]
    public async Task EntriesAsync_deserializes_values_with_explicit_serializer()
    {
        var lattice = CreateMock();
        var item1 = new TestItem("alice", 10);
        var item2 = new TestItem("bob", 20);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", JsonSerializer.SerializeToUtf8Bytes(item1)),
            new("k2", JsonSerializer.SerializeToUtf8Bytes(item2)),
        };
        lattice.EntriesAsync(null, null, false)
            .Returns(entries.ToAsyncEnumerable());

        var result = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in lattice.EntriesAsync(Serializer))
            result.Add(e);

        Assert.That(result, Has.Count.EqualTo(2));
        Assert.That(result[0].Key, Is.EqualTo("k1"));
        Assert.That(result[0].Value, Is.EqualTo(item1));
        Assert.That(result[1].Key, Is.EqualTo("k2"));
        Assert.That(result[1].Value, Is.EqualTo(item2));
    }

    [Test]
    public async Task EntriesAsync_deserializes_values_with_default_serializer()
    {
        var lattice = CreateMock();
        var item = new TestItem("alice", 10);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", JsonSerializer.SerializeToUtf8Bytes(item)),
        };
        lattice.EntriesAsync(null, null, false)
            .Returns(entries.ToAsyncEnumerable());

        var result = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in lattice.EntriesAsync<TestItem>())
            result.Add(e);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].Value, Is.EqualTo(item));
    }

    [Test]
    public async Task EntriesAsync_empty_returns_nothing()
    {
        var lattice = CreateMock();
        lattice.EntriesAsync(null, null, false)
            .Returns(AsyncEnumerable.Empty<KeyValuePair<string, byte[]>>());

        var result = new List<KeyValuePair<string, TestItem>>();
        await foreach (var e in lattice.EntriesAsync<TestItem>(Serializer))
            result.Add(e);

        Assert.That(result, Is.Empty);
    }

    // ── Custom Serializer ───────────────────────────────────────

    [Test]
    public async Task Custom_serializer_is_used()
    {
        var lattice = CreateMock();
        var item = new TestItem("test", 99);
        var custom = new TrackingSerializer();
        var serializedBytes = custom.Serialize(item);
        custom.ResetCounts();
        lattice.GetAsync("k1").Returns(Task.FromResult<byte[]?>(serializedBytes));

        await lattice.SetAsync("k1", item, custom);
        var result = await lattice.GetAsync("k1", custom);

        Assert.That(custom.SerializeCount, Is.EqualTo(1));
        Assert.That(custom.DeserializeCount, Is.EqualTo(1));
        Assert.That(result, Is.EqualTo(item));
    }

    private sealed class TrackingSerializer : ILatticeSerializer<TestItem>
    {
        private readonly JsonLatticeSerializer<TestItem> _inner = JsonLatticeSerializer<TestItem>.Default;
        public int SerializeCount { get; private set; }
        public int DeserializeCount { get; private set; }

        public void ResetCounts() { SerializeCount = 0; DeserializeCount = 0; }
        public byte[] Serialize(TestItem value) { SerializeCount++; return _inner.Serialize(value); }
        public TestItem Deserialize(byte[] bytes) { DeserializeCount++; return _inner.Deserialize(bytes); }
    }
}
