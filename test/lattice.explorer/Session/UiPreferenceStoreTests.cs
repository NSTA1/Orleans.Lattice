using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

[TestFixture]
public class UiPreferenceStoreTests
{
    private static readonly TimeSpan Retention = TimeSpan.FromDays(90);

    private static UiPreferenceStore CreateStore(
        IUiPreferenceBackingStore backing,
        MutableTimeProvider? clock = null)
        => new(backing, clock ?? new MutableTimeProvider(), Retention);

    [Test]
    public void IsLoaded_BeforeEnsureLoaded_IsFalse()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());

        Assert.That(store.IsLoaded, Is.False);
    }

    [Test]
    public async Task EnsureLoaded_OverEmptyBacking_SetsLoaded()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());

        await store.EnsureLoadedAsync();

        Assert.That(store.IsLoaded, Is.True);
    }

    [Test]
    public async Task GetOrDefault_UnknownKey_ReturnsFallback()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());
        await store.EnsureLoadedAsync();

        Assert.That(store.GetOrDefault("missing", "fallback"), Is.EqualTo("fallback"));
    }

    [Test]
    public async Task SetAsync_ThenGet_ReturnsStoredValue()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());

        await store.SetAsync("k", "abc");

        Assert.That(store.GetOrDefault("k", string.Empty), Is.EqualTo("abc"));
    }

    [Test]
    public async Task SetAsync_SupportsNonStringValueTypes()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());

        await store.SetAsync("count", 42);

        Assert.That(store.GetOrDefault("count", 0), Is.EqualTo(42));
    }

    [Test]
    public async Task TryGet_WrongType_ReturnsFalse()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());
        await store.SetAsync("k", "abc");

        var found = store.TryGet<int>("k", out var value);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(value, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task SetAsync_PersistsAcrossStoreInstances()
    {
        var backing = new InMemoryUiPreferenceBackingStore();
        var first = CreateStore(backing);
        await first.SetAsync("k", "durable");

        // A fresh store over the same backing hydrates the prior value.
        var second = CreateStore(backing);
        await second.EnsureLoadedAsync();

        Assert.That(second.GetOrDefault("k", string.Empty), Is.EqualTo("durable"));
    }

    [Test]
    public async Task RemoveAsync_DeletesAndPersists()
    {
        var backing = new InMemoryUiPreferenceBackingStore();
        var store = CreateStore(backing);
        await store.SetAsync("k", "abc");

        await store.RemoveAsync("k");

        var rehydrated = CreateStore(backing);
        await rehydrated.EnsureLoadedAsync();
        Assert.Multiple(() =>
        {
            Assert.That(store.TryGet<string>("k", out _), Is.False);
            Assert.That(rehydrated.TryGet<string>("k", out _), Is.False);
        });
    }

    [Test]
    public async Task EnsureLoaded_PrunesEntriesPastRetention()
    {
        var backing = new InMemoryUiPreferenceBackingStore();
        var clock = new MutableTimeProvider();
        var writer = CreateStore(backing, clock);
        await writer.SetAsync("stale", "old");

        // Advance past the retention window and reload in a fresh store.
        clock.Advance(Retention + TimeSpan.FromDays(1));
        var reader = CreateStore(backing, clock);
        await reader.EnsureLoadedAsync();

        Assert.That(reader.TryGet<string>("stale", out _), Is.False);
    }

    [Test]
    public async Task GarbageCollect_DropsEntriesOfDeadOwnersAndKeepsLiveAndOwnerless()
    {
        var store = CreateStore(new InMemoryUiPreferenceBackingStore());
        await store.SetAsync("a", "1", owner: "tree-a");
        await store.SetAsync("b", "2", owner: "tree-b");
        await store.SetAsync("global", "g");

        await store.GarbageCollectAsync(new[] { "tree-a" });

        Assert.Multiple(() =>
        {
            Assert.That(store.GetOrDefault("a", string.Empty), Is.EqualTo("1"));
            Assert.That(store.TryGet<string>("b", out _), Is.False);
            Assert.That(store.GetOrDefault("global", string.Empty), Is.EqualTo("g"));
        });
    }

    [Test]
    public async Task EnsureLoaded_ToleratesUnreachableBacking()
    {
        var store = CreateStore(new ThrowingBackingStore());

        await store.EnsureLoadedAsync();

        // Stays unloaded so a later call can retry, and reads fall back.
        Assert.Multiple(() =>
        {
            Assert.That(store.IsLoaded, Is.False);
            Assert.That(store.GetOrDefault("k", "fallback"), Is.EqualTo("fallback"));
        });
    }

    [Test]
    public async Task SetAsync_RoundTripsCatalogItemAcrossInstances()
    {
        var backing = new InMemoryUiPreferenceBackingStore();
        var item = new CatalogItem
        {
            Id = "tree-42",
            Kind = CatalogKind.TagIndexes,
            IndexName = "by-status",
            ShardCount = 4,
        };

        var writer = CreateStore(backing);
        await writer.SetAsync("nav-selected", item);

        var reader = CreateStore(backing);
        await reader.EnsureLoadedAsync();
        var restored = reader.GetOrDefault<CatalogItem?>("nav-selected", null);

        Assert.That(restored, Is.EqualTo(item));
    }

    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch.AddYears(54);

        public void Advance(TimeSpan delta) => _now = _now.Add(delta);

        public override DateTimeOffset GetUtcNow() => _now;
    }

    private sealed class ThrowingBackingStore : IUiPreferenceBackingStore
    {
        public Task<string?> GetAsync(string key, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("backing unreachable");

        public Task SetAsync(string key, string value, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("backing unreachable");

        public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("backing unreachable");
    }
}
