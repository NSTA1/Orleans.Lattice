using NSubstitute;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

[TestFixture]
public class UiPreferenceStoreLifecycleTests
{
    [TestCase("set")]
    [TestCase("remove")]
    [TestCase("collect")]
    public async Task Mutation_after_disposal_does_not_change_preferences(string operation)
    {
        var backing = new InMemoryUiPreferenceBackingStore();
        var store = new UiPreferenceStore(backing);
        await store.SetAsync("k", 42, owner: "tree");
        var original = await backing.GetAsync(UiPreferenceStore.BackingKey);
        store.Dispose();

        await MutateAsync(store, operation);

        Assert.That(store.GetOrDefault("k", 0), Is.EqualTo(42));
        Assert.That(await backing.GetAsync(UiPreferenceStore.BackingKey), Is.EqualTo(original));
    }

    [Test]
    public async Task EnsureLoaded_disposal_during_read_does_not_hydrate_or_prune_storage()
    {
        var backing = Substitute.For<IUiPreferenceBackingStore>();
        var read = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        backing.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(read.Task);
        var store = new UiPreferenceStore(backing);
        var loading = store.EnsureLoadedAsync();
        store.Dispose();
        read.SetResult("""{"old":{"Json":"42","TouchedUnixMs":0}}""");

        await loading;

        Assert.That(store.IsLoaded, Is.False);
        await backing.DidNotReceive().RemoveAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await backing.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [TestCase("set")]
    [TestCase("remove")]
    [TestCase("collect")]
    public async Task Mutation_queued_before_disposal_does_not_write_after_hydration(string operation)
    {
        var backing = Substitute.For<IUiPreferenceBackingStore>();
        var read = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        backing.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(read.Task);
        var store = new UiPreferenceStore(backing);
        var loading = store.EnsureLoadedAsync();
        var mutation = MutateAsync(store, operation);
        store.Dispose();
        read.SetResult(null);

        await Task.WhenAll(loading, mutation);

        Assert.That(store.TryGet<int>("k", out _), Is.False);
        await backing.DidNotReceive().RemoveAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await backing.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    private static Task MutateAsync(UiPreferenceStore store, string operation) => operation switch
    {
        "set" => store.SetAsync("k", 7),
        "remove" => store.RemoveAsync("k"),
        "collect" => store.GarbageCollectAsync(Array.Empty<string>()),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };
}
