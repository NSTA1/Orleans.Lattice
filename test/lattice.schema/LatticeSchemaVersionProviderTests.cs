using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionProvider"/>: cached resolution,
/// the unversioned-tree null sentinel, reserved-tree short-circuit, cache
/// invalidation (explicit and via the mutation observer), and the strict flag.
/// Mirrors <c>LatticeSchemaPolicyProviderTests</c>.
/// </summary>
public sealed class LatticeSchemaVersionProviderTests
{
    private static LatticeSchemaVersionProvider CreateProvider(
        ILatticeSchemaVersionStore store, bool strict = false)
    {
        var options = Options.Create(new LatticeSchemaVersioningOptions { StrictIngest = strict });
        return new LatticeSchemaVersionProvider(store, options);
    }

    private static LatticeSchemaVersionConfig Config() => new(schemaId: 1, targetVersion: 1);

    [Test]
    public async Task GetConfigAsync_versioned_tree_returns_config()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        var config = await provider.GetConfigAsync("orders");

        Assert.That(config, Is.Not.Null);
        Assert.That(config!.Value.SchemaId, Is.EqualTo(1u));
    }

    [Test]
    public async Task GetConfigAsync_distinct_tree_ids_do_not_grow_cache_without_bound()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);
        var provider = CreateProvider(store);

        // Every miss caches a null sentinel keyed by a caller-supplied tree id, so
        // an unbounded cache let a caller grow silo memory without limit (CWE-770).
        for (var i = 0; i < SchemaTreeCacheExtensions.MaxCachedTrees + 500; i++)
        {
            Assert.That(await provider.GetConfigAsync("tree-" + i), Is.Null);
        }

        Assert.That(
            provider.CachedTreeCount,
            Is.LessThanOrEqualTo(SchemaTreeCacheExtensions.MaxCachedTrees));
    }

    [Test]
    public async Task GetConfigAsync_still_resolves_correctly_once_the_cache_is_full()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        for (var i = 0; i < SchemaTreeCacheExtensions.MaxCachedTrees + 10; i++)
        {
            _ = await provider.GetConfigAsync("filler-" + i);
        }

        // The bound must never change an answer, only cost an extra round-trip.
        var config = await provider.GetConfigAsync("orders");

        Assert.That(config, Is.Not.Null);
        Assert.That(config!.Value.SchemaId, Is.EqualTo(1u));
    }

    [Test]
    public async Task GetConfigAsync_unversioned_tree_returns_null()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);
        var provider = CreateProvider(store);

        Assert.That(await provider.GetConfigAsync("orders"), Is.Null);
    }

    [Test]
    public async Task GetConfigAsync_caches_after_first_load()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        _ = await provider.GetConfigAsync("orders");
        _ = await provider.GetConfigAsync("orders");

        await store.Received(1).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetConfigAsync_cached_reads_and_unrelated_mutations_do_not_allocate()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);
        _ = await provider.GetConfigAsync("orders");
        var mutation = new LatticeMutation { TreeId = "orders", Key = "some-key" };

        for (var i = 0; i < 100; i++)
        {
            _ = provider.GetConfigAsync("orders").GetAwaiter().GetResult();
            provider.OnMutationAsync(mutation, CancellationToken.None).GetAwaiter().GetResult();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            _ = provider.GetConfigAsync("orders").GetAwaiter().GetResult();
            provider.OnMutationAsync(mutation, CancellationToken.None).GetAwaiter().GetResult();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.Zero);
        await store.Received(1).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetConfigAsync_caches_null_sentinel_for_unversioned_tree()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);
        var provider = CreateProvider(store);

        _ = await provider.GetConfigAsync("orders");
        _ = await provider.GetConfigAsync("orders");

        await store.Received(1).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetConfigAsync_reserved_tree_short_circuits_without_store()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        var provider = CreateProvider(store);

        Assert.That(await provider.GetConfigAsync("sys-schema-version"), Is.Null);
        await store.DidNotReceive().GetConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetConfigAsync_null_tree_throws()
    {
        var provider = CreateProvider(Substitute.For<ILatticeSchemaVersionStore>());

        Assert.That(async () => await provider.GetConfigAsync(null!), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await provider.GetConfigAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task Invalidate_forces_reload_on_next_get()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        _ = await provider.GetConfigAsync("orders");
        provider.Invalidate("orders");
        _ = await provider.GetConfigAsync("orders");

        await store.Received(2).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetConfigAsync_load_overtaken_by_invalidate_does_not_cache_stale_config()
    {
        // The store read blocks; a config change commits and invalidates while it
        // is in flight; the read then returns the pre-change answer.
        var blockedRead = new TaskCompletionSource<LatticeSchemaVersionConfig?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(blockedRead.Task, Task.FromResult<LatticeSchemaVersionConfig?>(Config()));
        var provider = CreateProvider(store);

        var inFlight = provider.GetConfigAsync("orders").AsTask();
        provider.Invalidate("orders");
        blockedRead.SetResult(null);
        Assert.That(await inFlight, Is.Null);

        // The overtaken load must not install its pre-change answer: the next
        // lookup re-reads the store and sees the new config.
        var config = await provider.GetConfigAsync("orders");

        Assert.That(config, Is.Not.Null);
        await store.Received(2).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetConfigAsync_load_overtaken_by_version_mutation_does_not_cache_stale_config()
    {
        var blockedRead = new TaskCompletionSource<LatticeSchemaVersionConfig?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(blockedRead.Task, Task.FromResult<LatticeSchemaVersionConfig?>(Config()));
        var provider = CreateProvider(store);

        var inFlight = provider.GetConfigAsync("orders").AsTask();
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "sys-schema-version", Key = "orders" }, CancellationToken.None);
        blockedRead.SetResult(null);
        Assert.That(await inFlight, Is.Null);

        var config = await provider.GetConfigAsync("orders");

        Assert.That(config, Is.Not.Null);
        await store.Received(2).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetConfigAsync_load_not_overtaken_is_cached()
    {
        var blockedRead = new TaskCompletionSource<LatticeSchemaVersionConfig?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(blockedRead.Task);
        var provider = CreateProvider(store);

        var inFlight = provider.GetConfigAsync("orders").AsTask();
        blockedRead.SetResult(Config());
        _ = await inFlight;
        _ = await provider.GetConfigAsync("orders");

        await store.Received(1).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InvalidateAll_forces_reload_of_every_tree_and_overtakes_an_in_flight_load()
    {
        var blockedRead = new TaskCompletionSource<LatticeSchemaVersionConfig?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        store.GetConfigAsync("invoices", Arg.Any<CancellationToken>())
            .Returns(blockedRead.Task, Task.FromResult<LatticeSchemaVersionConfig?>(Config()));
        var provider = CreateProvider(store);
        _ = await provider.GetConfigAsync("orders");

        var inFlight = provider.GetConfigAsync("invoices").AsTask();
        provider.InvalidateAll();
        blockedRead.SetResult(null);
        _ = await inFlight;

        Assert.That(await provider.GetConfigAsync("invoices"), Is.Not.Null);
        _ = await provider.GetConfigAsync("orders");
        await store.Received(2).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Invalidate_null_tree_throws()
    {
        var provider = CreateProvider(Substitute.For<ILatticeSchemaVersionStore>());

        Assert.That(() => provider.Invalidate(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task OnMutationAsync_version_tree_write_evicts_affected_tree()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        _ = await provider.GetConfigAsync("orders");
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "sys-schema-version", Key = "orders" }, CancellationToken.None);
        _ = await provider.GetConfigAsync("orders");

        await store.Received(2).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_version_tree_write_with_empty_key_does_not_evict()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        _ = await provider.GetConfigAsync("orders");
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "sys-schema-version", Key = string.Empty }, CancellationToken.None);
        _ = await provider.GetConfigAsync("orders");

        await store.Received(1).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OnMutationAsync_unrelated_tree_write_does_not_evict()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(Config());
        var provider = CreateProvider(store);

        _ = await provider.GetConfigAsync("orders");
        await provider.OnMutationAsync(
            new LatticeMutation { TreeId = "orders", Key = "some-key" }, CancellationToken.None);
        _ = await provider.GetConfigAsync("orders");

        await store.Received(1).GetConfigAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public void StrictIngestEnabled_reflects_options()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        Assert.That(CreateProvider(store, strict: true).StrictIngestEnabled, Is.True);
        Assert.That(CreateProvider(store, strict: false).StrictIngestEnabled, Is.False);
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        Assert.That(
            () => new LatticeSchemaVersionProvider(null!, Options.Create(new LatticeSchemaVersioningOptions())),
            Throws.ArgumentNullException);
        Assert.That(
            () => new LatticeSchemaVersionProvider(store, null!),
            Throws.ArgumentNullException);
    }
}
