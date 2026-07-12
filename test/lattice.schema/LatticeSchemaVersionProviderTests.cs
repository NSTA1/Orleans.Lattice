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
