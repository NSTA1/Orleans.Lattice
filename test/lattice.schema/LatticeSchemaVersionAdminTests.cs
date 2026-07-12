using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionAdmin"/>: set / get / clear
/// delegation, monotonic target-version advance guard, and provider-cache
/// invalidation on every mutation.
/// </summary>
public sealed class LatticeSchemaVersionAdminTests
{
    private static (LatticeSchemaVersionAdmin Admin, ILatticeSchemaVersionStore Store, ILatticeSchemaVersionProvider Provider)
        Create()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        var grainFactory = Substitute.For<IGrainFactory>();
        return (new LatticeSchemaVersionAdmin(store, provider, grainFactory), store, provider);
    }

    private static (LatticeSchemaVersionAdmin Admin, ILatticeSchemaVersionStore Store, ILatticeSchemaVersionProvider Provider, ILatticeSchemaRemediationGrain Grain)
        CreateWithGrain()
    {
        var store = Substitute.For<ILatticeSchemaVersionStore>();
        var provider = Substitute.For<ILatticeSchemaVersionProvider>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<ILatticeSchemaRemediationGrain>();
        grainFactory.GetGrain<ILatticeSchemaRemediationGrain>("orders").Returns(grain);
        return (new LatticeSchemaVersionAdmin(store, provider, grainFactory), store, provider, grain);
    }

    [Test]
    public async Task SetVersionConfigAsync_writes_store_and_invalidates_cache()
    {
        var (admin, store, provider) = Create();
        var config = new LatticeSchemaVersionConfig(1, 1);

        await admin.SetVersionConfigAsync("orders", config);

        await store.Received(1).SetConfigAsync("orders", config, Arg.Any<CancellationToken>());
        provider.Received(1).Invalidate("orders");
    }

    [Test]
    public async Task GetVersionConfigAsync_reads_from_store()
    {
        var (admin, store, _) = Create();
        var config = new LatticeSchemaVersionConfig(2, 3);
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(config);

        var result = await admin.GetVersionConfigAsync("orders");

        Assert.That(result, Is.EqualTo(config));
    }

    [Test]
    public async Task AdvanceTargetVersionAsync_advances_and_persists()
    {
        var (admin, store, provider) = Create();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(schemaId: 7, targetVersion: 2));

        var advanced = await admin.AdvanceTargetVersionAsync("orders", 5);

        Assert.That(advanced.TargetVersion, Is.EqualTo(5u));
        Assert.That(advanced.SchemaId, Is.EqualTo(7u));
        await store.Received(1).SetConfigAsync(
            "orders",
            Arg.Is<LatticeSchemaVersionConfig>(c => c.TargetVersion == 5 && c.SchemaId == 7),
            Arg.Any<CancellationToken>());
        provider.Received(1).Invalidate("orders");
    }

    [Test]
    public void AdvanceTargetVersionAsync_non_advancing_throws()
    {
        var (admin, store, _) = Create();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(1, targetVersion: 3));

        Assert.That(
            async () => await admin.AdvanceTargetVersionAsync("orders", 3),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(
            async () => await admin.AdvanceTargetVersionAsync("orders", 2),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void AdvanceTargetVersionAsync_unversioned_tree_throws()
    {
        var (admin, store, _) = Create();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);

        Assert.That(
            async () => await admin.AdvanceTargetVersionAsync("orders", 2),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task ClearVersionConfigAsync_removes_and_invalidates()
    {
        var (admin, store, provider) = Create();
        store.ClearConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(true);

        var removed = await admin.ClearVersionConfigAsync("orders");

        Assert.That(removed, Is.True);
        provider.Received(1).Invalidate("orders");
    }

    [Test]
    public void Methods_reject_null_or_empty_tree()
    {
        var (admin, _, _) = Create();
        var config = new LatticeSchemaVersionConfig(1, 1);

        Assert.That(async () => await admin.SetVersionConfigAsync(null!, config), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await admin.GetVersionConfigAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await admin.AdvanceTargetVersionAsync(null!, 2), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await admin.ClearVersionConfigAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await admin.AdvanceAndMigrateAsync(null!, 2), Throws.InstanceOf<ArgumentException>());
        Assert.That(async () => await admin.MigrateToTargetVersionAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task AdvanceAndMigrateAsync_advances_config_then_drives_migration_to_new_target()
    {
        var (admin, store, provider, grain) = CreateWithGrain();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(schemaId: 7, targetVersion: 2));
        var terminal = LatticeSchemaRemediationReport.Completed(3, "orders/remediated/op", "op");
        grain.StartVersionMigrationAsync(7, 5, Arg.Any<CancellationToken>()).Returns(terminal);

        var report = await admin.AdvanceAndMigrateAsync("orders", 5);

        // Config advanced and cache invalidated (the lazy path), then the eager
        // migration re-stamped to the newly advanced target.
        await store.Received(1).SetConfigAsync(
            "orders",
            Arg.Is<LatticeSchemaVersionConfig>(c => c.TargetVersion == 5 && c.SchemaId == 7),
            Arg.Any<CancellationToken>());
        provider.Received(1).Invalidate("orders");
        await grain.Received(1).StartVersionMigrationAsync(7, 5, Arg.Any<CancellationToken>());
        Assert.That(report, Is.EqualTo(terminal));
    }

    [Test]
    public void AdvanceAndMigrateAsync_non_advancing_target_throws()
    {
        var (admin, store, _, _) = CreateWithGrain();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(7, targetVersion: 3));

        Assert.That(
            async () => await admin.AdvanceAndMigrateAsync("orders", 3),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void AdvanceAndMigrateAsync_reserved_tree_throws()
    {
        var (admin, _, _, _) = CreateWithGrain();

        Assert.That(
            async () => await admin.AdvanceAndMigrateAsync("sys-schema-orders", 5),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task MigrateToTargetVersionAsync_drives_migration_to_current_target()
    {
        var (admin, store, _, grain) = CreateWithGrain();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new LatticeSchemaVersionConfig(schemaId: 7, targetVersion: 4));
        var terminal = LatticeSchemaRemediationReport.Completed(2, "orders/remediated/op", "op");
        grain.StartVersionMigrationAsync(7, 4, Arg.Any<CancellationToken>()).Returns(terminal);

        var report = await admin.MigrateToTargetVersionAsync("orders");

        // No config advance: re-stamp to the tree's current target.
        await store.DidNotReceive().SetConfigAsync(
            Arg.Any<string>(), Arg.Any<LatticeSchemaVersionConfig>(), Arg.Any<CancellationToken>());
        await grain.Received(1).StartVersionMigrationAsync(7, 4, Arg.Any<CancellationToken>());
        Assert.That(report, Is.EqualTo(terminal));
    }

    [Test]
    public void MigrateToTargetVersionAsync_unversioned_tree_throws()
    {
        var (admin, store, _, _) = CreateWithGrain();
        store.GetConfigAsync("orders", Arg.Any<CancellationToken>())
            .Returns((LatticeSchemaVersionConfig?)null);

        Assert.That(
            async () => await admin.MigrateToTargetVersionAsync("orders"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void MigrateToTargetVersionAsync_reserved_tree_throws()
    {
        var (admin, _, _, _) = CreateWithGrain();

        Assert.That(
            async () => await admin.MigrateToTargetVersionAsync("sys-schema-orders"),
            Throws.InstanceOf<ArgumentException>());
    }
}
