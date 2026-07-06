using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Integration coverage for the <see cref="ILatticeBackupCatalogStore"/>: manifest
/// register / read / list / remove round-trips over the dogfooded
/// <c>sys-backup-catalog</c> tree, and idempotent re-registration.
/// </summary>
[Category("Integration")]
public sealed class BackupCatalogStoreIntegrationTests
{
    private BackupClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new BackupClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task RegisterAsync_then_GetAsync_round_trips_the_manifest()
    {
        var manifest = BackupManifestModelTests.Sample(id: "cat-1");
        await _fixture.Catalog.RegisterAsync(manifest);

        var readBack = await _fixture.Catalog.GetAsync("cat-1");
        Assert.That(readBack, Is.Not.Null);
        Assert.That(readBack!.Id, Is.EqualTo("cat-1"));
        Assert.That(readBack.Topology.ShardCount, Is.EqualTo(2));
    }

    [Test]
    public async Task ListAsync_returns_every_registered_manifest_in_id_order()
    {
        await _fixture.Catalog.RegisterAsync(BackupManifestModelTests.Sample(id: "cat-charlie"));
        await _fixture.Catalog.RegisterAsync(BackupManifestModelTests.Sample(id: "cat-alpha"));
        await _fixture.Catalog.RegisterAsync(BackupManifestModelTests.Sample(id: "cat-bravo"));

        var ids = await ToListAsync(_fixture.Catalog.ListAsync());
        Assert.That(
            ids.Select(m => m.Id),
            Is.EqualTo(new[] { "cat-alpha", "cat-bravo", "cat-charlie" }));
    }

    [Test]
    public async Task RegisterAsync_is_idempotent_for_the_same_backup_id()
    {
        var manifest = BackupManifestModelTests.Sample(id: "cat-once");
        await _fixture.Catalog.RegisterAsync(manifest);
        await _fixture.Catalog.RegisterAsync(manifest);

        var all = await ToListAsync(_fixture.Catalog.ListAsync());
        Assert.That(all.Select(m => m.Id), Is.EqualTo(new[] { "cat-once" }));
    }

    [Test]
    public async Task RemoveAsync_removes_the_manifest()
    {
        await _fixture.Catalog.RegisterAsync(BackupManifestModelTests.Sample(id: "cat-del"));

        Assert.That(await _fixture.Catalog.RemoveAsync("cat-del"), Is.True);
        Assert.That(await _fixture.Catalog.RemoveAsync("cat-del"), Is.False);
        Assert.That(await _fixture.Catalog.GetAsync("cat-del"), Is.Null);
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
        {
            list.Add(item);
        }

        return list;
    }
}
