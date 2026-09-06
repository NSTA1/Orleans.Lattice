using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for <see cref="ILatticeBackupControl.ColdRestoreAsync"/>: the
/// catalog-free disaster-restore entry point on the facade. It reconstructs a tree
/// from the sink when the catalog has been wiped, re-projects the catalog from the
/// sink afterwards, fails closed under a denying gate before touching data, and
/// guards its argument.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlColdRestoreTests
{
    private const string Source = "orders";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ColdRestoreAsync_reconstructs_the_tree_from_the_sink_with_empty_catalog()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var backup = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        // Simulate a lost cluster: wipe the catalog while the sink keeps the backup.
        await ClearCatalogAsync();
        Assert.That(await _fixture.Catalog.GetAsync(backup.BackupId), Is.Null);

        const string target = "orders-cold-facade";
        var result = await _fixture.Control.ColdRestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.EntriesApplied, Is.EqualTo(2));
            Assert.That(Str((await restored.GetAsync("k1"))!), Is.EqualTo("v1"));
            Assert.That(Str((await restored.GetAsync("k2"))!), Is.EqualTo("v2"));
        });
        Assert.That(await _fixture.Catalog.GetAsync(backup.BackupId), Is.Not.Null,
            "the recovered cluster is left with a correct catalog");
    }

    [Test]
    public async Task ColdRestoreAsync_denied_permission_fails_closed_and_writes_nothing()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("denied", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-cold-denied";
        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no restore grant"), membership: null));

        Assert.That(
            async () => await denying.ColdRestoreAsync(new LatticeRestoreRequest(backup.BackupId, target)),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        Assert.That(await restored.GetAsync("k1"), Is.Null);
    }

    [Test]
    public async Task ColdRestoreAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.ColdRestoreAsync(null!),
            Throws.ArgumentNullException);
    }

    private async Task ClearCatalogAsync()
    {
        var ids = new List<string>();
        await foreach (var manifest in _fixture.Catalog.ListAsync())
        {
            ids.Add(manifest.Id);
        }

        foreach (var id in ids)
        {
            await _fixture.Catalog.RemoveAsync(id);
        }
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
