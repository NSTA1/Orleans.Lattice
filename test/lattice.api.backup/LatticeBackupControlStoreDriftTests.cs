using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for selection-time liveness on the listing surfaces: a catalogue row
/// whose sink manifest has gone missing must be pruned from every enumeration
/// rather than offered as a restore point.
/// </summary>
/// <remarks>
/// The catalogue and the sink are separate stores, so an unclean restart or an
/// out-of-band deletion can leave a catalogue row pointing at a manifest the
/// sink can no longer resolve. Both <c>ListBackupsAsync</c> and
/// <c>StreamBackupsAsync</c> carry a presence probe for exactly this, and
/// neither arm had a test: every other fixture writes catalogue and sink
/// together, so the two stores never disagree and the probe always answered
/// "present".
/// </remarks>
[Category("Integration")]
public sealed class LatticeBackupControlStoreDriftTests
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

    /// <summary>
    /// Captures two backups and drops one's manifest from the sink alone, so the
    /// catalogue still lists it but the sink can no longer resolve it - the
    /// store-drift state the liveness probe exists for.
    /// </summary>
    private async Task<(string Live, string Orphaned)> CaptureWithOneOrphanedAsync()
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var first = await CaptureBackupIdAsync(
            new LatticeBackupCaptureRequest("first", BackupScopeSelector.WholeTree(Source)));

        await tree.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        var second = await CaptureBackupIdAsync(
            new LatticeBackupCaptureRequest("second", BackupScopeSelector.WholeTree(Source)));

        // Delete from the SINK only. The catalogue row survives, which is the
        // asymmetry the probe has to notice.
        var deleted = await _fixture.Sink.DeleteManifestAsync(first);
        Assert.That(deleted, Is.True, "The precondition must actually hold.");
        Assert.That(
            await _fixture.Catalog.GetAsync(first), Is.Not.Null,
            "The catalogue row must survive, or the test proves nothing about the probe.");

        return (second, first);
    }

    [Test]
    public async Task ListBackupsAsync_prunes_a_row_whose_sink_manifest_is_gone()
    {
        await _fixture.InitializeAsync();
        var (live, orphaned) = await CaptureWithOneOrphanedAsync();

        var page = await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest());

        var ids = page.Entries.Select(e => e.Id).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(ids, Does.Not.Contain(orphaned),
                "An unresolvable row must not be offered as a restore point.");
            Assert.That(ids, Does.Contain(live),
                "A resolvable row must still be listed; pruning everything would prove nothing.");
        });
    }

    [Test]
    public async Task StreamBackupsAsync_prunes_a_row_whose_sink_manifest_is_gone()
    {
        // The streaming surface carries its own copy of the probe, so it is a
        // separate claim from the paged one above.
        await _fixture.InitializeAsync();
        var (live, orphaned) = await CaptureWithOneOrphanedAsync();

        var streamed = new List<string>();
        await foreach (var manifest in _fixture.Control.StreamBackupsAsync())
        {
            streamed.Add(manifest.Id);
        }

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Does.Not.Contain(orphaned));
            Assert.That(streamed, Does.Contain(live));
        });
    }

    [Test]
    public async Task Both_listing_surfaces_agree_on_which_rows_are_resolvable()
    {
        // The paged and streamed surfaces must not disagree about liveness: a
        // caller that pages and a caller that streams should see the same set.
        await _fixture.InitializeAsync();
        await CaptureWithOneOrphanedAsync();

        var paged = (await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest()))
            .Entries.Select(e => e.Id).OrderBy(id => id, StringComparer.Ordinal).ToArray();

        var streamed = new List<string>();
        await foreach (var manifest in _fixture.Control.StreamBackupsAsync())
        {
            streamed.Add(manifest.Id);
        }

        Assert.That(
            streamed.OrderBy(id => id, StringComparer.Ordinal).ToArray(),
            Is.EqualTo(paged));
    }

    private ILatticeBackupOperations Operations => (ILatticeBackupOperations)_fixture.Control;

    private static async Task<LatticeOperationStatus> UntilTerminalAsync(ILatticeBackupOperations operations, string operationId)
    {
        LatticeOperationStatus? status = null;
        await Orleans.Lattice.Testing.TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    private async Task<string> CaptureBackupIdAsync(LatticeBackupCaptureRequest request)
    {
        var handle = await Operations.StartBackupAsync(request);
        var status = await UntilTerminalAsync(Operations, handle.OperationId);
        return status.ResultReference!;
    }
}
