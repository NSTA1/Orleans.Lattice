using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for the two <c>LatticeBackupControl</c> arms that only a
/// <i>multi-entry</i> catalogue can reach: walking a base chain past its first
/// link, and skipping a catalogue row belonging to a different scope.
/// </summary>
/// <remarks>
/// <c>ComputeScopeChainDepthAsync</c> folds the catalogue twice - once to find
/// the newest manifest matching the requested scope, then again to walk that
/// manifest's <c>BaseBackupId</c> ancestry. Every existing fixture captures a
/// single full backup for a single scope, so the fold never skipped a row and
/// the ancestry walk always stopped at its first link. Both arms need a
/// catalogue holding more than one scope, and a chain longer than one backup.
/// </remarks>
[Category("Integration")]
public sealed class LatticeBackupControlChainDepthTests
{
    private const string Source = "orders";
    private const string Other = "invoices";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task SeedAsync(string treeId, string key, string value) =>
        await _fixture.GrainFactory.GetGrain<ILattice>(treeId).SetAsync(key, Bytes(value));

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

    private async Task<string> CaptureBackupIdAsync(LatticeBackupIncrementalCaptureRequest request)
    {
        var handle = await Operations.StartIncrementalBackupAsync(request);
        var status = await UntilTerminalAsync(Operations, handle.OperationId);
        return status.ResultReference!;
    }

    [Test]
    public async Task GetScopeStatusAsync_walks_the_whole_base_chain_not_just_its_first_link()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        await SeedAsync(Source, "k1", "v1");

        var full = await CaptureBackupIdAsync(new LatticeBackupCaptureRequest("full", scope));

        await SeedAsync(Source, "k2", "v2");
        var first = await CaptureBackupIdAsync(
            new LatticeBackupIncrementalCaptureRequest("incr-1", scope, full));

        await SeedAsync(Source, "k3", "v3");
        await CaptureBackupIdAsync(
            new LatticeBackupIncrementalCaptureRequest("incr-2", scope, first));

        var status = await _fixture.Control.GetScopeStatusAsync(scope);

        // Three links: incr-2 -> incr-1 -> full. A walk that stopped at the first
        // ancestor would report 2, and one that never stepped at all would report 1.
        Assert.That(status, Is.Not.Null);
        Assert.That(status!.ChainDepth, Is.EqualTo(3),
            "The chain depth must count every ancestor the catalogue can resolve.");
    }

    [Test]
    public async Task GetScopeStatusAsync_reports_depth_one_for_a_standalone_full_backup()
    {
        // The lower bound of the walk, and the control that stops the assertion
        // above passing for a depth that simply counts catalogue rows.
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        await SeedAsync(Source, "k1", "v1");

        await CaptureBackupIdAsync(new LatticeBackupCaptureRequest("full", scope));

        var status = await _fixture.Control.GetScopeStatusAsync(scope);

        Assert.That(status, Is.Not.Null);
        Assert.That(status!.ChainDepth, Is.EqualTo(1));
    }

    [Test]
    public async Task GetScopeStatusAsync_ignores_catalogue_rows_belonging_to_another_scope()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var foreign = BackupScopeSelector.WholeTree(Other);

        await SeedAsync(Source, "k1", "v1");
        await SeedAsync(Other, "k1", "v1");

        await CaptureBackupIdAsync(new LatticeBackupCaptureRequest("full", scope));

        // Two further backups on a different scope. They share the catalogue but
        // must not contribute to this scope's chain, so the scope-match filter
        // has to skip them.
        var otherFull = await CaptureBackupIdAsync(
            new LatticeBackupCaptureRequest("other-full", foreign));
        await SeedAsync(Other, "k2", "v2");
        await CaptureBackupIdAsync(
            new LatticeBackupIncrementalCaptureRequest("other-incr", foreign, otherFull));

        var status = await _fixture.Control.GetScopeStatusAsync(scope);

        Assert.That(status, Is.Not.Null);
        Assert.That(status!.ChainDepth, Is.EqualTo(1),
            "A deeper chain on a different scope must not be counted against this one.");
    }

    [Test]
    public async Task GetScopeStatusAsync_reports_each_scope_its_own_chain_depth()
    {
        // The paired reading of the test above: the foreign rows are not merely
        // ignored, they are attributed to their own scope. Asserting only that
        // this scope reads 1 is satisfied by a filter that drops everything.
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var foreign = BackupScopeSelector.WholeTree(Other);

        await SeedAsync(Source, "k1", "v1");
        await SeedAsync(Other, "k1", "v1");

        await CaptureBackupIdAsync(new LatticeBackupCaptureRequest("full", scope));

        var otherFull = await CaptureBackupIdAsync(
            new LatticeBackupCaptureRequest("other-full", foreign));
        await SeedAsync(Other, "k2", "v2");
        await CaptureBackupIdAsync(
            new LatticeBackupIncrementalCaptureRequest("other-incr", foreign, otherFull));

        var mine = await _fixture.Control.GetScopeStatusAsync(scope);
        var theirs = await _fixture.Control.GetScopeStatusAsync(foreign);

        Assert.Multiple(() =>
        {
            Assert.That(mine!.ChainDepth, Is.EqualTo(1));
            Assert.That(theirs!.ChainDepth, Is.EqualTo(2),
                "The other scope's own chain must still be walked in full.");
        });
    }
}
