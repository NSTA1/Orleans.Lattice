using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Regression coverage for the backup-catalog listing page-size clamp: nothing
/// validates <see cref="LatticeApiBackupOptions"/>, so a non-positive bound used
/// to yield a zero page whose continuation read indexed an empty list, and an
/// unset request took a default above <see cref="LatticeApiBackupOptions.MaxListPageSize"/>.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlPageSizeTests
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

    [TestCase(false, 0)]
    [TestCase(false, -3)]
    [TestCase(true, 0)]
    [TestCase(true, -3)]
    public async Task A_non_positive_configured_bound_still_lists_one_entry_per_page(bool newestFirst, int bound)
    {
        await _fixture.InitializeAsync();
        await CaptureTwoAsync();
        var options = _fixture.SiloServices.GetRequiredService<IOptions<LatticeApiBackupOptions>>().Value;
        options.DefaultListPageSize = bound;
        options.MaxListPageSize = bound;

        var page = await _fixture.Control.ListBackupsAsync(
            new BackupCatalogRequest { PageSize = 25, OrderByCreatedDescending = newestFirst });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(1));
            Assert.That(page.NextPageToken, Is.Not.Null);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task An_unset_request_never_exceeds_the_configured_maximum(bool newestFirst)
    {
        await _fixture.InitializeAsync();
        await CaptureTwoAsync();
        var options = _fixture.SiloServices.GetRequiredService<IOptions<LatticeApiBackupOptions>>().Value;
        options.DefaultListPageSize = 50;
        options.MaxListPageSize = 1;

        var page = await _fixture.Control.ListBackupsAsync(
            new BackupCatalogRequest { OrderByCreatedDescending = newestFirst });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(1));
            Assert.That(page.NextPageToken, Is.Not.Null);
        });
    }

    private async Task CaptureTwoAsync()
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await CaptureAsync("first");
        await tree.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        await CaptureAsync("second");
    }

    private async Task CaptureAsync(string label)
    {
        var operations = (ILatticeBackupOperations)_fixture.Control;
        var handle = await operations.StartBackupAsync(
            new LatticeBackupCaptureRequest(label, BackupScopeSelector.WholeTree(Source)));
        LatticeOperationStatus? status = null;
        await Orleans.Lattice.Testing.TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(handle.OperationId)) is { IsTerminal: true },
            $"operation {handle.OperationId} to finish");
        Assert.That(status!.ResultReference, Is.Not.Null);
    }
}
