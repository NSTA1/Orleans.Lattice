using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The restore stream consults its per-record admission on both apply paths: a
/// record the admission refuses is dead-lettered, never written. The admission's
/// own rules are pinned by the tenancy package's tests; these pin that the restore
/// loops actually ask it (the <c>RestoreAdmitsOnlyNamespace</c> row of
/// <c>spec/backup/RefinementRestore.md</c>).
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupRestoreAdmissionWiringTests
{
    private const string Source = "admission-src";
    private const string ForeignKey = "foreign:1";

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task A_bulk_load_restore_dead_letters_a_record_the_admission_refuses()
    {
        var backup = await CaptureSourceAsync();

        var result = await CreateRestore().RestoreAsync(new LatticeRestoreRequest(backup, "admission-fresh"));

        await AssertForeignRecordDeadLetteredAsync("admission-fresh", result);
    }

    [Test]
    public async Task A_merge_restore_dead_letters_a_record_the_admission_refuses()
    {
        var backup = await CaptureSourceAsync();
        var target = _fixture.GrainFactory.GetGrain<ILattice>("admission-existing");
        await target.SetAsync("pre-existing", Encoding.UTF8.GetBytes("x"));

        var result = await CreateRestore().RestoreAsync(new LatticeRestoreRequest(backup, "admission-existing"));

        await AssertForeignRecordDeadLetteredAsync("admission-existing", result);
    }

    private async Task<string> CaptureSourceAsync()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("own:1", Encoding.UTF8.GetBytes("a"));
        await source.SetAsync(ForeignKey, Encoding.UTF8.GetBytes("b"));
        var capture = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("admission", BackupScopeSelector.WholeTree(Source)));
        return capture.BackupId;
    }

    private async Task AssertForeignRecordDeadLetteredAsync(string treeId, LatticeRestoreResult result)
    {
        var target = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        var own = await target.GetAsync("own:1");
        var foreign = await target.GetAsync(ForeignKey);

        Assert.Multiple(() =>
        {
            Assert.That(own, Is.Not.Null, "an admitted record is restored");
            Assert.That(foreign, Is.Null, "a refused record must never be written");
            Assert.That(result.DeadLetteredCrossTenant, Is.EqualTo(1));
        });
    }

    private ILatticeBackupRestoreService CreateRestore()
    {
        var services = _fixture.SiloServices;
        return new LatticeBackupRestoreService(
            _fixture.GrainFactory,
            _fixture.Sink,
            _fixture.Catalog,
            new BackupAccessAuthorizer(new NullLatticeAccessGate()),
            _fixture.Serializer,
            services.GetRequiredService<ITagIndexReconcileTrigger>(),
            services,
            new RefusingForeignScope(),
            services.GetRequiredService<ILoggerFactory>().CreateLogger<LatticeBackupRestoreService>());
    }

    /// <summary>An active tenant scope whose admission refuses every <c>foreign:</c> key.</summary>
    private sealed class RefusingForeignScope : ILatticeBackupTenantScope
    {
        public bool IsActive => true;

        public void AuthorizeCapture(string treeId)
        {
        }

        public void AuthorizeRestoreTarget(string treeId)
        {
        }

        public ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(
            string targetTreeId,
            CancellationToken cancellationToken = default) =>
            new(new RefusingForeignAdmission());
    }

    private sealed class RefusingForeignAdmission : IBackupRestoreAdmission
    {
        public long AdmittedCount { get; private set; }

        public long DeadLetteredCrossTenant { get; private set; }

        public long DeadLetteredOverQuota => 0;

        public BackupRestoreRecordDisposition Admit(string key)
        {
            if (key.StartsWith("foreign:", StringComparison.Ordinal))
            {
                DeadLetteredCrossTenant++;
                return BackupRestoreRecordDisposition.CrossTenant;
            }

            AdmittedCount++;
            return BackupRestoreRecordDisposition.Admit;
        }
    }
}
