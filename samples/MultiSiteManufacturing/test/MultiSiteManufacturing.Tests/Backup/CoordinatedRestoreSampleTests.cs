using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Backup;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Backup;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace MultiSiteManufacturing.Tests.Backup;

/// <summary>
/// Sample-level demonstration of the coordinated multi-cluster restore feature.
/// Two independent in-memory clusters ("us" and "eu") share a single filesystem
/// backup sink directory - the same seam the sample host wires so a replicated
/// tree can be backed up on one cluster and restored on any peer. A backup
/// captured on one cluster is restored into the fact tree on both, and the test
/// asserts the outcome is consistent across the participating clusters:
/// all-or-nothing (every captured key lands, identically, on every cluster; no
/// torn / partial state) and no re-advance (a repeated restore converges to the
/// same content and never double-applies the cut).
/// <para>
/// The clusters are wired with the core lattice and the backup add-on over the
/// shared <see cref="FileSystemBackupSink"/>, so the restore runs the real
/// backup restore engine. The full cross-cluster saga dispatch (coordinator +
/// participant + write fence + gRPC control channel) that the sample host
/// activates through <c>AddLatticeReplication</c> is covered end to end by the
/// replication package's own coordinated-restore integration and chaos suites;
/// this in-process sample test focuses on the shared-sink-enabled cross-cluster
/// consistency and idempotency property that a coordinated restore guarantees.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class CoordinatedRestoreSampleTests
{
    private const string FactTreeId = LatticeFactBackend.FactTreeId;

    private string _sharedSinkDir = null!;
    private SampleClusterHandle _us = null!;
    private SampleClusterHandle _eu = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _sharedSinkDir = Path.Combine(
            Path.GetTempPath(), "msmfg-coordinated-restore-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_sharedSinkDir);

        _us = await SampleClusterHandle.StartAsync(_sharedSinkDir);
        _eu = await SampleClusterHandle.StartAsync(_sharedSinkDir);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_us is not null)
        {
            await _us.DisposeAsync();
        }

        if (_eu is not null)
        {
            await _eu.DisposeAsync();
        }

        if (_sharedSinkDir is not null && Directory.Exists(_sharedSinkDir))
        {
            try
            {
                Directory.Delete(_sharedSinkDir, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup of the shared temp directory.
            }
        }
    }

    [Test]
    public async Task Restore_of_captured_tree_lands_identically_on_every_cluster()
    {
        var treeId = NewTreeId();
        var expected = await SeedAsync(_us, treeId, entries: 8);

        // Capture on "us" to the shared sink, then restore the SAME backup into the
        // fact tree on BOTH clusters. "eu" has never seen this data; it resolves the
        // manifest and artifacts purely from the shared sink.
        var backupId = await _us.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("cross-cluster", BackupScopeSelector.WholeTree(treeId)));

        var usResult = await RestoreInPlaceAsync(_us, backupId.BackupId, treeId);
        var euResult = await RestoreInPlaceAsync(_eu, backupId.BackupId, treeId);

        var usContent = await SnapshotAsync(_us, treeId);
        var euContent = await SnapshotAsync(_eu, treeId);

        // All-or-nothing: both clusters hold the complete captured content, byte for
        // byte, with nothing missing and nothing extra.
        AssertContentEquals(expected, usContent, "us");
        AssertContentEquals(expected, euContent, "eu");
        AssertContentEquals(usContent, euContent, "eu-vs-us");

        Assert.That(usResult.EntriesApplied, Is.EqualTo(expected.Count));
        Assert.That(euResult.EntriesApplied, Is.EqualTo(expected.Count));
    }

    [Test]
    public async Task Repeated_restore_converges_without_re_advancing_the_cut()
    {
        var treeId = NewTreeId();
        var expected = await SeedAsync(_us, treeId, entries: 6);

        var backupId = await _us.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("idempotent", BackupScopeSelector.WholeTree(treeId)));

        // Restore twice onto "eu". The second restore must be a no-op in effect: the
        // restored cut is not re-advanced, no entry is double-applied, and a reader
        // never observes a torn state between the two runs.
        var first = await RestoreInPlaceAsync(_eu, backupId.BackupId, treeId);
        var afterFirst = await SnapshotAsync(_eu, treeId);

        var second = await RestoreInPlaceAsync(_eu, backupId.BackupId, treeId);
        var afterSecond = await SnapshotAsync(_eu, treeId);

        AssertContentEquals(expected, afterFirst, "eu-first");
        AssertContentEquals(afterFirst, afterSecond, "eu-second");
        Assert.That(second.OperationId, Is.EqualTo(first.OperationId));
    }

    [Test]
    public async Task Operator_facade_round_trips_the_fact_tree_through_the_shared_sink()
    {
        // The fact tree is the tree the host declares replicated, so the operator
        // facade's shadow-cutover restore is the path a coordinated restore commits
        // per cluster. Seed the fact tree, capture + restore through the facade, and
        // read the freshly cutover shadow tree back.
        var expected = await SeedAsync(_us, FactTreeId, entries: 5);

        var backupId = await _us.RestoreOperator.CaptureFactTreeAsync("facade-demo");
        var result = await _us.RestoreOperator.RestoreFactTreeAsync(backupId);

        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
        Assert.That(result.EntriesApplied, Is.EqualTo(expected.Count));
        Assert.That(result.ShadowPhysicalTreeId, Is.Not.Null);

        var restored = await SnapshotAsync(_us, result.ShadowPhysicalTreeId!);
        AssertContentEquals(expected, restored, "facade-shadow");
    }

    private static string NewTreeId() => $"mfg-facts-{Guid.NewGuid():N}";

    private static Task<LatticeRestoreResult> RestoreInPlaceAsync(
        SampleClusterHandle cluster, string backupId, string treeId) =>
        cluster.Restore.RestoreAsync(new LatticeRestoreRequest(
            backupId, targetTreeId: treeId, mode: LatticeRestoreMode.InPlace));

    private static async Task<IReadOnlyDictionary<string, byte[]>> SeedAsync(
        SampleClusterHandle cluster, string treeId, int entries)
    {
        var tree = cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var seeded = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        for (var i = 0; i < entries; i++)
        {
            var key = $"part-{i:D4}";
            var value = Encoding.UTF8.GetBytes($"state-{treeId}-{i}");
            await tree.SetAsync(key, value);
            seeded[key] = value;
        }

        return seeded;
    }

    private static async Task<IReadOnlyDictionary<string, byte[]>> SnapshotAsync(
        SampleClusterHandle cluster, string treeId)
    {
        var tree = cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var content = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        await foreach (var key in tree.KeysAsync())
        {
            var value = await tree.GetAsync(key);
            if (value is not null)
            {
                content[key] = value;
            }
        }

        return content;
    }

    private static void AssertContentEquals(
        IReadOnlyDictionary<string, byte[]> expected,
        IReadOnlyDictionary<string, byte[]> actual,
        string label)
    {
        Assert.That(actual.Count, Is.EqualTo(expected.Count), $"[{label}] entry count");
        foreach (var (key, value) in expected)
        {
            Assert.That(actual.ContainsKey(key), Is.True, $"[{label}] missing key '{key}'");
            Assert.That(actual[key], Is.EqualTo(value).AsCollection, $"[{label}] value for '{key}'");
        }
    }

    /// <summary>
    /// A single in-memory sample cluster wired with the core lattice and the backup
    /// add-on over the shared <see cref="FileSystemBackupSink"/> directory staged in
    /// the pending shared-sink field.
    /// </summary>
    private sealed class SampleClusterHandle
    {
        private static string? _pendingSharedSinkDir;

        private TestCluster _cluster = null!;

        public IGrainFactory GrainFactory => _cluster.GrainFactory;

        private IServiceProvider SiloServices =>
            _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

        public ILatticeBackupCaptureService Capture =>
            SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

        public ILatticeBackupRestoreService Restore =>
            SiloServices.GetRequiredService<ILatticeBackupRestoreService>();

        public CoordinatedRestoreOperator RestoreOperator => new(Capture, Restore, NullOperatorLogger());

        public static async Task<SampleClusterHandle> StartAsync(string sharedSinkDir)
        {
            _pendingSharedSinkDir = sharedSinkDir;

            var handle = new SampleClusterHandle();
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
            handle._cluster = builder.Build();
            await handle._cluster.DeployAsync();
            return handle;
        }

        public async Task DisposeAsync()
        {
            if (_cluster is not null)
            {
                await _cluster.StopAllSilosAsync();
                await _cluster.DisposeAsync();
            }
        }

        private static ILogger<CoordinatedRestoreOperator> NullOperatorLogger() =>
            Microsoft.Extensions.Logging.Abstractions.NullLogger<CoordinatedRestoreOperator>.Instance;

        private sealed class SiloConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                var sharedSinkDir = _pendingSharedSinkDir
                    ?? throw new InvalidOperationException("No shared sink directory was staged for the silo build.");

                siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
                siloBuilder.UseInMemoryReminderService();

                // Register the shared external sink BEFORE AddLatticeBackup so the
                // package's in-cluster default does not win, mirroring the host.
                siloBuilder.Services.AddSingleton<ILatticeBackupSink>(sp => new FileSystemBackupSink(
                    sharedSinkDir,
                    sp.GetRequiredService<Serializer>(),
                    sp.GetRequiredService<ILogger<FileSystemBackupSink>>()));
                siloBuilder.AddLatticeBackup();
            }
        }
    }
}
