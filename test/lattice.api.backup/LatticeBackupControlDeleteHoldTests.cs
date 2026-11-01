using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// An operator delete removes a backup under the backup's per-id hold on its
/// scope's scheduler grain, through the publication gate (issue #5418), so it
/// never interleaves with a capture republishing the same content-addressed id
/// or with a retention pass.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlDeleteHoldTests
{
    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new ApiBackupClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task DeleteBackupAsync_removes_the_backup_under_its_per_id_hold()
    {
        RedProof.Routing = true; // RED-PROOF
        await _fixture.GrainFactory.GetGrain<ILattice>("orders").SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var captured = await _fixture.SiloServices.GetRequiredService<ILatticeBackupCaptureService>()
            .CaptureAsync(new LatticeBackupCaptureRequest("delete-me", BackupScopeSelector.WholeTree("orders")));
        var gate = new RecordingGate(_fixture.SiloServices.GetRequiredService<IBackupPublicationGate>());
        var control = _fixture.CreateControlWith(
            _fixture.SiloServices.GetRequiredService<BackupAccessAuthorizer>(),
            services: new GateOverride(_fixture.SiloServices, gate));

        var deleted = await control.DeleteBackupAsync(captured.BackupId);

        Assert.Multiple(async () =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(gate.Retired, Does.Contain($"{captured.BackupId}:{BackupRetireKind.Delete}"), "the delete runs under the id's hold");
            Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Null);
            Assert.That(await _fixture.Sink.ManifestExistsAsync(captured.BackupId), Is.False);
        });
    }

    // Serves the recording gate in place of the silo's, and everything else from the silo.
    private sealed class GateOverride(IServiceProvider inner, IBackupPublicationGate gate) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            serviceType == typeof(IBackupPublicationGate) ? gate : inner.GetService(serviceType);
    }

    private sealed class RecordingGate(IBackupPublicationGate inner) : IBackupPublicationGate
    {
        public ConcurrentQueue<string> Retired { get; } = new();

        public Task<BackupManifest> PublishFullAsync(BackupManifest manifest, bool register, CancellationToken cancellationToken) =>
            inner.PublishFullAsync(manifest, register, cancellationToken);

        public Task<BackupManifest?> PublishIncrementAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
            inner.PublishIncrementAsync(manifest, cancellationToken);

        public Task<BackupManifest> RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
            inner.RegisterAsync(manifest, cancellationToken);

        public Task<bool> RegisterRebuiltAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
            inner.RegisterRebuiltAsync(manifest, cancellationToken);

        public async Task<bool> RetireAsync(BackupManifest row, BackupRetireKind kind, CancellationToken cancellationToken)
        {
            var removed = await inner.RetireAsync(row, kind, cancellationToken);
            Retired.Enqueue($"{row.Id}:{kind}");
            return removed;
        }
    }
}
