using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupRestoreService"/> that do not require a
/// live silo. Covers:
/// - Coordinated-dispatch short-circuits (lines 58, 163): non-null dispatcher
///   result returned immediately.
/// - Backup-chain cycle detection (lines 1048-1049): a manifest whose
///   BaseBackupId references itself causes BuildChainAsync to throw
///   <see cref="LatticeRestoreValidationException"/>.
/// - Out-of-scope sub-range validation (lines 1089-1091): a requested scope
///   that falls outside the captured scope throws
///   <see cref="LatticeRestoreValidationException"/>.
/// - Unknown backup scope kind (line 1111): a scope with an invalid
///   <see cref="BackupScopeKind"/> value throws
///   <see cref="ArgumentOutOfRangeException"/> from ResolveRange.
/// </summary>
[TestFixture]
public sealed class LatticeBackupRestoreServiceUnitTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private (LatticeBackupRestoreService Service, ILatticeBackupCatalogStore Catalog, ILatticeBackupSink Sink)
        CreateServiceWithCatalog(IRestoreSagaDispatcher dispatcher)
    {
        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IRestoreSagaDispatcher)).Returns(dispatcher);

        var gate = Substitute.For<ILatticeAccessGate>();
        var catalog = Substitute.For<ILatticeBackupCatalogStore>();
        var sink = Substitute.For<ILatticeBackupSink>();
        var authorizer = new BackupAccessAuthorizer(gate);

        var service = new LatticeBackupRestoreService(
            Substitute.For<IGrainFactory>(),
            sink,
            catalog,
            authorizer,
            _serializer,
            Substitute.For<ITagIndexReconcileTrigger>(),
            serviceProvider,
            Substitute.For<ILatticeBackupTenantScope>(),
            NullLogger<LatticeBackupRestoreService>.Instance);

        return (service, catalog, sink);
    }

    private LatticeBackupRestoreService CreateService(IRestoreSagaDispatcher dispatcher) =>
        CreateServiceWithCatalog(dispatcher).Service;

    [Test]
    public async Task RestoreAsync_returns_dispatched_result_when_dispatcher_handles_it()
    {
        // Line 58: when TryDispatchAsync returns a non-null LatticeRestoreResult
        // the service returns it immediately without entering the local restore path.
        var expected = new LatticeRestoreResult(
            "backup-id", "orders", LatticeRestoreMode.InPlace, "op-1",
            new[] { "backup-id" }, 0);

        var dispatcher = Substitute.For<IRestoreSagaDispatcher>();
        dispatcher.TryDispatchAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeRestoreResult?>(expected));

        var service = CreateService(dispatcher);
        var request = new LatticeRestoreRequest("backup-id");

        var result = await service.RestoreAsync(request);

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public async Task RestoreSetAsync_returns_dispatched_result_when_dispatcher_handles_it()
    {
        // Line 163: when TryDispatchSetAsync returns a non-null list the service
        // returns it immediately without entering the local set-restore path.
        var expected = new LatticeRestoreResult(
            "backup-id", "orders", LatticeRestoreMode.ShadowCutover, "op-2",
            new[] { "backup-id" }, 0);
        IReadOnlyList<LatticeRestoreResult> dispatchedList = new[] { expected };

        var dispatcher = Substitute.For<IRestoreSagaDispatcher>();
        dispatcher.TryDispatchSetAsync(Arg.Any<string>(), Arg.Any<LatticeRestoreMode>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<LatticeRestoreResult>?>(dispatchedList));

        var service = CreateService(dispatcher);

        var result = await service.RestoreSetAsync("set-id");

        Assert.That(result, Is.SameAs(dispatchedList));
    }

    [Test]
    public async Task RestoreAsync_throws_when_backup_chain_contains_a_cycle()
    {
        // Lines 1048-1049: BuildChainAsync detects a self-referencing cycle and
        // throws LatticeRestoreValidationException before any apply step runs.
        var dispatcher = Substitute.For<IRestoreSagaDispatcher>();
        dispatcher.TryDispatchAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeRestoreResult?>(null));

        var (service, catalog, _) = CreateServiceWithCatalog(dispatcher);

        // A manifest that references itself as its own base backup creates a cycle.
        var cycleManifest = BackupManifestModelTests.Sample(
            id: "cycle-backup",
            kind: BackupKind.Incremental,
            baseBackupId: "cycle-backup");
        catalog.GetAsync("cycle-backup", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BackupManifest?>(cycleManifest));

        var request = new LatticeRestoreRequest("cycle-backup");

        // Use system origin so the authorization gate is bypassed and
        // BuildChainAsync (where the cycle check lives) is actually reached.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(
                async () => await service.RestoreAsync(request),
                Throws.TypeOf<LatticeRestoreValidationException>()
                    .With.Message.Contains("cycle"));
        }
    }

    [Test]
    public async Task RestoreAsync_throws_when_requested_scope_falls_outside_captured_scope()
    {
        // Lines 1089-1091: ResolveEffectiveScope throws LatticeRestoreValidationException
        // when the requested sub-scope does not fall within the captured scope.
        var dispatcher = Substitute.For<IRestoreSagaDispatcher>();
        dispatcher.TryDispatchAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeRestoreResult?>(null));

        var (service, catalog, _) = CreateServiceWithCatalog(dispatcher);

        // Captured scope covers prefix "a/" in "orders"; requesting prefix "z/" is outside.
        var manifestScope = BackupScopeSelector.Prefix("orders", "a/");
        var manifest = new BackupManifest(
            id: "prefix-backup",
            name: "nightly",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: BackupKind.Full,
            scope: manifestScope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(1, 4096, new[] { "d0" }),
            structuralDigest: "digest-root",
            keyDescriptors: Array.Empty<BackupKeyDescriptor>(),
            contentDescriptors: Array.Empty<BackupContentDescriptor>(),
            provenance: Array.Empty<BackupOriginProvenance>());
        catalog.GetAsync("prefix-backup", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BackupManifest?>(manifest));

        // Requested scope is outside the captured prefix.
        var request = new LatticeRestoreRequest("prefix-backup")
        {
            Scope = BackupScopeSelector.Prefix("orders", "z/"),
        };

        Assert.That(
            async () => await service.RestoreAsync(request),
            Throws.TypeOf<LatticeRestoreValidationException>()
                .With.Message.Contains("falls outside"));
    }

    [Test]
    public async Task RestoreAsync_throws_on_unknown_backup_scope_kind()
    {
        // Line 1111: ResolveRange throws ArgumentOutOfRangeException when the
        // scope's Kind does not match any known BackupScopeKind value.
        var dispatcher = Substitute.For<IRestoreSagaDispatcher>();
        dispatcher.TryDispatchAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LatticeRestoreResult?>(null));

        var (service, catalog, _) = CreateServiceWithCatalog(dispatcher);

        var manifest = BackupManifestModelTests.Sample("unknown-kind-backup");
        catalog.GetAsync("unknown-kind-backup", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<BackupManifest?>(manifest));

        // An invalid enum value reaches the ResolveRange switch's default arm.
        var unknownKindScope = new BackupScopeSelector((BackupScopeKind)99, "orders", null);
        var request = new LatticeRestoreRequest("unknown-kind-backup")
        {
            Scope = unknownKindScope,
        };

        Assert.That(
            async () => await service.RestoreAsync(request),
            Throws.TypeOf<ArgumentOutOfRangeException>()
                .With.Message.Contains("Unknown backup scope kind"));
    }
}
