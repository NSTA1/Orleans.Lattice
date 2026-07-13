using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupColdRestoreService"/> argument
/// guards. The service is a thin orchestration over the sink, the restore engine,
/// the catalog-rebuild engine, and the once-per-silo initializer; its
/// resolution-from-the-sink, chain-walk, missing-artifact, and end-to-end restore
/// behaviour is proven against a live cluster in
/// <see cref="LatticeBackupColdRestoreIntegrationTests"/>.
/// </summary>
[TestFixture]
public sealed class LatticeBackupColdRestoreServiceTests
{
    private static BackupInitializer CreateInitializer() =>
        new(
            Substitute.For<IGrainFactory>(),
            Substitute.For<IServiceProvider>(),
            Substitute.For<IOptionsMonitor<LatticeBackupOptions>>());

    private static LatticeBackupColdRestoreService CreateService() =>
        new(
            Substitute.For<ILatticeBackupSink>(),
            Substitute.For<ILatticeBackupRestoreService>(),
            Substitute.For<ILatticeBackupCatalogRebuildService>(),
            CreateInitializer(),
            NullLogger<LatticeBackupColdRestoreService>.Instance);

    [Test]
    public void Constructor_null_sink_throws()
    {
        Assert.That(
            () => new LatticeBackupColdRestoreService(
                null!,
                Substitute.For<ILatticeBackupRestoreService>(),
                Substitute.For<ILatticeBackupCatalogRebuildService>(),
                CreateInitializer(),
                NullLogger<LatticeBackupColdRestoreService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_restore_throws()
    {
        Assert.That(
            () => new LatticeBackupColdRestoreService(
                Substitute.For<ILatticeBackupSink>(),
                null!,
                Substitute.For<ILatticeBackupCatalogRebuildService>(),
                CreateInitializer(),
                NullLogger<LatticeBackupColdRestoreService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_catalog_rebuild_throws()
    {
        Assert.That(
            () => new LatticeBackupColdRestoreService(
                Substitute.For<ILatticeBackupSink>(),
                Substitute.For<ILatticeBackupRestoreService>(),
                null!,
                CreateInitializer(),
                NullLogger<LatticeBackupColdRestoreService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_initializer_throws()
    {
        Assert.That(
            () => new LatticeBackupColdRestoreService(
                Substitute.For<ILatticeBackupSink>(),
                Substitute.For<ILatticeBackupRestoreService>(),
                Substitute.For<ILatticeBackupCatalogRebuildService>(),
                null!,
                NullLogger<LatticeBackupColdRestoreService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_logger_throws()
    {
        Assert.That(
            () => new LatticeBackupColdRestoreService(
                Substitute.For<ILatticeBackupSink>(),
                Substitute.For<ILatticeBackupRestoreService>(),
                Substitute.For<ILatticeBackupCatalogRebuildService>(),
                CreateInitializer(),
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ColdRestoreAsync_null_request_throws()
    {
        var service = CreateService();

        Assert.That(
            async () => await service.ColdRestoreAsync(null!),
            Throws.ArgumentNullException);
    }
}
