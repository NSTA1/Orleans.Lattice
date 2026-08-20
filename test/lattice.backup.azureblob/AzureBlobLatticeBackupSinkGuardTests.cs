using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Argument-guard tests for <see cref="AzureBlobLatticeBackupSink"/>. Each guard
/// runs before the sink touches Azure, so these execute without an emulator: the
/// container client is built from the development connection string but never
/// contacted, since validation short-circuits first.
/// </summary>
[TestFixture]
public sealed class AzureBlobLatticeBackupSinkGuardTests
{
    private const string DevConnectionString = "UseDevelopmentStorage=true";

    private ServiceProvider _services = null!;
    private AzureBlobLatticeBackupSink _sut = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(b => b.AddAssembly(typeof(BackupManifest).Assembly))
            .BuildServiceProvider();
        var serializer = _services.GetRequiredService<Serializer<BackupManifest>>();
        var options = new LatticeBackupAzureBlobOptions { ConnectionString = DevConnectionString };
        _sut = new AzureBlobLatticeBackupSink(options.BuildContainerClient(), serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Constructor_throws_on_null_container()
    {
        var serializer = _services.GetRequiredService<Serializer<BackupManifest>>();
        Assert.That(() => new AzureBlobLatticeBackupSink(null!, serializer), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_serializer()
    {
        var options = new LatticeBackupAzureBlobOptions { ConnectionString = DevConnectionString };
        Assert.That(() => new AzureBlobLatticeBackupSink(options.BuildContainerClient(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public void WriteArtifactAsync_throws_on_null_or_empty_artifact_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _sut.WriteArtifactAsync(null!, EmptyContent()), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _sut.WriteArtifactAsync(string.Empty, EmptyContent()), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void WriteArtifactAsync_throws_on_null_content()
    {
        Assert.That(async () => await _sut.WriteArtifactAsync("artifact-1", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ReadArtifactAsync_throws_on_null_or_empty_artifact_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await Drain(_sut.ReadArtifactAsync(null!)), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await Drain(_sut.ReadArtifactAsync(string.Empty)), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void DeleteArtifactAsync_throws_on_null_or_empty_artifact_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _sut.DeleteArtifactAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _sut.DeleteArtifactAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void WriteManifestAsync_throws_on_null_manifest()
    {
        Assert.That(async () => await _sut.WriteManifestAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ReadManifestAsync_throws_on_null_or_empty_backup_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _sut.ReadManifestAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _sut.ReadManifestAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void DeleteManifestAsync_throws_on_null_or_empty_backup_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _sut.DeleteManifestAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _sut.DeleteManifestAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ManifestExistsAsync_throws_on_null_or_empty_backup_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _sut.ManifestExistsAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _sut.ManifestExistsAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ProbeAsync_throws_on_null_or_empty_backup_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _sut.ProbeAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await _sut.ProbeAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> EmptyContent()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async Task Drain(IAsyncEnumerable<ReadOnlyMemory<byte>> source)
    {
        await foreach (var _ in source)
        {
        }
    }
}
