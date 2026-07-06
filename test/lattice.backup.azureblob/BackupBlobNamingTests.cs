namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Unit tests for <see cref="BackupBlobNaming"/> - the deterministic blob-name
/// layout and id round-tripping that keeps manifest and artifact listings
/// efficient and ordered. Exercised without an emulator.
/// </summary>
[TestFixture]
public class BackupBlobNamingTests
{
    [Test]
    public void ManifestBlobName_is_prefixed_with_the_manifest_prefix()
    {
        Assert.That(BackupBlobNaming.ManifestBlobName("backup-1"), Is.EqualTo("manifests/backup-1"));
    }

    [Test]
    public void ArtifactBlobName_is_prefixed_with_the_artifact_prefix()
    {
        Assert.That(BackupBlobNaming.ArtifactBlobName("abc123"), Is.EqualTo("artifacts/abc123"));
    }

    [Test]
    public void Manifest_and_artifact_prefixes_are_distinct()
    {
        Assert.That(BackupBlobNaming.ManifestPrefix, Is.Not.EqualTo(BackupBlobNaming.ArtifactPrefix));
    }

    [Test]
    public void ManifestBlobName_throws_on_null_or_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupBlobNaming.ManifestBlobName(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupBlobNaming.ManifestBlobName(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void ArtifactBlobName_throws_on_null_or_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupBlobNaming.ArtifactBlobName(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupBlobNaming.ArtifactBlobName(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void BackupIdFromManifestBlobName_round_trips()
    {
        var name = BackupBlobNaming.ManifestBlobName("backup-42");
        Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName(name), Is.EqualTo("backup-42"));
    }

    [Test]
    public void ArtifactIdFromBlobName_round_trips()
    {
        var name = BackupBlobNaming.ArtifactBlobName("deadbeef");
        Assert.That(BackupBlobNaming.ArtifactIdFromBlobName(name), Is.EqualTo("deadbeef"));
    }

    [Test]
    public void BackupIdFromManifestBlobName_returns_null_for_a_non_manifest_name()
    {
        Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName("artifacts/x"), Is.Null);
    }

    [Test]
    public void ArtifactIdFromBlobName_returns_null_for_a_non_artifact_name()
    {
        Assert.That(BackupBlobNaming.ArtifactIdFromBlobName("manifests/x"), Is.Null);
    }

    [Test]
    public void Artifact_blob_names_sort_in_id_order()
    {
        // Azure returns listings in lexicographical blob-name order; because the
        // prefix is fixed and ids never contain a slash, that is exactly id order.
        var ids = new[] { "c", "a", "b" };
        var names = ids.Select(BackupBlobNaming.ArtifactBlobName).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        var recovered = names.Select(n => BackupBlobNaming.ArtifactIdFromBlobName(n)).ToArray();

        Assert.That(recovered, Is.EqualTo(new[] { "a", "b", "c" }));
    }
}
