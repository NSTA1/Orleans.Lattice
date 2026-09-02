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
        // prefix is fixed, that is exactly id order.
        var ids = new[] { "c", "a", "b" };
        var names = ids.Select(BackupBlobNaming.ArtifactBlobName).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        var recovered = names.Select(n => BackupBlobNaming.ArtifactIdFromBlobName(n)).ToArray();

        Assert.That(recovered, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    /// <summary>
    /// Ids that would resolve to a blob outside the manifest or artifact prefix.
    /// The Azure SDK addresses a blob through <see cref="UriBuilder"/>, which
    /// performs RFC 3986 dot-segment removal, so a <c>..</c> segment silently
    /// walks up out of the prefix and, with enough segments, out of the
    /// configured container altogether. Percent-encoded forms collapse
    /// identically because dot-segment removal happens after percent-decoding.
    /// </summary>
    private static readonly string[] EscapingIds =
    [
        "../secrets",
        "../../secrets/keys.json",
        "a/../../b",
        "a/../../../../etc/passwd",
        "%2E%2E/secrets",
        "%2e%2e%2Fsecrets",
        ".",
        "..",
        "a/./b",
        "a//b",
        "/absolute",
        "a\\..\\b",
        "back\\slash",
    ];

    [TestCaseSource(nameof(EscapingIds))]
    public void ManifestBlobName_rejects_an_id_that_would_escape_its_prefix(string backupId)
    {
        Assert.That(
            () => BackupBlobNaming.ManifestBlobName(backupId),
            Throws.InstanceOf<ArgumentException>(),
            $"'{backupId}' must not be concatenated onto the manifest prefix: the blob address "
            + "would resolve outside it.");
    }

    [TestCaseSource(nameof(EscapingIds))]
    public void ArtifactBlobName_rejects_an_id_that_would_escape_its_prefix(string artifactId)
    {
        Assert.That(
            () => BackupBlobNaming.ArtifactBlobName(artifactId),
            Throws.InstanceOf<ArgumentException>(),
            $"'{artifactId}' must not be concatenated onto the artifact prefix: the blob address "
            + "would resolve outside it.");
    }

    [Test]
    public void Blob_names_reject_a_control_character()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupBlobNaming.ManifestBlobName("a\nb"), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupBlobNaming.ArtifactBlobName("a\0b"), Throws.InstanceOf<ArgumentException>());
        });
    }

    /// <summary>
    /// A tenant-composed tree id of the form <c>t/{tenant}/{name}</c> is embedded
    /// verbatim in every artifact id, so an interior <c>/</c> is legitimate and
    /// must keep working. Only segments that change the resolved location are
    /// rejected.
    /// </summary>
    [TestCase("t/acme/orders")]
    [TestCase("t/acme/orders-Full-638000000000000000-0123456789abcdef")]
    [TestCase("backup-42")]
    [TestCase("deadbeef")]
    [TestCase("a.b.c")]
    [TestCase("...")]
    public void A_legitimate_id_containing_a_separator_is_accepted_and_round_trips(string id)
    {
        var manifest = BackupBlobNaming.ManifestBlobName(id);
        var artifact = BackupBlobNaming.ArtifactBlobName(id);

        Assert.Multiple(() =>
        {
            Assert.That(manifest, Is.EqualTo(BackupBlobNaming.ManifestPrefix + id));
            Assert.That(artifact, Is.EqualTo(BackupBlobNaming.ArtifactPrefix + id));
            Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName(manifest), Is.EqualTo(id));
            Assert.That(BackupBlobNaming.ArtifactIdFromBlobName(artifact), Is.EqualTo(id));
        });
    }

    /// <summary>
    /// The property that actually matters: whatever name is produced, resolving it
    /// the way the Azure SDK does must land strictly beneath the container and the
    /// prefix. This asserts the outcome rather than the validator's internals, so
    /// it still holds if the validation strategy is ever changed.
    /// </summary>
    [TestCaseSource(nameof(EscapingIds))]
    public void No_accepted_id_can_resolve_outside_its_container_prefix(string hostileId)
    {
        string? manifestName = null;
        try
        {
            manifestName = BackupBlobNaming.ManifestBlobName(hostileId);
        }
        catch (ArgumentException)
        {
            Assert.Pass("The hostile id was rejected before it could be used to address a blob.");
        }

        var resolved = new UriBuilder("https://account.blob.core.windows.net/container/" + manifestName).Uri;

        Assert.That(
            resolved.AbsolutePath,
            Does.StartWith("/container/" + BackupBlobNaming.ManifestPrefix),
            "An accepted id resolved outside the container's manifest prefix.");
    }
}
