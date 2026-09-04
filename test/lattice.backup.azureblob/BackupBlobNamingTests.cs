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

    /// <summary>
    /// Both recovery helpers are fed straight from a listing, so a null name is a
    /// reachable input rather than a defensive impossibility: the Azure SDK's
    /// <c>BlobItem.Name</c> is a plain string that a future or partial listing
    /// response can leave unset. Recovering "no id" is the only safe answer, and it
    /// must not become a <see cref="NullReferenceException"/> that aborts the whole
    /// enumeration.
    /// </summary>
    [Test]
    public void Id_recovery_returns_null_for_a_null_blob_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName(null!), Is.Null);
            Assert.That(BackupBlobNaming.ArtifactIdFromBlobName(null!), Is.Null);
        });
    }

    [Test]
    public void Id_recovery_returns_null_for_a_name_that_only_shares_a_prefix_fragment()
    {
        // 'manifest' is a proper prefix of 'manifests/', so a name that stops short
        // must not be mistaken for a manifest and yield a nonsense id.
        Assert.Multiple(() =>
        {
            Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName("manifest"), Is.Null);
            Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName(string.Empty), Is.Null);
            Assert.That(BackupBlobNaming.ArtifactIdFromBlobName("artifact"), Is.Null);
            Assert.That(BackupBlobNaming.ArtifactIdFromBlobName(string.Empty), Is.Null);
        });
    }

    [Test]
    public void Id_recovery_is_case_sensitive_on_the_prefix()
    {
        // Blob names are case-sensitive, so an ordinal prefix match is correct: a
        // differently cased name is a different blob and owns no id here.
        Assert.Multiple(() =>
        {
            Assert.That(BackupBlobNaming.BackupIdFromManifestBlobName("Manifests/x"), Is.Null);
            Assert.That(BackupBlobNaming.ArtifactIdFromBlobName("Artifacts/x"), Is.Null);
        });
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

    /// <summary>
    /// Alternative spellings of a dot segment that a traversal attempt reaches for
    /// once the obvious ones are rejected: overlong and invalid UTF-8 encodings of
    /// <c>.</c>, double-encoded escapes, Unicode look-alike dots, a parameter-style
    /// suffix, and runs of more than two dots.
    /// <para>
    /// Some of these are legitimately <em>accepted</em> - they are ordinary blob-name
    /// characters, not dot segments - so the assertion is deliberately not "everything
    /// odd is rejected". It is the property that matters: whatever survives validation
    /// must still resolve beneath the prefix. Written as escape sequences rather than
    /// literal characters so the file stays plain ASCII and the exact code point under
    /// test is unambiguous.
    /// </para>
    /// </summary>
    private static readonly string[] AlternativeDotSpellings =
    [
        "%C0%AE%C0%AE/secrets",         // overlong two-byte UTF-8 for '.'
        "%E0%80%AE%E0%80%AE/secrets",   // overlong three-byte UTF-8 for '.'
        "%F0%80%80%AE%F0%80%80%AE/x",   // overlong four-byte UTF-8 for '.'
        "%252E%252E/secrets",           // double-encoded '..'
        "%C0%2E%C0%2E/x",               // invalid lead byte followed by a real '.'
        "\u2024\u2024/x",               // ONE DOT LEADER look-alikes
        "\uFF0E\uFF0E/x",               // FULLWIDTH FULL STOP look-alikes
        "%EF%BC%8E%EF%BC%8E/x",         // percent-encoded fullwidth full stops
        "..;/x",                        // parameter-style suffix on a dot segment
        "..../x",
        "a/.../b",
        "%u002e%u002e/x",               // non-standard %u escape form
    ];

    [TestCaseSource(nameof(AlternativeDotSpellings))]
    public void An_alternative_dot_spelling_still_resolves_beneath_its_prefix(string candidate)
    {
        string manifestName;
        try
        {
            manifestName = BackupBlobNaming.ManifestBlobName(candidate);
        }
        catch (ArgumentException)
        {
            Assert.Pass("The id was rejected before it could be used to address a blob.");
            return;
        }

        var resolved = new UriBuilder("https://account.blob.core.windows.net/container/" + manifestName).Uri;

        Assert.That(
            resolved.AbsolutePath,
            Does.StartWith("/container/" + BackupBlobNaming.ManifestPrefix),
            $"'{candidate}' was accepted but resolved outside the manifest prefix.");
    }

    /// <summary>
    /// A malformed percent-escape is not a traversal vector, so it must not be
    /// rejected on that basis - and, critically, it must not fault.
    /// <see cref="Uri.UnescapeDataString"/> leaves an unparsable escape verbatim on
    /// this runtime rather than throwing, so an id carrying one is validated on its
    /// literal spelling and accepted when that spelling is a legal blob-name suffix.
    /// </summary>
    [TestCase("a%b")]
    [TestCase("100%")]
    [TestCase("%")]
    [TestCase("%2")]
    [TestCase("%ZZ")]
    public void A_malformed_percent_escape_is_treated_as_literal_text(string id)
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupBlobNaming.ManifestBlobName(id), Is.EqualTo("manifests/" + id));
            Assert.That(BackupBlobNaming.ArtifactBlobName(id), Is.EqualTo("artifacts/" + id));
        });
    }
}
