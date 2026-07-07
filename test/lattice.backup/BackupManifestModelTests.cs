using System.Text;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the backup manifest model: the scope selector shape, the
/// self-describing manifest fields, argument validation, and the content-address
/// helper.
/// </summary>
public sealed class BackupManifestModelTests
{
    [Test]
    public void WholeTree_scope_carries_no_key_or_prefix()
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.WholeTree));
        Assert.That(scope.TreeId, Is.EqualTo("orders"));
        Assert.That(scope.KeyOrPrefix, Is.Null);
    }

    [Test]
    public void Prefix_scope_carries_the_prefix()
    {
        var scope = BackupScopeSelector.Prefix("orders", "eu/");
        Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.Prefix));
        Assert.That(scope.KeyOrPrefix, Is.EqualTo("eu/"));
    }

    [Test]
    public void Key_scope_carries_the_key()
    {
        var scope = BackupScopeSelector.Key("orders", "order-1");
        Assert.That(scope.Kind, Is.EqualTo(BackupScopeKind.Key));
        Assert.That(scope.KeyOrPrefix, Is.EqualTo("order-1"));
    }

    [Test]
    public void Scope_rejects_empty_tree_id()
    {
        Assert.That(() => BackupScopeSelector.WholeTree(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void Full_manifest_rejects_a_base_backup_id()
    {
        Assert.That(
            () => Sample(kind: BackupKind.Full, baseBackupId: "base-1"),
            Throws.ArgumentException);
    }

    [Test]
    public void Incremental_manifest_requires_a_base_backup_id()
    {
        Assert.That(
            () => Sample(kind: BackupKind.Incremental, baseBackupId: null),
            Throws.ArgumentException);
    }

    [Test]
    public void Incremental_manifest_accepts_a_base_backup_id()
    {
        var manifest = Sample(kind: BackupKind.Incremental, baseBackupId: "base-1");
        Assert.That(manifest.BaseBackupId, Is.EqualTo("base-1"));
        Assert.That(manifest.Kind, Is.EqualTo(BackupKind.Incremental));
    }

    [Test]
    public void Manifest_rejects_an_id_with_the_reserved_separator()
    {
        Assert.That(
            () => Sample(id: "bad\u001fid"),
            Throws.ArgumentException);
    }

    [Test]
    public void Manifest_preserves_every_self_describing_field()
    {
        var manifest = Sample();
        Assert.That(manifest.Scope.TreeId, Is.EqualTo("orders"));
        Assert.That(manifest.ConsistencyCut.WalSequence, Is.EqualTo(42));
        Assert.That(manifest.Topology.ShardCount, Is.EqualTo(2));
        Assert.That(manifest.StructuralDigest, Is.EqualTo("digest-root"));
        Assert.That(manifest.KeyDescriptors, Has.Count.EqualTo(1));
        Assert.That(manifest.KeyDescriptors[0].MergeMode, Is.EqualTo(BackupKeyMergeMode.Crdt));
        Assert.That(manifest.ContentDescriptors, Has.Count.EqualTo(1));
        Assert.That(manifest.Provenance[0].OriginId, Is.EqualTo("replica-a"));
        Assert.That(manifest.CompressionDictionary!.DictionaryId, Is.EqualTo("dict-1"));
    }

    [Test]
    public void Manifest_has_no_set_membership_by_default()
    {
        var manifest = Sample();
        Assert.Multiple(() =>
        {
            Assert.That(manifest.SetId, Is.Null);
            Assert.That(manifest.SetName, Is.Null);
        });
    }

    [Test]
    public void Set_membership_round_trips_through_a_with_expression()
    {
        var stamped = Sample() with { SetId = "set-abc", SetName = "nightly-set" };
        Assert.Multiple(() =>
        {
            Assert.That(stamped.SetId, Is.EqualTo("set-abc"));
            Assert.That(stamped.SetName, Is.EqualTo("nightly-set"));
        });
    }

    [Test]
    public void ContentHash_is_stable_for_identical_bytes()
    {
        var bytes = Encoding.UTF8.GetBytes("hello backup");
        var a = BackupContentHash.Compute(bytes);
        var b = BackupContentHash.Compute(bytes);
        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Has.Length.EqualTo(64));
    }

    [Test]
    public void ContentHash_over_chunks_matches_the_concatenated_bytes()
    {
        var whole = Encoding.UTF8.GetBytes("hello backup");
        var chunks = new[]
        {
            (ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes("hello "),
            (ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes("backup"),
        };

        Assert.That(BackupContentHash.Compute(chunks), Is.EqualTo(BackupContentHash.Compute(whole)));
    }

    [Test]
    public void ContentHash_differs_for_different_bytes()
    {
        Assert.That(
            BackupContentHash.Compute(Encoding.UTF8.GetBytes("a")),
            Is.Not.EqualTo(BackupContentHash.Compute(Encoding.UTF8.GetBytes("b"))));
    }

    internal static BackupManifest Sample(
        string id = "backup-1",
        BackupKind kind = BackupKind.Full,
        string? baseBackupId = null)
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        return new BackupManifest(
            id: id,
            name: "nightly",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: kind,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: new[]
            {
                new BackupContentDescriptor("artifact-1", "abc123", 12, 1, scope),
            },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) },
            baseBackupId: baseBackupId,
            compressionDictionary: new BackupCompressionDictionaryRef("dict-1", "dd"));
    }
}
