namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the backup-catalog index building blocks: the
/// <see cref="BackupCatalogIndexKey"/> ordering / set-contiguity scheme and the
/// <see cref="BackupCatalogIndexProjection"/> lowering of a catalog mutation into
/// a compact index row (and its deliberate silence on deletes).
/// </summary>
[TestFixture]
public sealed class BackupCatalogIndexTests
{
    private static BackupManifest Manifest(
        string id,
        DateTimeOffset createdAtUtc,
        string tree = "orders",
        BackupKind kind = BackupKind.Full,
        string? setId = null,
        string? setName = null,
        DateTimeOffset? setCreatedAtUtc = null,
        string? baseBackupId = null) =>
        BackupManifestModelTests.Sample(id: id, kind: kind, baseBackupId: baseBackupId) with
        {
            CreatedAtUtc = createdAtUtc,
            Scope = BackupScopeSelector.WholeTree(tree),
            SetId = setId,
            SetName = setName,
            SetCreatedAtUtc = setCreatedAtUtc,
        };

    private static LatticeMutation SetMutation(BackupManifest manifest) => new()
    {
        TreeId = BackupConstants.CatalogTree,
        Kind = MutationKind.Set,
        Key = manifest.Id,
        Value = JsonLatticeSerializer<BackupManifest>.Default.Serialize(manifest),
        Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
    };

    [Test]
    public void SetCreatedAtUtc_round_trips_through_a_with_expression()
    {
        var stamp = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var manifest = BackupManifestModelTests.Sample() with { SetCreatedAtUtc = stamp };
        Assert.That(manifest.SetCreatedAtUtc, Is.EqualTo(stamp));
    }

    [Test]
    public void A_newer_backup_sorts_before_an_older_one()
    {
        var older = BackupCatalogIndexKey.Encode(Manifest("a", DateTimeOffset.UnixEpoch));
        var newer = BackupCatalogIndexKey.Encode(Manifest("b", DateTimeOffset.UnixEpoch.AddHours(1)));

        // A forward ordinal scan must yield the newer backup first.
        Assert.That(string.CompareOrdinal(newer, older), Is.LessThan(0));
    }

    [Test]
    public void Set_members_share_the_group_prefix_and_are_contiguous()
    {
        var setCreated = DateTimeOffset.UnixEpoch.AddHours(5);
        var m1 = Manifest("m1", DateTimeOffset.UnixEpoch.AddHours(5), tree: "orders", setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated);
        var m2 = Manifest("m2", DateTimeOffset.UnixEpoch.AddHours(6), tree: "customers", setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated);

        var k1 = BackupCatalogIndexKey.Encode(m1);
        var k2 = BackupCatalogIndexKey.Encode(m2);

        // Both members share the {ticks}\u001f{groupId} prefix (first two segments)
        // even though their own capture times differ, so they scan contiguously.
        var prefix1 = string.Join('\u001f', k1.Split('\u001f')[..2]);
        var prefix2 = string.Join('\u001f', k2.Split('\u001f')[..2]);
        Assert.That(prefix1, Is.EqualTo(prefix2));

        // The full keys still differ by the trailing backup id, so neither shadows
        // the other in the index tree.
        Assert.That(k1, Is.Not.EqualTo(k2));
    }

    [Test]
    public void Project_set_emits_one_rekeyed_row_carrying_the_filter_fields()
    {
        var manifest = Manifest("cafef00d", DateTimeOffset.UnixEpoch.AddMinutes(3), tree: "orders", kind: BackupKind.Full);

        var projection = new BackupCatalogIndexProjection();
        var writes = projection.Project(SetMutation(manifest)).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        var write = writes[0];
        Assert.That(write.Kind, Is.EqualTo(ViewWriteKind.Upsert));
        Assert.That(write.Key, Is.EqualTo(BackupCatalogIndexKey.Encode(manifest)));
        Assert.That(write.SourceKey, Is.Null, "the index key is value-derived and must skip the re-key collision detector");

        var row = JsonLatticeSerializer<BackupCatalogIndexRow>.Default.Deserialize(write.Value!);
        Assert.Multiple(() =>
        {
            Assert.That(row.BackupId, Is.EqualTo("cafef00d"));
            Assert.That(row.Kind, Is.EqualTo(BackupKind.Full));
            Assert.That(row.TreeId, Is.EqualTo("orders"));
            Assert.That(row.CreatedAtUtc, Is.EqualTo(manifest.CreatedAtUtc));
        });
    }

    [Test]
    public void Project_set_member_carries_the_set_identity()
    {
        var manifest = Manifest("m1", DateTimeOffset.UnixEpoch, setId: "set-1", setName: "nightly", setCreatedAtUtc: DateTimeOffset.UnixEpoch);

        var writes = new BackupCatalogIndexProjection().Project(SetMutation(manifest)).ToList();

        var row = JsonLatticeSerializer<BackupCatalogIndexRow>.Default.Deserialize(writes[0].Value!);
        Assert.Multiple(() =>
        {
            Assert.That(row.SetId, Is.EqualTo("set-1"));
            Assert.That(row.SetName, Is.EqualTo("nightly"));
            Assert.That(row.DisplayName, Is.EqualTo("nightly"), "the display name prefers the set name");
        });
    }

    [Test]
    public void Project_delete_emits_nothing()
    {
        var mutation = new LatticeMutation
        {
            TreeId = BackupConstants.CatalogTree,
            Kind = MutationKind.Delete,
            Key = "gone",
            Timestamp = new HybridLogicalClock { WallClockTicks = 2 },
        };

        Assert.That(new BackupCatalogIndexProjection().Project(mutation), Is.Empty);
    }

    [Test]
    public void Project_range_delete_emits_nothing()
    {
        var mutation = new LatticeMutation
        {
            TreeId = BackupConstants.CatalogTree,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            MatchedKeys = new[] { "a", "b" },
            Timestamp = new HybridLogicalClock { WallClockTicks = 2 },
        };

        Assert.That(new BackupCatalogIndexProjection().Project(mutation), Is.Empty);
    }

    [Test]
    public void ProjectionVersion_is_the_stable_identity() =>
        Assert.That(new BackupCatalogIndexProjection().ProjectionVersion, Is.EqualTo(BackupCatalogIndexProjection.Version));
}
