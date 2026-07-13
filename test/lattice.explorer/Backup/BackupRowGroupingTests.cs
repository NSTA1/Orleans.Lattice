using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Unit coverage for <see cref="BackupRowGrouping"/>: standalone backups pass
/// through as their own rows, backup-set members (sharing a non-null
/// <see cref="BackupManifest.SetId"/>) collapse into a single row that carries
/// every member, and the input's first-seen order is preserved.
/// </summary>
[TestFixture]
public sealed class BackupRowGroupingTests
{
    [Test]
    public void Group_null_manifests_throws()
    {
        Assert.Throws<ArgumentNullException>(() => BackupRowGrouping.Group(null!));
    }

    [Test]
    public void Group_standalone_backups_pass_through_as_their_own_rows()
    {
        var manifests = new[]
        {
            SampleBackup.Manifest("a", treeId: "t1"),
            SampleBackup.Manifest("b", treeId: "t2"),
        };

        var rows = BackupRowGrouping.Group(manifests);

        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(rows[0].IsSet, Is.False);
            Assert.That(rows[0].DisplayId, Is.EqualTo("a"));
            Assert.That(rows[0].Members, Has.Count.EqualTo(1));
            Assert.That(rows[1].DisplayId, Is.EqualTo("b"));
        });
    }

    [Test]
    public void Group_set_members_collapse_into_one_row_keyed_by_set_id()
    {
        var manifests = new[]
        {
            SampleBackup.Manifest("m1", treeId: "t1", setId: "set-1", setName: "my-set"),
            SampleBackup.Manifest("m2", treeId: "t2", setId: "set-1", setName: "my-set"),
        };

        var rows = BackupRowGrouping.Group(manifests);

        Assert.That(rows, Has.Count.EqualTo(1));
        var row = rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row.IsSet, Is.True);
            Assert.That(row.SetId, Is.EqualTo("set-1"));
            Assert.That(row.DisplayId, Is.EqualTo("set-1"));
            Assert.That(row.Name, Is.EqualTo("my-set"));
            Assert.That(row.Members.Select(m => m.Id), Is.EqualTo(new[] { "m1", "m2" }));
            Assert.That(row.TreeIds, Is.EqualTo(new[] { "t1", "t2" }));
        });
    }

    [Test]
    public void Group_set_row_carries_the_earliest_member_capture_time()
    {
        var early = DateTimeOffset.UnixEpoch;
        var late = DateTimeOffset.UnixEpoch.AddHours(1);
        var manifests = new[]
        {
            SampleBackup.Manifest("m1", treeId: "t1", setId: "set-1", setName: "s", createdAtUtc: late),
            SampleBackup.Manifest("m2", treeId: "t2", setId: "set-1", setName: "s", createdAtUtc: early),
        };

        var rows = BackupRowGrouping.Group(manifests);

        Assert.That(rows[0].CreatedAtUtc, Is.EqualTo(early));
    }

    [Test]
    public void Group_preserves_first_seen_order_across_mixed_standalone_and_set_rows()
    {
        var manifests = new[]
        {
            SampleBackup.Manifest("solo-1", treeId: "t0"),
            SampleBackup.Manifest("m1", treeId: "t1", setId: "set-1", setName: "s"),
            SampleBackup.Manifest("solo-2", treeId: "t2"),
            SampleBackup.Manifest("m2", treeId: "t3", setId: "set-1", setName: "s"),
        };

        var rows = BackupRowGrouping.Group(manifests);

        // The set row keeps the position of its first member (index 1); the second
        // member is merged in, not appended as a new row.
        Assert.That(rows.Select(r => r.DisplayId), Is.EqualTo(new[] { "solo-1", "set-1", "solo-2" }));
        Assert.That(rows[1].Members, Has.Count.EqualTo(2));
    }

    [Test]
    public void Group_distinct_sets_produce_distinct_rows()
    {
        var manifests = new[]
        {
            SampleBackup.Manifest("m1", treeId: "t1", setId: "set-1", setName: "s1"),
            SampleBackup.Manifest("m2", treeId: "t2", setId: "set-2", setName: "s2"),
        };

        var rows = BackupRowGrouping.Group(manifests);

        Assert.That(rows.Select(r => r.SetId), Is.EqualTo(new[] { "set-1", "set-2" }));
    }
}
