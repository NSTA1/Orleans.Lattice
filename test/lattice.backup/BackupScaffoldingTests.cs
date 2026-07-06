using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Scaffolding sanity checks for the <c>Orleans.Lattice.Backup</c> package: the
/// assembly loads, the reserved system-tree prefix guard behaves, and the
/// reserved serialization-alias prefix is stable. These guard the reservations
/// that later backup releases depend on.
/// </summary>
public sealed class BackupScaffoldingTests
{
    [Test]
    public void Reserved_tree_prefix_is_the_backup_system_namespace()
    {
        Assert.That(LatticeBackupReservedTrees.Prefix, Is.EqualTo("sys-backup-"));
    }

    [Test]
    public void IsReserved_returns_true_for_a_backup_namespaced_tree()
    {
        Assert.That(LatticeBackupReservedTrees.IsReserved("sys-backup-catalog"), Is.True);
    }

    [Test]
    public void IsReserved_returns_false_for_an_application_tree()
    {
        Assert.That(LatticeBackupReservedTrees.IsReserved("orders"), Is.False);
    }

    [Test]
    public void IsReserved_throws_on_null()
    {
        Assert.That(() => LatticeBackupReservedTrees.IsReserved(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ThrowIfReserved_throws_for_a_reserved_tree()
    {
        Assert.That(
            () => LatticeBackupReservedTrees.ThrowIfReserved("sys-backup-catalog"),
            Throws.ArgumentException);
    }

    [Test]
    public void ThrowIfReserved_returns_for_an_application_tree()
    {
        Assert.That(() => LatticeBackupReservedTrees.ThrowIfReserved("orders"), Throws.Nothing);
    }

    [Test]
    public void Alias_prefix_is_the_reserved_backup_namespace()
    {
        Assert.That(BackupTypeAliases.AliasPrefix, Is.EqualTo("olb."));
    }
}
