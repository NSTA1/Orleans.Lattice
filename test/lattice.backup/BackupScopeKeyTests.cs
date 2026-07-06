namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="BackupScopeKey"/>: the key is deterministic and
/// distinguishes scopes by kind, tree, and key / prefix, while treating a
/// null-vs-empty key/prefix identically.
/// </summary>
public sealed class BackupScopeKeyTests
{
    [Test]
    public void For_null_scope_throws()
    {
        Assert.That(() => BackupScopeKey.For(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void For_is_deterministic_for_the_same_scope()
    {
        var a = BackupScopeKey.For(BackupScopeSelector.WholeTree("orders"));
        var b = BackupScopeKey.For(BackupScopeSelector.WholeTree("orders"));

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void For_distinguishes_kind_tree_and_selector()
    {
        var wholeTree = BackupScopeKey.For(BackupScopeSelector.WholeTree("orders"));
        var otherTree = BackupScopeKey.For(BackupScopeSelector.WholeTree("customers"));
        var prefix = BackupScopeKey.For(BackupScopeSelector.Prefix("orders", "a:"));
        var key = BackupScopeKey.For(BackupScopeSelector.Key("orders", "a:"));

        Assert.Multiple(() =>
        {
            Assert.That(wholeTree, Is.Not.EqualTo(otherTree));
            Assert.That(wholeTree, Is.Not.EqualTo(prefix));
            // Same tree and selector text but different kind must differ.
            Assert.That(prefix, Is.Not.EqualTo(key));
        });
    }
}
