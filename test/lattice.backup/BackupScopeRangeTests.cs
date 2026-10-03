namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Covers <see cref="BackupScopeRange"/>, the scope-to-key-range mapping capture
/// and restore share.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class BackupScopeRangeTests
{
    [Test]
    public void Resolve_maps_each_scope_kind_to_its_half_open_range()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupScopeRange.Resolve(BackupScopeSelector.WholeTree("t")), Is.EqualTo(((string?)null, (string?)null)));
            Assert.That(BackupScopeRange.Resolve(BackupScopeSelector.Prefix("t", "p")), Is.EqualTo(("p", BackupConstants.PrefixUpperBound("p"))));
            Assert.That(BackupScopeRange.Resolve(BackupScopeSelector.Key("t", "k")), Is.EqualTo(("k", "k\0")));
        });
    }

    [Test]
    public void Resolve_rejects_an_unknown_scope_kind()
    {
        var scope = BackupScopeSelector.WholeTree("t") with { Kind = (BackupScopeKind)99 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => BackupScopeRange.Resolve(scope));

        Assert.That(ex!.ParamName, Is.EqualTo("scope"));
    }
}
