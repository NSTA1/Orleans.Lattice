namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupIdentity"/>: a backup id is unique to its
/// capture, so two captures of identical bytes never share one (issue #5418).
/// </summary>
[TestFixture]
public sealed class BackupIdentityTests
{
    [Test]
    public void Two_captures_of_identical_content_get_distinct_ids()
    {
        var first = BackupIdentity.Compute(null, "cafef00d", "orders-WholeTree-1-aaaa");
        var second = BackupIdentity.Compute(null, "cafef00d", "orders-WholeTree-2-bbbb");

        Assert.That(second, Is.Not.EqualTo(first));
    }

    [Test]
    public void The_id_folds_in_the_base_of_an_increment()
    {
        var full = BackupIdentity.Compute(null, "cafef00d", "art");
        var increment = BackupIdentity.Compute("base", "cafef00d", "art");

        Assert.That(increment, Is.Not.EqualTo(full));
    }

    [Test]
    public void The_id_is_a_deterministic_lowercase_hex_sha256()
    {
        var id = BackupIdentity.Compute("base", "cafef00d", "art");

        Assert.Multiple(() =>
        {
            Assert.That(id, Does.Match("^[0-9a-f]{64}$"));
            Assert.That(BackupIdentity.Compute("base", "cafef00d", "art"), Is.EqualTo(id));
        });
    }

    [Test]
    public void An_empty_content_hash_or_artifact_id_is_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupIdentity.Compute(null, "", "art"), Throws.ArgumentException);
            Assert.That(() => BackupIdentity.Compute(null, "cafef00d", ""), Throws.ArgumentException);
        });
    }
}