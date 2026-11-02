using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="LatticeRowOrigin"/>, the single seam that turns a
/// WAL record's origin into the origin a rebuilt row stores (issue #5524): an
/// own write's stamp becomes null, as a live own write stores it, and a peer's
/// id is kept.
/// </summary>
[TestFixture]
public sealed class LatticeRowOriginTests
{
    [Test]
    public void The_local_cluster_stamp_of_an_own_write_becomes_null() =>
        Assert.That(LatticeRowOrigin.FromRecord("west", "west"), Is.Null);

    [Test]
    public void The_empty_single_cluster_stamp_becomes_null() =>
        Assert.That(LatticeRowOrigin.FromRecord(string.Empty, string.Empty), Is.Null);

    [Test]
    public void An_unstamped_record_stays_null() =>
        Assert.Multiple(() =>
        {
            Assert.That(LatticeRowOrigin.FromRecord(null, "west"), Is.Null);
            Assert.That(LatticeRowOrigin.FromRecord(null, null), Is.Null);
        });

    [Test]
    public void A_peer_stamp_is_kept() =>
        Assert.Multiple(() =>
        {
            Assert.That(LatticeRowOrigin.FromRecord("east", "west"), Is.EqualTo("east"));
            Assert.That(LatticeRowOrigin.FromRecord("east", string.Empty), Is.EqualTo("east"));
            Assert.That(LatticeRowOrigin.FromRecord("east", null), Is.EqualTo("east"));
        });

    [Test]
    public void The_comparison_is_ordinal() =>
        Assert.That(LatticeRowOrigin.FromRecord("West", "west"), Is.EqualTo("West"));
}