using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Proves the digest-read helper that fixes the anti-entropy remediation
/// self-block on deny-by-default (auth-enforced) clusters: both the probe's
/// local digest read and the replication gRPC service's peer-serving handlers
/// funnel through the fail-closed data-plane access gate, so without an ambient
/// identity they resolve to the anonymous subject and a deny-by-default tree
/// refuses them. <see cref="ReplicationSystemOriginDigestReader"/> reads under a
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope so the gate's
/// infrastructure bypass applies. These tests observe the ambient
/// <see cref="LatticeAccessGateContext.IsSystemOrigin"/> flag at the exact moment
/// the read is driven, and assert it is restored afterwards.
/// </summary>
[TestFixture]
public class ReplicationSystemOriginDigestReaderTests
{
    private static LeafProjectionDigest Digest(params byte[] hash)
        => new() { Hash = hash, EntryCount = hash.Length, CheckpointOffset = 1, Version = LeafProjectionDigest.CurrentVersion };

    [Test]
    public async Task ReadShardDigestAsync_reads_under_a_system_origin_scope()
    {
        var lattice = Substitute.For<ILattice>();
        bool? systemOriginDuringRead = null;
        lattice.GetLeafProjectionDigestAsync(7, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                systemOriginDuringRead = LatticeAccessGateContext.IsSystemOrigin;
                return Task.FromResult(Digest(1, 2, 3));
            });

        var result = await ReplicationSystemOriginDigestReader.ReadShardDigestAsync(
            lattice, 7, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(systemOriginDuringRead, Is.True, "the shard digest read must run under a system-origin scope");
            Assert.That(result.Hash, Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the scope must be restored after the read");
        });
    }

    [Test]
    public async Task ReadRangeDigestAsync_reads_the_requested_range_under_a_system_origin_scope()
    {
        var lattice = Substitute.For<ILattice>();
        bool? systemOriginDuringRead = null;
        lattice.GetLeafProjectionDigestForRangeAsync(2, "k010", "k090", Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                systemOriginDuringRead = LatticeAccessGateContext.IsSystemOrigin;
                return Task.FromResult(Digest(9, 9));
            });

        var result = await ReplicationSystemOriginDigestReader.ReadRangeDigestAsync(
            lattice, 2, "k010", "k090", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(systemOriginDuringRead, Is.True, "the range digest read must run under a system-origin scope");
            Assert.That(result.Hash, Is.EqualTo(new byte[] { 9, 9 }));
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the scope must be restored after the read");
        });

        await lattice.Received(1).GetLeafProjectionDigestForRangeAsync(
            2, "k010", "k090", Arg.Any<CancellationToken>());
    }

    [Test]
    public void ReadShardDigestAsync_throws_when_lattice_null()
        => Assert.ThrowsAsync<ArgumentNullException>(
            async () => await ReplicationSystemOriginDigestReader.ReadShardDigestAsync(
                null!, 0, CancellationToken.None));

    [Test]
    public void ReadRangeDigestAsync_throws_when_lattice_null()
        => Assert.ThrowsAsync<ArgumentNullException>(
            async () => await ReplicationSystemOriginDigestReader.ReadRangeDigestAsync(
                null!, 0, null, null, CancellationToken.None));
}
