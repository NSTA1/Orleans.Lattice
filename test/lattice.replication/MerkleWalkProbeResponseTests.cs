using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Tests for <see cref="MerkleWalkProbeResponse"/>.</summary>
[TestFixture]
public sealed class MerkleWalkProbeResponseTests
{
    [Test]
    public void Unavailable_reports_not_available()
    {
        var response = MerkleWalkProbeResponse.Unavailable;

        Assert.That(response.Available, Is.False);
    }

    [Test]
    public void Available_response_carries_digest()
    {
        var digest = new LeafProjectionDigest
        {
            Hash = new byte[] { 1, 2, 3 },
            EntryCount = 3,
            CheckpointOffset = 1,
            Version = LeafProjectionDigest.CurrentVersion,
        };

        var response = new MerkleWalkProbeResponse { Available = true, Digest = digest };

        Assert.That(response.Available, Is.True);
        Assert.That(response.Digest.EntryCount, Is.EqualTo(3));
        Assert.That(response.Digest.Hash, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }
}
