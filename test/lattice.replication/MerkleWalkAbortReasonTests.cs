using NUnit.Framework;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Tests for <see cref="MerkleWalkAbortReason"/>.</summary>
[TestFixture]
public sealed class MerkleWalkAbortReasonTests
{
    [Test]
    public void None_is_the_default_value()
    {
        Assert.That(default(MerkleWalkAbortReason), Is.EqualTo(MerkleWalkAbortReason.None));
    }

    [Test]
    public void Defines_all_documented_abort_reasons()
    {
        Assert.That(System.Enum.GetValues<MerkleWalkAbortReason>(), Is.EquivalentTo(new[]
        {
            MerkleWalkAbortReason.None,
            MerkleWalkAbortReason.DepthCapExceeded,
            MerkleWalkAbortReason.ByteBudgetExceeded,
            MerkleWalkAbortReason.RemoteUnavailable,
            MerkleWalkAbortReason.VersionSkew,
        }));
    }
}
