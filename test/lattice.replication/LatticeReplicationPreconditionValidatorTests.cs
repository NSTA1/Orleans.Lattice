using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for the reusable runtime precondition seam
/// (<see cref="LatticeReplicationPreconditionValidator"/> and
/// <see cref="LatticeReplicationPreconditionResult"/>) that the later enable
/// path (#1315) calls to reject an unsafe flag-mode enable cleanly, instead of
/// failing the whole silo at boot.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationPreconditionValidatorTests
{
    private static LatticeReplicationPreconditionValidator Validator(string localReplicaId)
    {
        var ctx = Substitute.For<ILatticeReplicationContext>();
        ctx.LocalReplicaId.Returns(localReplicaId);
        return new LatticeReplicationPreconditionValidator(ctx);
    }

    [Test]
    public void Validate_rejects_or_flag_mode_without_a_local_replica_id()
    {
        var result = Validator(string.Empty).Validate("tag-orders", LatticeMergeMode.OrFlag);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSatisfied, Is.False);
            Assert.That(result.FailureReason, Is.Not.Null.And.Contains("tag-orders"));
        });
    }

    [Test]
    public void Validate_rejects_rw_flag_mode_without_a_local_replica_id()
    {
        var result = Validator(string.Empty).Validate("tag-orders", LatticeMergeMode.RwFlag);

        Assert.That(result.IsSatisfied, Is.False);
    }

    [Test]
    public void Validate_accepts_flag_mode_when_a_local_replica_id_is_configured()
    {
        var result = Validator("site-a").Validate("tag-orders", LatticeMergeMode.OrFlag);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSatisfied, Is.True);
            Assert.That(result.FailureReason, Is.Null);
        });
    }

    [Test]
    public void Validate_accepts_non_flag_mode_without_a_local_replica_id()
    {
        var result = Validator(string.Empty).Validate("orders", LatticeMergeMode.LwwRegister);

        Assert.That(result.IsSatisfied, Is.True);
    }

    [Test]
    public void Validate_accepts_bounded_register_modes_without_a_local_replica_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Validator(string.Empty).Validate("gauge", LatticeMergeMode.MaxRegister).IsSatisfied, Is.True);
            Assert.That(Validator(string.Empty).Validate("floor", LatticeMergeMode.MinRegister).IsSatisfied, Is.True);
        });
    }

    [Test]
    public void Validate_throws_on_null_or_empty_tree_id()
    {
        var validator = Validator("site-a");

        Assert.Multiple(() =>
        {
            Assert.That(() => validator.Validate(null!, LatticeMergeMode.LwwRegister), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => validator.Validate(string.Empty, LatticeMergeMode.LwwRegister), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Satisfied_result_has_no_failure_reason()
    {
        var result = LatticeReplicationPreconditionResult.Satisfied;

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSatisfied, Is.True);
            Assert.That(result.FailureReason, Is.Null);
        });
    }

    [Test]
    public void Rejected_result_carries_the_reason()
    {
        var result = LatticeReplicationPreconditionResult.Rejected("boom");

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSatisfied, Is.False);
            Assert.That(result.FailureReason, Is.EqualTo("boom"));
        });
    }

    [Test]
    public void Rejected_throws_on_empty_reason()
    {
        Assert.That(() => LatticeReplicationPreconditionResult.Rejected(string.Empty), Throws.ArgumentException);
    }
}
