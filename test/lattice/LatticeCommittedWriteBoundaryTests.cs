using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeCommittedWriteBoundary"/>, the
/// scope that detaches a side write (a schema dead-letter append) from the write
/// that caused it, so the side write is an ordinary committed write (issue #5297).
/// </summary>
[TestFixture]
public class LatticeCommittedWriteBoundaryTests
{
    private static readonly string[] WriteShapingKeys =
    [
        LatticeEventConstants.TransactionIdRequestContextKey,
        LatticeEventConstants.PreparedRequestContextKey,
        LatticeEventConstants.ForwardedPrepareRequestContextKey,
        LatticeEventConstants.AtomicBatchRequestContextKey,
        LatticeEventConstants.AtomicShardCountRequestContextKey,
        LatticeEventConstants.DeltaRequestContextKey,
        LatticeEventConstants.OriginClusterIdRequestContextKey,
        LatticeEventConstants.VectorClockRequestContextKey,
        LatticeEventConstants.HlcOverrideRequestContextKey,
        LatticeEventConstants.ApplyOffsetRequestContextKey,
        LatticeEventConstants.CrdtJoinMergeRequestContextKey,
        LatticeEventConstants.PredicateRequestContextKey,
        LatticeEventConstants.IdempotencyKeyRequestContextKey,
    ];

    [SetUp]
    [TearDown]
    public void EnsureCleanContext() => RequestContext.Clear();

    [Test]
    public void Enter_returns_the_shared_no_op_when_no_write_shaping_entry_is_present()
    {
        LatticeAccessGateContext.EnterSystemOrigin();

        using var scope = LatticeCommittedWriteBoundary.Enter();

        Assert.That(scope, Is.SameAs(NoOpDisposable.Instance));
    }

    [Test]
    public void Enter_clears_every_write_shaping_entry_and_restores_them_on_dispose()
    {
        foreach (var key in WriteShapingKeys)
        {
            RequestContext.Set(key, key);
        }

        using (LatticeCommittedWriteBoundary.Enter())
        {
            Assert.Multiple(() =>
            {
                foreach (var key in WriteShapingKeys)
                {
                    Assert.That(RequestContext.Get(key), Is.Null, key);
                }
            });
        }

        Assert.Multiple(() =>
        {
            foreach (var key in WriteShapingKeys)
            {
                Assert.That(RequestContext.Get(key), Is.EqualTo(key), key);
            }
        });
    }

    [Test]
    public void Enter_detaches_a_prepare_so_a_write_inside_it_is_not_prepared_or_transactional()
    {
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        using var prepare = LatticePreparedContext.BeginScope();

        using (LatticeCommittedWriteBoundary.Enter())
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticePreparedContext.Current, Is.False);
                Assert.That(LatticeTransactionContext.Current, Is.EqualTo(Guid.Empty));
            });
        }

        Assert.Multiple(() =>
        {
            Assert.That(LatticePreparedContext.Current, Is.True, "the enclosing prepare is restored");
            Assert.That(LatticeTransactionContext.Current, Is.EqualTo(txid), "the enclosing transaction is restored");
        });
    }

    [Test]
    public void Enter_preserves_the_identity_of_the_containing_call()
    {
        RequestContext.Set(LatticeEventConstants.PreparedRequestContextKey, true);
        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        using var maintenance = LatticeMaintenanceContext.BeginScope();

        using (LatticeCommittedWriteBoundary.Enter())
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
                Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.Maintenance));
            });
        }
    }

    [Test]
    public void Dispose_is_idempotent_and_does_not_overwrite_a_later_value()
    {
        RequestContext.Set(LatticeEventConstants.PreparedRequestContextKey, true);
        var scope = LatticeCommittedWriteBoundary.Enter();
        scope.Dispose();
        RequestContext.Remove(LatticeEventConstants.PreparedRequestContextKey);

        scope.Dispose();

        Assert.That(RequestContext.Get(LatticeEventConstants.PreparedRequestContextKey), Is.Null);
    }
}
