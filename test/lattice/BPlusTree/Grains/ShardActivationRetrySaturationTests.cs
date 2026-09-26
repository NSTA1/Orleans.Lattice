using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for #3294: the replay-permit admission refusal must be
/// retried by the shard-dispatch envelope, and it must be the <b>only</b>
/// saturation source that is.
/// <para>
/// The refusal aborts a grain <em>activation</em>. An activation that fails
/// has no queue to park on and no retry policy of its own, so before this arm
/// existed the admission bound did not shed the request, it failed it: a
/// request that would have succeeded slowly became a hard error at the call
/// site.
/// </para>
/// <para>
/// The negative tests are not symmetry for its own sake. Append writes reach
/// this same envelope - <c>LatticeGrain.SetManyAsyncCore</c> wraps every
/// per-shard dispatch in it - so a retry filtered on the exception
/// <em>type</em> rather than on
/// <see cref="LatticeSaturatedException.SaturationSource"/> would re-fan a
/// whole batch across every shard of a saturated tree, reintroducing #3348 one
/// layer below where it was fixed. Each negative test pins one seam against
/// that.
/// </para>
/// </summary>
[TestFixture]
public class ShardActivationRetrySaturationTests
{
    private static LatticeSaturatedException Refusal(LatticeSaturationSource source)
        => new($"refused by {source}", "tree-1", source);

    [Test]
    public async Task RunAsync_retries_a_replay_permit_refusal_then_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw Refusal(LatticeSaturationSource.ReplayPermitAdmission);
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(2),
            "a replay-permit refusal is raised before the caller does any work, so it is the one "
            + "saturation source a retry does not amplify");
    }

    [Test]
    public async Task RunAsync_generic_overload_retries_a_replay_permit_refusal()
    {
        var calls = 0;
        var result = await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw Refusal(LatticeSaturationSource.ReplayPermitAdmission);
            return Task.FromResult(42);
        });

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(result, Is.EqualTo(42));
        });
    }

    [Test]
    public void RunAsync_exhausts_its_budget_on_a_persistent_replay_permit_refusal()
    {
        var calls = 0;
        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw Refusal(LatticeSaturationSource.ReplayPermitAdmission);
            }));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(ShardActivationRetry.MaxAttempts),
                "the retry must stay bounded; an unbounded one would be the regenerative loop #3284 removed");
            Assert.That(thrown!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission),
                "on exhaustion the original refusal surfaces, so the caller sees the real reason");
        });
    }

    /// <summary>
    /// The #3348-preservation test. A WAL append refusal reaches this envelope
    /// on every batch write, so retrying it here would re-offer the whole
    /// batch into the tree that just refused it.
    /// </summary>
    [Test]
    public void RunAsync_does_not_retry_a_wal_admission_refusal()
    {
        var calls = 0;
        Assert.ThrowsAsync<LatticeSaturatedException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw Refusal(LatticeSaturationSource.WalAdmission);
            }));

        Assert.That(calls, Is.EqualTo(1),
            "retrying a WAL append refusal re-fans the batch across every shard of a saturated tree (#3348)");
    }

    [Test]
    public void RunAsync_does_not_retry_an_atomic_write_saga_refusal()
    {
        var calls = 0;
        Assert.ThrowsAsync<LatticeSaturatedException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw Refusal(LatticeSaturationSource.AtomicWriteSaga);
            }));

        Assert.That(calls, Is.EqualTo(1),
            "the saga refuses precisely to avoid re-issuing RowKeys into a back-pressured account");
    }

    [Test]
    public void RunAsync_does_not_retry_a_snapshot_cursor_refusal()
    {
        var calls = 0;
        Assert.ThrowsAsync<LatticeSaturatedException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw Refusal(LatticeSaturationSource.SnapshotCursorOpen);
            }));

        Assert.That(calls, Is.EqualTo(1),
            "the snapshot open is deliberate load shedding; retrying it defeats the shed");
    }

    /// <summary>
    /// An unattributed refusal - a legacy throw site, or one deserialised from
    /// a host predating the discriminator - must not be retried. Three of the
    /// four known seams amplify, so refusing to retry is the conservative
    /// reading of an unknown one.
    /// </summary>
    [Test]
    public void RunAsync_does_not_retry_an_unspecified_refusal()
    {
        var calls = 0;
        Assert.ThrowsAsync<LatticeSaturatedException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw new LatticeSaturatedException("legacy refusal", "tree-1");
            }));

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void IsRetryableSaturation_finds_a_wrapped_replay_permit_refusal()
    {
        var wrapped = new InvalidOperationException(
            "grain call failed", Refusal(LatticeSaturationSource.ReplayPermitAdmission));

        Assert.That(ShardActivationRetry.IsRetryableSaturation(wrapped), Is.True,
            "Orleans wraps exceptions crossing a grain boundary, so the inner chain must be walked");
    }

    /// <summary>
    /// The walk stops at the outermost <see cref="LatticeSaturatedException"/>
    /// rather than hunting the chain for a retryable one. The outer refusal is
    /// the one that describes what actually happened; letting an inner,
    /// already-handled refusal license a retry of an outer refusal that
    /// forbids it is how a "search for any retryable" reading would
    /// reintroduce the amplification.
    /// </summary>
    [Test]
    public void IsRetryableSaturation_stops_at_the_outermost_saturation()
    {
        var outerForbids = new LatticeSaturatedException(
            "append refused",
            "tree-1",
            LatticeSaturationSource.WalAdmission,
            Refusal(LatticeSaturationSource.ReplayPermitAdmission));

        Assert.That(ShardActivationRetry.IsRetryableSaturation(outerForbids), Is.False);
    }

    [Test]
    public void IsRetryableSaturation_is_false_for_an_unrelated_exception()
        => Assert.That(ShardActivationRetry.IsRetryableSaturation(new TimeoutException()), Is.False);

    /// <summary>
    /// The batch fan-out seam refuses *above* the routing layer, after the
    /// batch has already been split across shards. Retrying it down here
    /// would re-fan the whole batch across every shard of a tree that just
    /// told us it cannot keep up - precisely the amplification
    /// <see href="https://github.com/NSTA1/Orleans.Lattice/issues/3348">#3348</see>
    /// removed. The allow-list shape of <c>IsRetryableSaturation</c> excludes
    /// it by construction; this pins that, so a future edit that widens the
    /// predicate to a deny-list has to fail a test rather than silently
    /// reintroduce the amplification.
    /// </summary>
    [Test]
    public void IsRetryableSaturation_is_false_for_a_batch_fan_out_refusal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ShardActivationRetry.IsRetryableSaturation(Refusal(LatticeSaturationSource.SetManyFanOut)),
                Is.False,
                "retrying a fan-out refusal below the routing layer re-fans the batch across every shard");

            Assert.That(
                ShardActivationRetry.IsRetryableSaturation(
                    new InvalidOperationException("wrapped", Refusal(LatticeSaturationSource.SetManyFanOut))),
                Is.False,
                "the exclusion must hold through an inner chain too");
        });
    }

    /// <summary>
    /// The transaction-registry capacity refusal (issue #3475) frees capacity
    /// only as tombstones age out of the retention window, which takes seconds,
    /// so an immediate shard-dispatch retry could never succeed and would only
    /// burn the retry budget. The retry belongs to the caller, after back-off.
    /// </summary>
    [Test]
    public void IsRetryableSaturation_is_false_for_a_tx_registry_capacity_refusal()
        => Assert.That(
            ShardActivationRetry.IsRetryableSaturation(Refusal(LatticeSaturationSource.TxRegistryCapacity)),
            Is.False);
}
