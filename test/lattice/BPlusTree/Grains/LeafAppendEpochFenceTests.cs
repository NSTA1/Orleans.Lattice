using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for a leaf activation's side of the WAL append fence (epic #5605):
/// the eager three-step claim, the strict conditional epoch row, the bump above a
/// retired floor, stale deactivation, the fail-safe fallback to an unstamped
/// append, and the own-append self-report.
/// </summary>
[TestFixture]
public sealed partial class LeafAppendEpochFenceTests
{
    private static readonly Guid Leaf = new("1e1e1e1e-0000-0000-0000-000000000001");

    private sealed class SettableGate(bool open) : IWalAppendFenceGate
    {
        public bool IsOpen { get; set; } = open;
    }

    private sealed class Harness
    {
        public EnumerableMemoryGrainStorage Storage { get; } = new();

        public SettableGate Gate { get; } = new(open: true);

        public IWalShardGrain Shard { get; } = Substitute.For<IWalShardGrain>();

        public List<long> Superseded { get; } = new();

        public LeafAppendEpochFence Fence { get; }

        public Harness()
        {
            Shard.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(0)));
            Fence = new LeafAppendEpochFence(
                Leaf,
                Gate,
                Storage,
                Superseded.Add,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
                LatticeTenantLabel.ForTree("tree"));
        }

        public async Task<long> RowEpochAsync() => (await WalAppendFenceRows.ReadLeafEpochAsync(Storage, Leaf)).State.Epoch;

        /// <summary>Another activation of the leaf claims: it bumps the epoch row behind this one's back.</summary>
        public async Task ClaimByAnotherActivationAsync()
        {
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, Leaf);
            row.State.Epoch++;
            await WalAppendFenceRows.WriteAsync(Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(Leaf), row);
        }
    }

    [Test]
    public async Task The_first_append_persists_the_epoch_row_and_claims_eagerly_at_the_partition()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(42)));

        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 3, CancellationToken.None);
        var again = h.Fence.StampForAsync(h.Shard, partition: 3, CancellationToken.None);
        var againCompletedSynchronously = again.IsCompletedSuccessfully;
        var againStamp = await again;
        var rowEpoch = await h.RowEpochAsync();

        Assert.Multiple(() =>
        {
            Assert.That(stamp, Is.EqualTo(WalAppendStamp.ForLeaf(Leaf, 1)));
            Assert.That(rowEpoch, Is.EqualTo(1));
            Assert.That(h.Fence.ClaimHead(3), Is.EqualTo(42));
            Assert.That(againCompletedSynchronously, Is.True, "a claimed partition stamps synchronously");
            Assert.That(againStamp?.Epoch, Is.EqualTo(1));
        });
        await h.Shard.Received(1).ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Each_new_partition_is_claimed_before_its_first_append()
    {
        var h = new Harness();

        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.Fence.StampForAsync(h.Shard, partition: 1, CancellationToken.None);

        await h.Shard.Received(2).ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>());
        Assert.That(await h.RowEpochAsync(), Is.EqualTo(1), "the epoch row is persisted once per activation");
    }

    [Test]
    public async Task A_closed_gate_appends_unstamped_and_claims_nothing()
    {
        var h = new Harness();
        h.Gate.IsOpen = false;

        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.That(stamp, Is.Null);
        await h.Shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
        Assert.That(h.Storage.GrainIds(WalAppendFenceRows.LeafEpochGrainType), Is.Empty);
    }

    [Test]
    public async Task A_refusal_from_the_retired_floor_raises_the_epoch_above_it()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Refuse(5)));

        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        var rowEpoch = await h.RowEpochAsync();

        Assert.Multiple(() =>
        {
            Assert.That(stamp?.Epoch, Is.EqualTo(6), "the claim is re-made above the refusal");
            Assert.That(rowEpoch, Is.EqualTo(6));
            Assert.That(h.Superseded, Is.Empty, "a refusal whose row has not moved is not a supersession");
        });
    }

    [Test]
    public async Task Refusals_bump_and_re_claim_for_at_most_the_round_cap_then_go_unstamped()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(WalAppendFenceClaimResult.Refuse(call.ArgAt<long>(1) + 10)));

        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stamp, Is.Null, "a partition that keeps refusing is appended to unstamped, never spun on");
            Assert.That(h.Superseded, Is.Empty, "refusals whose row has not moved are not a supersession");
            Assert.That(h.Fence.IsStale, Is.False);
        });
        await h.Shard.Received(LeafAppendEpochFence.MaxClaimRounds)
            .ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>());

        // #5605 review, S2: the next append goes unstamped at once, instead of paying
        // another eight rounds, until the backoff passes.
        var next = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        Assert.That(next, Is.Null);
        await h.Shard.Received(LeafAppendEpochFence.MaxClaimRounds)
            .ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_faulting_store_costs_one_claim_attempt_per_backoff_window()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("the partition's grain storage is faulting"));

        var stamps = new List<WalAppendStamp?>();
        for (var i = 0; i < 5; i++)
        {
            stamps.Add(await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None));
        }

        Assert.That(stamps, Is.All.Null, "every append goes unstamped, which is always admitted");
        await h.Shard.Received(1).ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>());

        h.Fence.ClaimFailureBackoffForTesting = TimeSpan.Zero;
        h.Fence.OnClaimsExhausted();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.Shard.Received(2).ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_synchronously_faulted_row_claim_is_retried_once_the_backoff_passes()
    {
        // #5605 review, N2: a row claim that faults synchronously must not be pinned
        // as the shared in-flight claim, or the activation could never stamp again.
        var storage = new FaultFirstWriteStorage(new EnumerableMemoryGrainStorage());
        var shard = Substitute.For<IWalShardGrain>();
        shard.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(0)));
        var fence = new LeafAppendEpochFence(
            Leaf, new SettableGate(open: true), storage, _ => { }, default, LatticeTenantLabel.ForTree("tree"))
        {
            ClaimFailureBackoffForTesting = TimeSpan.Zero,
        };

        var first = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);
        var second = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Null, "the faulted claim goes unstamped");
            Assert.That(second?.Epoch, Is.EqualTo(1), "the next claim runs afresh and succeeds");
        });
    }

    [Test]
    public async Task A_cleared_leaf_claims_afresh_without_being_superseded()
    {
        // #5605 review, N1: an activation that keeps serving after its own clear claims
        // as the re-created incarnation, instead of meeting its retired entries,
        // conflicting on its deleted row, and deactivating.
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(h.Storage, Leaf);
        await WalAppendFenceRows.ClearAsync(h.Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(Leaf), row);

        h.Fence.ResetAfterClear();
        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stamp?.Epoch, Is.EqualTo(1), "the re-created incarnation's first epoch");
            Assert.That(h.Fence.IsStale, Is.False);
            Assert.That(h.Superseded, Is.Empty);
        });
        await h.Shard.Received(2).ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>());
        Assert.That(await h.RowEpochAsync(), Is.EqualTo(1), "the row was re-created by a fresh claim");
    }

    [Test]
    public async Task Unloaded_rejections_past_the_cap_go_unstamped_and_never_supersede()
    {
        // #5605 review, N1: exhausting re-presentations is not a supersession.
        var h = new Harness();
        var shard = h.Shard;
        shard.AppendStampedAsync(Arg.Any<WalRecord>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalStampedAppendResult(WalStampedBatchResult.Rejected, -1, WalAppendFenceOutcome.UnloadedEntry)));
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(41L));

        var offset = await WalFencedAppendDispatch.AppendPointAsync(
            shard, new WalRecord(), batched: false, partition: 0, h.Fence, WalRoutingStamp.None, waitForDeadline: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offset, Is.EqualTo(41), "the append goes unstamped, which is always admitted");
            Assert.That(h.Fence.IsStale, Is.False);
            Assert.That(h.Superseded, Is.Empty, "an unloaded entry is not a supersession");
        });
    }

    private sealed class FaultFirstWriteStorage(Orleans.Storage.IGrainStorage inner) : Orleans.Storage.IGrainStorage
    {
        private bool _faulted;

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (!_faulted)
            {
                _faulted = true;
                throw new IOException("the leaf's grain storage is faulting");
            }

            return inner.WriteStateAsync(stateName, grainId, grainState);
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }

    [Test]
    public async Task A_conflict_on_the_epoch_row_supersedes_the_activation()
    {
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        // A newer activation claims: it bumps the row and the partition entry.
        await h.ClaimByAnotherActivationAsync();
        h.Shard.ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Refuse(2)));

        Assert.ThrowsAsync<LeafRetiredException>(
            async () => await h.Fence.StampForAsync(h.Shard, partition: 1, CancellationToken.None));
        Assert.ThrowsAsync<LeafRetiredException>(
            async () => await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None),
            "a superseded activation stamps nothing more, even at a partition it had claimed");
        Assert.Multiple(() =>
        {
            Assert.That(h.Fence.IsStale, Is.True);
            Assert.That(h.Superseded, Is.EqualTo(new[] { 1L }), "the activation is deactivated exactly once");
        });
    }

    [Test]
    public async Task Two_activations_racing_to_create_the_epoch_row_leave_exactly_one_live()
    {
        var h = new Harness();
        var other = new LeafAppendEpochFence(
            Leaf, h.Gate, h.Storage, _ => { }, default, LatticeTenantLabel.ForTree("tree"));

        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        // The other activation read the row before the first one created it: model it
        // by a conditional create that finds the row present.
        var stale = new GrainState<LeafAppendEpochState>(new LeafAppendEpochState { Epoch = 1 });
        Assert.ThrowsAsync<Orleans.Storage.InconsistentStateException>(() => WalAppendFenceRows.WriteAsync(
            h.Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(Leaf), stale),
            "an empty-ETag write over an existing row conflicts: the claim never adopts the winner's row (#5593)");

        // The other activation's own claim reads the current row and goes above it.
        var otherStamp = await other.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        Assert.That(otherStamp?.Epoch, Is.EqualTo(2));
    }

    [Test]
    public async Task A_rejected_append_as_stale_supersedes_the_activation()
    {
        var h = new Harness();
        var stamp = (await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None))!.Value;

        var exception = h.Fence.OnStale(stamp);
        h.Fence.OnStale(stamp);

        Assert.Multiple(() =>
        {
            Assert.That(exception.ActivationSuperseded, Is.True);
            Assert.That(exception.LeafId, Is.EqualTo(Leaf.ToString("N")));
            Assert.That(exception.SupersededEpoch, Is.EqualTo(1));
            Assert.That(h.Superseded, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_supersession_reaches_callers_as_the_retired_leaf_refusal_10_2_0_retries()
    {
        // #5605 review, S1: a 10.2.0 shard root can call a leaf on a newer silo while
        // the gate races a rolling upgrade or rollback. What it receives must be a type
        // it already knows and retries, never one only the newer build can resolve.
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        await h.ClaimByAnotherActivationAsync();
        h.Shard.ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Refuse(2)));

        var thrown = Assert.CatchAsync(async () => await h.Fence.StampForAsync(h.Shard, partition: 1, CancellationToken.None));
        var alias = typeof(LeafRetiredException).GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>().Single().Alias;

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.TypeOf<LeafRetiredException>(),
                "the refusal crosses the wire as the type 10.2.0's ShouldRetryLeafDispatch retries until its deadline");
            Assert.That(((LeafRetiredException)thrown!).ActivationSuperseded, Is.True,
                "a newer caller tells it from a reclaimed leaf by the trailing field 10.2.0 skips");
            Assert.That(alias, Is.EqualTo("ol.lrt"), "the alias 10.2.0 resolves, unchanged");
        });
    }

    [Test]
    public async Task An_unloaded_rejection_re_presents_the_same_epoch()
    {
        var h = new Harness();
        var stamp = (await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None))!.Value;

        h.Fence.OnUnloaded(0, stamp);
        var again = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.That(again, Is.EqualTo(stamp));
        await h.Shard.Received(2).ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_claim_fault_falls_back_to_an_unstamped_append()
    {
        var h = new Harness();
        h.Shard.ClaimAppendEpochAsync(Leaf, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("claim timed out"));

        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stamp, Is.Null, "an append that cannot be stamped goes unstamped, which is always admitted");
            Assert.That(h.Fence.IsStale, Is.False);
        });
    }

    [Test]
    public void Own_unstamped_appends_report_themselves()
    {
        var h = new Harness();

        h.Fence.OnUnstampedAppended(0, 9);
        h.Fence.OnStampedAppended(0, 4);

        Assert.That(h.Fence.ReportedUnstamped(0), Is.EqualTo(9),
            "ReadPositionHonestNoSelfReport: an own unstamped append raises the reported watermark");
    }

    [Test]
    public void Stamped_responses_raise_the_reported_watermark()
    {
        var h = new Harness();

        h.Fence.OnStampedAppended(2, 11);

        Assert.Multiple(() =>
        {
            Assert.That(h.Fence.ReportedUnstamped(2), Is.EqualTo(11));
            Assert.That(h.Fence.ReportedUnstamped(3), Is.EqualTo(-1));
        });
    }

    [Test]
    public async Task A_method_unknown_failure_closes_the_activation_view_of_the_gate()
    {
        var h = new Harness();

        h.Fence.OnStampedMethodUnknown();
        var stamp = await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);

        Assert.That(stamp, Is.Null);
    }

    [Test]
    public async Task Concurrent_appends_to_one_partition_share_one_claim()
    {
        var h = new Harness();
        var pending = new TaskCompletionSource<WalAppendFenceClaimResult>();
        h.Shard.ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>()).Returns(pending.Task);

        var first = h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None).AsTask();
        var second = h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None).AsTask();
        await Task.Delay(50);
        pending.SetResult(WalAppendFenceClaimResult.Accept(0));
        var stamps = await Task.WhenAll(first, second);

        Assert.That(stamps, Is.All.EqualTo(WalAppendStamp.ForLeaf(Leaf, 1)));
        await h.Shard.Received(1).ClaimAppendEpochAsync(Leaf, 1, Arg.Any<CancellationToken>());
    }
}
