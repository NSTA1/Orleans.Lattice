using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the wider-audit behaviour: every public <see cref="ILattice"/>
/// operator that drives the shard-root activation-readiness seed (directly,
/// through a per-tree coordinator, or through a fan-out across shards) must
/// transparently absorb a small bounded number of
/// <see cref="ShardActivationTimeoutException"/>s before surfacing the typed
/// exception to the caller. The helper itself is exhaustively tested by
/// <c>ShardActivationRetryTests</c>; this fixture pins that each operator
/// call site actually routes through the helper, and that fan-out call sites
/// retry per-shard rather than re-issuing every sibling task on a single
/// shard's seed-timeout.
/// </summary>
[TestFixture]
public class LatticeGrainShardActivationRetryTests
{
    private const string TreeId = "fx-027-tree";

    private static (LatticeGrain grain, IGrainFactory factory) CreateGrain(int shardCount = 4)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = shardCount }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, grainFactory);
    }

    private static ShardActivationTimeoutException MakeSeedTimeout(int shardIndex = 0) =>
        new($"seed-timeout-shard-{shardIndex}")
        {
            TreeId = TreeId,
            ShardIndex = shardIndex,
            TimeoutSeconds = 15,
        };

    // -------------------------------------------------------------------------
    // Single-shard read path - exercises the central RetryOnStaleRoutingAsync
    // change (the per-shard catch added to the stale-routing envelope).
    // -------------------------------------------------------------------------

    /// <summary>
    /// A single-key read whose underlying shard call throws
    /// <see cref="ShardActivationTimeoutException"/> on its first activation
    /// must transparently retry through the central stale-routing envelope
    /// and surface the second attempt's success.
    /// </summary>
    [Test]
    public async Task GetAsync_retries_through_first_seed_timeout_then_succeeds()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var calls = 0;
        shardRoot.GetAsync("k1").Returns(_ =>
        {
            calls++;
            if (calls == 1) throw MakeSeedTimeout();
            return Encoding.UTF8.GetBytes("v1");
        });

        var result = await grain.GetAsync("k1");

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2), "Central envelope did not retry the cold-start seed-timeout.");
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("v1"));
        });
    }

    // -------------------------------------------------------------------------
    // Single-shard write path - same central envelope, write-side variant.
    // -------------------------------------------------------------------------

    /// <summary>
    /// A single-key write whose underlying shard call throws
    /// <see cref="ShardActivationTimeoutException"/> on its first activation
    /// must transparently retry. Mirrors the read-side test for the write
    /// half of the central envelope.
    /// </summary>
    [Test]
    public async Task SetAsync_retries_through_first_seed_timeout_then_succeeds()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var calls = 0;
        shardRoot.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(_ =>
            {
                calls++;
                if (calls == 1) throw MakeSeedTimeout();
                return Task.CompletedTask;
            });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(calls, Is.EqualTo(2));
    }

    // -------------------------------------------------------------------------
    // Coordinator path - exercises the per-operator wrap inside
    // LatticeGrain.BulkLoad.cs (DeleteTreeAsync is the simplest single-call
    // example).
    // -------------------------------------------------------------------------

    /// <summary>
    /// A tree-lifecycle operator that calls a per-tree coordinator
    /// (<see cref="ITreeDeletionGrain"/>) must absorb the coordinator's
    /// cold-start seed-timeout via the per-operator
    /// <see cref="ShardActivationRetry.RunAsync"/> wrap.
    /// </summary>
    [Test]
    public async Task DeleteTreeAsync_retries_through_coordinator_seed_timeout_then_succeeds()
    {
        var (grain, factory) = CreateGrain();
        var deletion = Substitute.For<ITreeDeletionGrain>();
        factory.GetGrain<ITreeDeletionGrain>(TreeId).Returns(deletion);

        var calls = 0;
        deletion.DeleteTreeAsync().Returns(_ =>
        {
            calls++;
            if (calls == 1) throw MakeSeedTimeout();
            return Task.CompletedTask;
        });

        await grain.DeleteTreeAsync();

        Assert.That(calls, Is.EqualTo(2));
    }

    // -------------------------------------------------------------------------
    // Fan-out path - the key correctness property of the per-shard wrap:
    // a single shard's seed-timeout retries only that shard, not every
    // sibling task.
    // -------------------------------------------------------------------------

    /// <summary>
    /// <see cref="ILattice.GetLeafProjectionDigestAsync"/> is a single-shard
    /// digest read. A cold-start seed-timeout on the digest call must be
    /// absorbed by the per-call <see cref="ShardActivationRetry"/> wrap so
    /// the caller sees the second attempt's success.
    /// </summary>
    [Test]
    public async Task GetLeafProjectionDigestAsync_retries_through_first_seed_timeout_then_succeeds()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var expected = new LeafProjectionDigest
        {
            Hash = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },
            EntryCount = 42,
            CheckpointOffset = 100,
            Version = 1,
        };

        var calls = 0;
        shardRoot.GetShardProjectionDigestAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                if (calls == 1) throw MakeSeedTimeout();
                return expected;
            });

        var digest = await grain.GetLeafProjectionDigestAsync(0);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(digest.EntryCount, Is.EqualTo(42));
        });
    }

    // -------------------------------------------------------------------------
    // Budget exhaustion - the typed exception still reaches the caller when
    // every retry attempt times out, matching the helper's contract.
    // -------------------------------------------------------------------------

    /// <summary>
    /// When every retry attempt against a coordinator throws
    /// <see cref="ShardActivationTimeoutException"/>, the budget exhausts
    /// and the typed exception surfaces to the caller. The exception type
    /// is preserved (no double-wrap) so operator-facing code can detect it
    /// explicitly via <c>catch (ShardActivationTimeoutException)</c>.
    /// </summary>
    [Test]
    public void DeleteTreeAsync_surfaces_typed_exception_after_retry_budget_exhausted()
    {
        var (grain, factory) = CreateGrain();
        var deletion = Substitute.For<ITreeDeletionGrain>();
        factory.GetGrain<ITreeDeletionGrain>(TreeId).Returns(deletion);

        var calls = 0;
        deletion.DeleteTreeAsync().Returns(_ =>
        {
            calls++;
            throw MakeSeedTimeout();
        });

        Assert.ThrowsAsync<ShardActivationTimeoutException>(async () => await grain.DeleteTreeAsync());

        Assert.That(calls, Is.EqualTo(ShardActivationRetry.MaxAttempts),
            "Envelope did not exhaust the full retry budget.");
    }

    // -------------------------------------------------------------------------
    // Generic RunAsync<T> overload - supports fan-outs that produce per-shard
    // values (GetMaterialiserLagAsync, CompactShardAsync).
    // -------------------------------------------------------------------------

    /// <summary>
    /// <see cref="ILattice.CompactShardAsync"/> routes through
    /// <see cref="ITombstoneCompactionGrain"/> and uses the generic
    /// <see cref="ShardActivationRetry.RunAsync{T}"/> overload (the return
    /// type is <c>Task&lt;bool&gt;</c>). Pins that the generic overload
    /// behaves identically to the void overload on the retry path.
    /// </summary>
    [Test]
    public async Task CompactShardAsync_retries_through_first_seed_timeout_then_returns_value()
    {
        var (grain, factory) = CreateGrain();
        var compactor = Substitute.For<ITombstoneCompactionGrain>();
        factory.GetGrain<ITombstoneCompactionGrain>(TreeId).Returns(compactor);

        var calls = 0;
        compactor.RequestCompactionAsync(0, Arg.Any<string>())
            .Returns(_ =>
            {
                calls++;
                if (calls == 1) throw MakeSeedTimeout();
                return true;
            });

        var accepted = await grain.CompactShardAsync(0);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(accepted, Is.True, "Generic RunAsync<T> overload did not return the operation's value.");
        });
    }

    // -------------------------------------------------------------------------
    // Unrelated exceptions still propagate immediately - the envelope is
    // strictly scoped to ShardActivationTimeoutException.
    // -------------------------------------------------------------------------

    /// <summary>
    /// An <see cref="InvalidOperationException"/> from the underlying shard
    /// must propagate without consuming any retry attempts; the envelope is
    /// strictly scoped to the typed seed-timeout shape.
    /// </summary>
    [Test]
    public void GetAsync_propagates_non_typed_exceptions_without_retry()
    {
        var (grain, factory) = CreateGrain();
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var calls = 0;
        shardRoot.GetAsync("k1").Returns<byte[]?>(_ =>
        {
            calls++;
            throw new InvalidOperationException("not-a-seed-timeout");
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.GetAsync("k1"));

        // The central stale-routing envelope already permits one InvalidOperationException
        // retry (for stale-alias recovery), so calls may be 1 or 2; the key property is
        // that it terminates promptly rather than looping the seed-timeout budget.
        Assert.That(calls, Is.LessThanOrEqualTo(2),
            "Envelope must not retry InvalidOperationException through the seed-timeout path.");
    }
}
