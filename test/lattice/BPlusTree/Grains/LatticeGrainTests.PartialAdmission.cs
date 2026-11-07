using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Partial admission of a batch write (issue #5970). A refused
/// <see cref="LatticeGrain.SetManyAsync"/> lists a superset of the keys that may
/// not have landed, so a caller retries only those, and a shard's retry re-sends
/// only them. Every test is driven by the order of completions alone: none
/// depends on a timing or a clock tick.
/// </summary>
public partial class LatticeGrainTests
{
    private const string PartialTreeId = "partial-admission";

    /// <summary>
    /// A tree whose shards are distinct substitutes, keyed by their grain key,
    /// each recording every bucket it is sent.
    /// </summary>
    private static (LatticeGrain Grain, IReadOnlyDictionary<int, IShardRootGrain> Shards, IReadOnlyDictionary<int, List<List<KeyValuePair<string, byte[]>>>> Sent)
        CreatePartialAdmissionTree(int shardCount, IServiceProvider? activationServices = null, LatticeOptions? options = null)
    {
        var (grain, factory) = CreateGrain(PartialTreeId, options, shardCount: shardCount, activationServices: activationServices);
        SetupCompactionGrain(factory, PartialTreeId);

        var shards = new Dictionary<int, IShardRootGrain>();
        var sent = new Dictionary<int, List<List<KeyValuePair<string, byte[]>>>>();
        for (var i = 0; i < shardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            var calls = new List<List<KeyValuePair<string, byte[]>>>();
            shard.SetManyAsync(Arg.Do<List<KeyValuePair<string, byte[]>>>(b => calls.Add([.. b])));
            shard.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.CompletedTask);
            shards[i] = shard;
            sent[i] = calls;
        }

        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>())
            .Returns(ci => shards[int.Parse(ci.ArgAt<string>(0).Split('/')[^1], System.Globalization.CultureInfo.InvariantCulture)]);
        return (grain, shards, sent);
    }

    private static List<KeyValuePair<string, byte[]>> PartialBatch(int count)
        => Enumerable.Range(0, count).Select(i => new KeyValuePair<string, byte[]>($"key-{i:D3}", [(byte)i])).ToList();

    /// <summary>
    /// A refusal a shard raises when its leaves were refused. WAL admission is
    /// not retried by the shard envelope, so each grain call is one attempt.
    /// </summary>
    private static LatticeSaturatedException ShardRefusal(IEnumerable<string>? unapplied, int batchCount)
        => unapplied is null
            ? new LatticeSaturatedException("refused", PartialTreeId, LatticeSaturationSource.WalAdmission)
            : new LatticeSaturatedException("refused", PartialTreeId, LatticeSaturationSource.WalAdmission, unapplied, batchCount, innerException: null);

    [Test]
    public async Task A_refused_batch_lists_the_refused_and_in_flight_keys_and_leaves_out_confirmed_shards()
    {
        var (grain, shards, sent) = CreatePartialAdmissionTree(shardCount: 3);
        var batch = PartialBatch(60);

        // Shard 0 confirms. Shard 1 lands half its bucket and reports the other
        // half. Shard 2 is still in flight when the refusal surfaces. Buckets are
        // read back from what each shard was sent, never recomputed.
        List<string>? reported = null;
        shards[1].SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(ci =>
            {
                var slice = ci.Arg<List<KeyValuePair<string, byte[]>>>();
                reported = slice.Take(slice.Count / 2).Select(e => e.Key).ToList();
                return Task.FromException(ShardRefusal(reported, slice.Count));
            });
        var straggler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        shards[2].SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ => straggler.Task);

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(async () => await grain.SetManyAsync(batch));
        straggler.SetResult();
        await Task.Yield();

        Assert.That(sent.Values.All(calls => calls.Count == 1), Is.True,
            "instrument validation: every shard holds keys of the batch and was sent them once");
        var confirmed = sent[0][0].Select(e => e.Key).ToList();
        var inFlight = sent[2][0].Select(e => e.Key).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(thrown!.UnappliedKeys, Is.EquivalentTo(reported!.Concat(inFlight)),
                "the refused shard's reported keys plus the in-flight shard's whole bucket");
            Assert.That(thrown.UnappliedKeys!.Intersect(confirmed), Is.Empty,
                "a shard that returned success is confirmed and never listed");
            Assert.That(thrown.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission));
            Assert.That(thrown.Message, Does.Contain($"of {batch.Count} key(s)"));
        });
    }

    [Test]
    public async Task A_batch_spanning_a_refused_shard_is_not_held_hostage_when_only_the_listed_keys_are_retried()
    {
        // The #5970 shape at the API: shard 1 stands for the leaves still queued
        // for a replay permit and refuses its first three attempts; shard 0
        // stands for the leaves already replayed. A producer that re-sends the
        // whole batch rewrites shard 0's keys on every attempt and has nothing
        // confirmed until the last one. One that retries UnappliedKeys sends
        // shard 0's keys once, and from the first refusal has every one of them
        // confirmed: only the refused shard's keys stay outstanding.
        const int refusals = 3;

        static async Task<(int Shard0Sends, List<int> Outstanding, int RefusedBucket)> RunProducerAsync(bool retryOnlyUnapplied)
        {
            var (grain, shards, sent) = CreatePartialAdmissionTree(shardCount: 2);
            var batch = PartialBatch(40);
            var attempts = 0;
            shards[1].SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
                .Returns(ci =>
                {
                    var slice = ci.Arg<List<KeyValuePair<string, byte[]>>>();
                    return ++attempts <= refusals
                        ? Task.FromException(ShardRefusal(slice.Select(e => e.Key), slice.Count))
                        : Task.CompletedTask;
                });

            var outstanding = new List<int>();
            var pending = batch;
            for (var attempt = 0; attempt <= refusals; attempt++)
            {
                try
                {
                    await grain.SetManyAsync(pending);
                    outstanding.Add(0);
                    break;
                }
                catch (LatticeSaturatedException refused)
                {
                    if (retryOnlyUnapplied && refused.UnappliedKeys is { } unapplied)
                    {
                        var keep = unapplied.ToHashSet(StringComparer.Ordinal);
                        pending = pending.FindAll(e => keep.Contains(e.Key));
                    }

                    outstanding.Add(retryOnlyUnapplied && refused.UnappliedKeys is not null ? pending.Count : batch.Count);
                }
            }

            return (sent[0].Count, outstanding, sent[1][0].Count);
        }

        var whole = await RunProducerAsync(retryOnlyUnapplied: false);
        var subset = await RunProducerAsync(retryOnlyUnapplied: true);

        Assert.Multiple(() =>
        {
            Assert.That(whole.Shard0Sends, Is.EqualTo(refusals + 1),
                "instrument validation: a whole-batch retry rewrites the confirmed shard on every attempt");
            Assert.That(subset.Shard0Sends, Is.EqualTo(1),
                "the confirmed shard's keys are sent once; no retry rewrites them");
            Assert.That(subset.Outstanding.Take(refusals), Is.All.EqualTo(subset.RefusedBucket),
                "from the first refusal only the refused shard's keys are outstanding");
            Assert.That(subset.Outstanding[^1], Is.Zero, "the batch still lands whole");
        });
    }

    [Test]
    public void A_refusal_with_a_fault_that_is_not_saturation_on_another_branch_lists_nothing()
    {
        // The saturation branch is dispatched first and carries its own shard's
        // list. Surfacing that list would let a subset retry drop the other
        // shard's keys, which failed with a fault that is not saturation.
        var (grain, shards, _) = CreatePartialAdmissionTree(shardCount: 2);
        shards[0].SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(ci =>
            {
                var slice = ci.Arg<List<KeyValuePair<string, byte[]>>>();
                return Task.FromException(ShardRefusal(slice.Select(e => e.Key), slice.Count));
            });
        shards[1].SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ => Task.FromException(new InvalidOperationException("permanent")));

        var thrown = Assert.CatchAsync(async () => await grain.SetManyAsync(PartialBatch(40)));

        Assert.That(thrown, Is.InstanceOf<LatticeSaturatedException>(),
            "instrument validation: the saturation branch is the first fault to surface");
        Assert.That(((LatticeSaturatedException)thrown!).UnappliedKeys, Is.Null,
            "a mixed fault cannot be attributed key by key, so the shard's partial list must not surface");
    }

    [Test]
    public async Task The_landed_keys_of_a_refused_batch_publish_their_set_events_before_the_refusal()
    {
        var published = new List<string>();
        var stream = Substitute.For<IAsyncStream<LatticeTreeEvent>>();
        stream.OnNextAsync(Arg.Do<LatticeTreeEvent>(e => { lock (published) published.Add(e.Key!); }), Arg.Any<StreamSequenceToken?>())
            .Returns(Task.CompletedTask);
        var provider = Substitute.For<IStreamProvider>();
        provider.GetStream<LatticeTreeEvent>(Arg.Any<StreamId>()).Returns(stream);
        var services = new ServiceCollection();
        services.AddKeyedSingleton("Default", provider);
        using var activationServices = services.BuildServiceProvider();

        var (grain, shards, _) = CreatePartialAdmissionTree(
            shardCount: 2,
            activationServices,
            new LatticeOptions { PublishEvents = true, EventStreamProviderName = "Default" });
        var batch = PartialBatch(40);
        List<string> refusedKeys = [];
        shards[1].SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(ci =>
            {
                refusedKeys = ci.Arg<List<KeyValuePair<string, byte[]>>>().Select(e => e.Key).ToList();
                return Task.FromException(ShardRefusal(refusedKeys, refusedKeys.Count));
            });

        var thrown = Assert.ThrowsAsync<LatticeSaturatedException>(async () => await grain.SetManyAsync(batch));

        Assert.That(thrown!.UnappliedKeys, Is.EquivalentTo(refusedKeys));
        Assert.That(published, Is.EquivalentTo(batch.Select(e => e.Key).Except(refusedKeys)),
            "a key left out of UnappliedKeys is never re-sent by a subset retry, so its Set event is published now");
        await Task.CompletedTask;
    }

    [Test]
    public async Task A_shard_retry_resends_only_the_keys_the_refused_attempt_listed()
    {
        var shard = Substitute.For<IShardRootGrain>();
        var calls = new List<List<string>>();
        var attempt = 0;
        shard.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(ci =>
            {
                calls.Add(ci.Arg<List<KeyValuePair<string, byte[]>>>().Select(e => e.Key).ToList());
                return ++attempt == 1
                    ? Task.FromException(new LatticeSaturatedException(
                        "refused", PartialTreeId, LatticeSaturationSource.ReplayPermitAdmission, ["b"], 4, innerException: null))
                    : Task.CompletedTask;
            });

        await ShardActivationRetry.RunSetManyAsync(
            shard,
            [new("a", [1]), new("b", [2]), new("c", [3]), new("b", [4])]);

        Assert.That(calls, Has.Count.EqualTo(2));
        Assert.That(calls[1], Is.EqualTo(new[] { "b", "b" }),
            "only the listed key, in order and with its duplicate, so its last write still wins");
    }
}
