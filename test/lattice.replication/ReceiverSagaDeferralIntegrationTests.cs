using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A receiver must never dead-letter a saga record alone (issue #4591). The
/// dead-letter decorator used to park a prepare or terminal that exhausted
/// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> and acknowledge it, so
/// the sender's terminal hold counted a parked prepare as delivered and released
/// the saga's terminal: this receiver then served the saga torn, and a parked
/// terminal stranded the saga's buckets. Each test drives the real
/// <see cref="DeadLetterTrackingReplicationApplier"/> over the real canonical
/// applier and real grains, injects a persistent apply failure on one saga
/// record, and plays the sender's rules: a record is delivered until it is
/// acknowledged (not thrown, not deferred), and the #4480 hold releases a
/// terminal only once every prepare of its saga is acknowledged.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ReceiverSagaDeferralIntegrationTests
{
    private const string Origin = "rsd-origin";
    private const int MaxApplyRetries = 3;

    private TestCluster _cluster = null!;
    private FailingApplier _failing = null!;
    private DeadLetterTrackingReplicationApplier _receiver = null!;
    private ILatticeReplicationDeadLetters _deadLetters = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _failing = new FailingApplier(services.GetRequiredService<ReplicationApplier>());
        _receiver = new DeadLetterTrackingReplicationApplier(
            _failing,
            services.GetRequiredService<IGrainFactory>(),
            services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        _deadLetters = services.GetRequiredService<ILatticeReplicationDeadLetters>();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [SetUp]
    public void ClearFailures() => _failing.Fail = _ => false;

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static (string KeyA, string KeyB) KeysOnDistinctShards(string prefix)
    {
        var keyA = prefix + "-a";
        var shardA = LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount);
        for (var i = 0; i < 1000; i++)
        {
            var candidate = $"{prefix}-b{i}";
            if (LatticeSharding.GetShardIndex(candidate, LatticeConstants.DefaultShardCount) != shardA)
            {
                return (keyA, candidate);
            }
        }

        throw new InvalidOperationException("could not find two keys on distinct shards");
    }

    private static WalRecord Prepare(string tree, string key, byte value, Guid txid, int index, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new[] { value },
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    private static WalRecord Commit(string tree, string key, Guid txid, long ticks)
    {
        var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
        return new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.TxCommit,
            Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = Hlc(ticks),
            OriginClusterId = Origin,
            TransactionId = txid,
            ShardIndex = shard,
            AtomicShardCount = 2,
        };
    }

    private static WalRecord PointSet(string tree, string key, byte value, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new[] { value },
        Timestamp = Hlc(ticks),
        OriginClusterId = Origin,
    };

    /// <summary>
    /// One push of <paramref name="batch"/>, as the sender sees it: acknowledged
    /// unless the receiver threw or deferred.
    /// </summary>
    private async Task<bool> PushAsync(params WalRecord[] batch)
    {
        try
        {
            var result = await _receiver.ApplyBatchAsync(batch);
            return !result.Deferred;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>The sender re-ships an unacknowledged push up to the receiver's retry budget and then some.</summary>
    private async Task<bool> DeliverAsync(params WalRecord[] batch)
    {
        for (var attempt = 0; attempt < MaxApplyRetries + 2; attempt++)
        {
            if (await PushAsync(batch))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<(byte[]? A, byte[]? B)> ReadAsync(string tree, string keyA, string keyB)
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        return (await lattice.GetAsync(keyA), await lattice.GetAsync(keyB));
    }

    private static Task WaitPastSagaDeferralTimeoutAsync() => Task.Delay(TimeSpan.FromMilliseconds(450));

    private Task<IReadOnlyCollection<Guid>> GetPoisonedAsync(string tree, string origin = Origin) =>
        _cluster.Client.GetGrain<IReceiverSagaPoisonGrain>(tree).GetPoisonedAsync(origin);

    private static void AssertNotTorn((byte[]? A, byte[]? B) read, string when)
    {
        var aPost = read.A is [1];
        var bPost = read.B is [2];
        Assert.That(aPost, Is.EqualTo(bPost),
            $"{when}: the saga is served torn (keyA post-saga={aPost}, keyB post-saga={bPost})");
    }

    [Test]
    public async Task Dead_lettered_prepare_is_deferred_so_the_saga_terminal_is_held_and_never_served_torn()
    {
        const string tree = "rsd-prepare";
        var (keyA, keyB) = KeysOnDistinctShards("prep");
        var txid = Guid.NewGuid();
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 1_001);

        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        var ackedA = await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 1_000));
        var ackedB = await DeliverAsync(prepareB);

        // The #4480 hold releases the terminals only once every prepare is acked.
        if (ackedA && ackedB)
        {
            await DeliverAsync(Commit(tree, keyA, txid, 1_100));
            await DeliverAsync(Commit(tree, keyB, txid, 1_101));
        }

        var during = await ReadAsync(tree, keyA, keyB);
        Assert.Multiple(() =>
        {
            Assert.That(ackedB, Is.False, "a saga prepare that exhausted its apply budget must not be acknowledged");
            AssertNotTorn(during, "while the prepare cannot be applied");
        });

        // A later write from the same origin advances the receiver's high-water
        // mark above the deferred prepare; its re-delivery must still apply.
        Assert.That(await DeliverAsync(PointSet(tree, "unrelated", 7, ticks: 5_000)), Is.True);

        _failing.Fail = _ => false;
        Assert.That(await DeliverAsync(prepareB), Is.True, "the re-shipped prepare applies once the failure clears");
        Assert.That(await DeliverAsync(Commit(tree, keyA, txid, 1_100)), Is.True);
        Assert.That(await DeliverAsync(Commit(tree, keyB, txid, 1_101)), Is.True);

        var after = await ReadAsync(tree, keyA, keyB);
        Assert.Multiple(() =>
        {
            Assert.That(after.A, Is.EqualTo(new byte[] { 1 }));
            Assert.That(after.B, Is.EqualTo(new byte[] { 2 }), "the deferred prepare is not dropped as a duplicate of a lower-HLC entry");
        });
    }

    [Test]
    public async Task Dead_lettered_terminal_is_deferred_and_redelivered_so_the_saga_is_not_stranded()
    {
        const string tree = "rsd-terminal";
        var (keyA, keyB) = KeysOnDistinctShards("term");
        var txid = Guid.NewGuid();
        var commitB = Commit(tree, keyB, txid, 2_101);

        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 2_000)), Is.True);
        Assert.That(await DeliverAsync(Prepare(tree, keyB, 2, txid, index: 1, ticks: 2_001)), Is.True);
        Assert.That(await DeliverAsync(Commit(tree, keyA, txid, 2_100)), Is.True);

        _failing.Fail = r => r.Op == MutationKind.TxCommit && r.ShardIndex == commitB.ShardIndex && r.TransactionId == txid;
        var ackedTerminal = await DeliverAsync(commitB);
        AssertNotTorn(await ReadAsync(tree, keyA, keyB), "while the terminal cannot be applied");

        // The sender re-ships an unacknowledged terminal once the failure clears;
        // an acknowledged (parked) one is never sent again.
        _failing.Fail = _ => false;
        if (!ackedTerminal)
        {
            await DeliverAsync(commitB);
        }

        var after = await ReadAsync(tree, keyA, keyB);
        Assert.Multiple(() =>
        {
            Assert.That(ackedTerminal, Is.False, "a saga terminal that exhausted its apply budget must not be acknowledged");
            Assert.That(after.A, Is.EqualTo(new byte[] { 1 }), "the saga commits once its terminal is re-delivered");
            Assert.That(after.B, Is.EqualTo(new byte[] { 2 }), "the saga commits once its terminal is re-delivered");
        });
    }

    [Test]
    public async Task Terminal_behind_a_deferred_prepare_in_the_same_batch_is_not_applied()
    {
        // One partition and a window of one take no terminal holds, so a
        // saga's terminals can ride in the same batch as its last prepare.
        const string tree = "rsd-batch";
        var (keyA, keyB) = KeysOnDistinctShards("batch");
        var txid = Guid.NewGuid();
        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 3_000)), Is.True);

        var batch = new[]
        {
            Prepare(tree, keyB, 2, txid, index: 1, ticks: 3_001),
            Commit(tree, keyA, txid, 3_100),
            Commit(tree, keyB, txid, 3_101),
        };
        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        var acked = await DeliverAsync(batch);

        Assert.Multiple(async () =>
        {
            Assert.That(acked, Is.False);
            AssertNotTorn(await ReadAsync(tree, keyA, keyB), "with the prepare deferred inside the batch");
        });

        _failing.Fail = _ => false;
        Assert.That(await DeliverAsync(batch), Is.True);
        var after = await ReadAsync(tree, keyA, keyB);
        Assert.That((after.A, after.B), Is.EqualTo((new byte[] { 1 }, new byte[] { 2 })));
    }

    [Test]
    public async Task Non_saga_entry_that_exhausts_its_budget_is_still_dead_lettered_and_acknowledged()
    {
        const string tree = "rsd-plain";
        _failing.Fail = r => r.Key == "plain";

        Assert.That(await DeliverAsync(PointSet(tree, "plain", 1, ticks: 4_000)), Is.True,
            "a plain write is parked and acknowledged as before, so it never stalls the stream");
    }

    [Test]
    public async Task Permanently_unappliable_saga_prepare_is_poisoned_after_the_deferral_bound_and_the_link_resumes()
    {
        const string tree = "rsd-poison-bound";
        var (keyA, keyB) = KeysOnDistinctShards("poison-bound");
        var txid = Guid.NewGuid();
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 5_001);

        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 5_000)), Is.True);
        Assert.That(await DeliverAsync(prepareB), Is.False,
            "before the bound the permanently failing prepare is deferred and keeps the sender link held");
        Assert.That(await GetPoisonedAsync(tree), Is.Empty);

        await WaitPastSagaDeferralTimeoutAsync();

        Assert.That(await PushAsync(prepareB), Is.True,
            "after the bound the prepare is poisoned, parked and acknowledged");
        Assert.That(await DeliverAsync(PointSet(tree, "after-poison", 9, ticks: 5_500)), Is.True,
            "the origin link must resume after the poisoned prepare is acknowledged");

        var parked = await _deadLetters.ListAsync(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await GetPoisonedAsync(tree), Does.Contain(txid));
            Assert.That(parked.Select(e => e.Entry.TransactionId), Does.Contain(txid),
                "the poisoned prepare is parked in the DLQ");
            Assert.That(await _cluster.Client.GetGrain<ILattice>(tree).GetAsync("after-poison"), Is.EqualTo(new byte[] { 9 }));
        });
    }

    [Test]
    public async Task Later_terminal_of_a_poisoned_saga_is_withheld_not_applied()
    {
        const string tree = "rsd-poison-terminal";
        var (keyA, keyB) = KeysOnDistinctShards("poison-term");
        var txid = Guid.NewGuid();
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 6_001);

        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 6_000)), Is.True);
        Assert.That(await DeliverAsync(prepareB), Is.False);
        await WaitPastSagaDeferralTimeoutAsync();
        Assert.That(await PushAsync(prepareB), Is.True);

        _failing.Fail = _ => false;
        var ackedA = await DeliverAsync(Commit(tree, keyA, txid, 6_100));
        var ackedB = await DeliverAsync(Commit(tree, keyB, txid, 6_101));

        var read = await ReadAsync(tree, keyA, keyB);
        var parked = await _deadLetters.ListAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(read.A, Is.Null, "keyA must stay pre-saga: a terminal of a poisoned saga is never applied");
            Assert.That(read.B, Is.Null, "keyB must stay pre-saga: a terminal of a poisoned saga is never applied");
            Assert.That((ackedA, ackedB), Is.EqualTo((false, false)),
                "a later terminal of a poisoned saga is withheld unacknowledged until the re-seed retires the poison, so it is never lost");
            Assert.That(parked.Count(e => e.Entry.TransactionId == txid && e.Entry.Op == MutationKind.TxCommit),
                Is.Zero, "a withheld terminal is not parked");
        });
    }

    [Test]
    public async Task Poison_is_refused_once_the_receiver_registry_has_recorded_a_decision()
    {
        const string tree = "rsd-poison-refused";
        var (_, keyB) = KeysOnDistinctShards("poison-refused");
        var txid = Guid.NewGuid();
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 7_001);

        await TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).MarkCommittedAsync(txid);

        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        Assert.That(await DeliverAsync(prepareB), Is.False);
        await WaitPastSagaDeferralTimeoutAsync();

        Assert.That(await PushAsync(prepareB), Is.False,
            "a receiver decision makes poison fail closed, so the record remains deferred");
        Assert.Multiple(async () =>
        {
            Assert.That(await GetPoisonedAsync(tree), Is.Empty);
            Assert.That((await _deadLetters.ListAsync(tree)).Where(e => e.Entry.TransactionId == txid), Is.Empty);
        });
    }

    [Test]
    public async Task Deferred_terminal_is_never_poisoned_and_stays_deferred()
    {
        const string tree = "rsd-terminal-never-poison";
        var (keyA, keyB) = KeysOnDistinctShards("terminal-never");
        var txid = Guid.NewGuid();
        var commitB = Commit(tree, keyB, txid, 8_101);

        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 8_000)), Is.True);
        Assert.That(await DeliverAsync(Prepare(tree, keyB, 2, txid, index: 1, ticks: 8_001)), Is.True);
        Assert.That(await DeliverAsync(Commit(tree, keyA, txid, 8_100)), Is.True);

        _failing.Fail = r => r.Op == MutationKind.TxCommit && r.ShardIndex == commitB.ShardIndex && r.TransactionId == txid;
        Assert.That(await DeliverAsync(commitB), Is.False);
        await WaitPastSagaDeferralTimeoutAsync();

        Assert.That(await PushAsync(commitB), Is.False,
            "terminals stay deferred past the prepare poison bound");
        Assert.Multiple(async () =>
        {
            Assert.That(await GetPoisonedAsync(tree), Is.Empty);
            Assert.That((await _deadLetters.ListAsync(tree)).Where(e => e.Entry.TransactionId == txid), Is.Empty);
        });
    }

    [Test]
    public async Task Operator_poison_of_a_saga_parks_its_records_and_resumes_the_link()
    {
        const string tree = "rsd-operator-poison";
        var (keyA, keyB) = KeysOnDistinctShards("operator-poison");
        var txid = Guid.NewGuid();
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 9_001);

        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 9_000)), Is.True);
        Assert.That(await DeliverAsync(prepareB), Is.False);

        Assert.That(await _deadLetters.PoisonSagaAsync(tree, Origin, txid), Is.True);
        Assert.That(await PushAsync(prepareB), Is.True,
            "operator poison causes the next copy of the saga prepare to be parked and acknowledged");
        Assert.That(await DeliverAsync(PointSet(tree, "after-operator-poison", 4, ticks: 9_500)), Is.True);

        var parked = await _deadLetters.ListAsync(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await GetPoisonedAsync(tree), Does.Contain(txid));
            Assert.That(parked.Select(e => e.Entry.TransactionId), Does.Contain(txid));
            Assert.That(await _cluster.Client.GetGrain<ILattice>(tree).GetAsync("after-operator-poison"),
                Is.EqualTo(new byte[] { 4 }));
        });
    }

    [Test]
    public async Task Poisoned_prepare_stays_unacknowledged_while_the_dead_letter_queue_is_full()
    {
        const string tree = FullQueueTree;
        var (keyA, keyB) = KeysOnDistinctShards("poison-full");
        var txid = Guid.NewGuid();
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 10_001);

        // A plain write fills the one-entry queue.
        _failing.Fail = r => r.Key == "filler" || (r.IsPrepared && r.Key == keyB);
        Assert.That(await DeliverAsync(PointSet(tree, "filler", 1, ticks: 10_000)), Is.True);
        Assert.That(await _deadLetters.CountAsync(tree), Is.EqualTo(1), "PRECONDITION: the queue is full");

        Assert.That(await DeliverAsync(Prepare(tree, keyA, 1, txid, index: 0, ticks: 10_000)), Is.True);
        Assert.That(await DeliverAsync(prepareB), Is.False);
        await WaitPastSagaDeferralTimeoutAsync();

        var refused = await _receiver.ApplyBatchAsync(new[] { prepareB });
        Assert.Multiple(async () =>
        {
            Assert.That(refused.Deferred, Is.True,
                "a poisoned prepare the full queue cannot park must be deferred, not acknowledged (#4603): parking is what keeps it");
            Assert.That(await GetPoisonedAsync(tree), Does.Contain(txid), "the poison itself stands");
        });

        // Freeing capacity lets the next re-delivery park it and resume the link.
        var filler = (await _deadLetters.ListAsync(tree)).Single();
        Assert.That(await _deadLetters.DiscardAsync(tree, filler.EntryId), Is.True);
        Assert.That(await PushAsync(prepareB), Is.True);
        Assert.That((await _deadLetters.ListAsync(tree)).Select(e => e.Entry.TransactionId), Does.Contain(txid));
    }

    /// <summary>The canonical applier, failing every record <see cref="Fail"/> selects.</summary>
    private sealed class FailingApplier(IReplicationApplier inner) : IReplicationApplier
    {
        public Func<WalRecord, bool> Fail { get; set; } = _ => false;

        public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            if (Fail(entry))
            {
                throw new IOException($"injected apply failure for {entry.Op} '{entry.Key}'");
            }

            return inner.ApplyAsync(entry, cancellationToken);
        }

        public async Task<ApplyResult> ApplyBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            foreach (var entry in entries)
            {
                if (Fail(entry))
                {
                    throw new IOException($"injected apply failure for {entry.Op} '{entry.Key}'");
                }
            }

            return await inner.ApplyBatchAsync(entries, cancellationToken);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "rsd-receiver";
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.SagaDeferralTimeout = TimeSpan.FromMilliseconds(300);
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.Services.Configure<LatticeReplicationOptions>(FullQueueTree, o => o.DeadLetterQueueCapacity = 1);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private const string FullQueueTree = "rsd-poison-dlq-full";

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
