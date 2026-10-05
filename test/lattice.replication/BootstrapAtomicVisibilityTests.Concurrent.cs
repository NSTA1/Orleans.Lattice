using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Concurrent two-cluster acceptance test for the bootstrap-boundary
/// atomic-visibility invariant. A producer cluster authors
/// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
/// sagas continuously while a fresh receiver cluster cross-cluster
/// bootstraps via the snapshot path; in parallel an in-process
/// producer-to-receiver pump delivers the post-snapshot incremental
/// WAL stream through the change-feed/applier seam, exactly as the
/// chaos suite's <c>ChaosDeliveryPump</c> does. After authorship
/// completes and the bootstrap reaches LiveIncremental, every
/// authored saga must be fully present on the bootstrapped peer, and
/// no read of a saga's keys may ever observe a strict subset of them.
/// </summary>
/// <remarks>
/// <para>
/// Every sample reads a saga's keys with ONE
/// <see cref="ILattice.GetManyAsync(List{string}, CancellationToken)"/>,
/// the read whose contract is atomic visibility across the requested
/// keys (the <c>RAllOrNothing</c> row of
/// <c>spec/atomic-commit/RefinementCrossCluster.md</c>). Separate
/// <see cref="ILattice.GetAsync(string, CancellationToken)"/> calls are
/// only per-key linearizable: a series of them that spans the
/// receiver's single flip point legitimately sees the keys read before
/// it pre-saga and the keys read after it post-saga. The test used to
/// sample that way while the pump was still delivering terminals, and
/// failed intermittently on exactly that legitimate interleaving
/// (issue #4598).
/// </para>
/// <para>
/// The flip check is deterministic: the pump samples a saga right after
/// it applies each of that saga's source-shard terminals, so every
/// partial-tally point it delivers is read, and a receiver that let
/// one shard's keys surface before the last terminal arrived fails the
/// check at the first such point. The final read requires every saga
/// fully present - no saga aborts, so an all-absent saga is a lost
/// commit, not an atomic outcome.
/// </para>
/// <para>
/// Per-saga atomicity of the apply mechanism itself is proved separately by
/// <c>Prepared_rows_replayed_on_receiver_become_atomically_visible_on_terminal</c>;
/// concurrent steady-state atomicity under partition cycling is
/// proved by the chaos suite
/// <c>Concurrent_cross_cluster_sagas_under_partition_remain_atomically_visible_on_every_site</c>.
/// This test composes the same invariant across the bootstrap
/// boundary.
/// </para>
/// <para>
/// Receiver bootstrap retry behaviour is left at the package default
/// (<see cref="LatticeReplicationOptions.DefaultBootstrapMaxAttempts"/>
/// attempts, <see cref="LatticeReplicationOptions.DefaultBootstrapInitialRetryDelay"/>
/// initial backoff, <see cref="LatticeReplicationOptions.DefaultBootstrapMaxRetryDelay"/>
/// ceiling). If a future change makes the default insufficient to
/// absorb transient enumerator-session drops caused by concurrent
/// producer activity, the default itself must widen - not a per-test
/// override.
/// </para>
/// </remarks>
public partial class BootstrapAtomicVisibilityTests
{
    [Test]
    public async Task Concurrent_producer_saga_during_bootstrap_is_atomically_visible_or_absent_on_the_bootstrapped_peer()
    {
        const string siteA = "cbav-site-a";
        const string siteB = "cbav-site-b";
        const string tree = "cbav-tree";
        const int sagaCount = 30;
        const int postLiveSagaCount = 3;
        const int keysPerSaga = 4;

        // Site A is the producer. Build it first so its grain client
        // is available for the cross-cluster snapshot transport that
        // site B will use as its remote source.
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<ProducerSiloConfigurator>();
        ProducerSiloConfigurator.ClusterId = siteA;
        var producerCluster = aBuilder.Build();
        await producerCluster.DeployAsync();

        try
        {
            // Wire the site-A snapshot provider behind a synthetic
            // cross-cluster transport that site B will use to bootstrap.
            // LatticeRemoteSnapshotService is itself an
            // IRemoteSnapshotTransport, so injecting it on the receiver
            // silo lets site B drive the producer's local snapshot
            // provider in-process while exercising the same bootstrap
            // coordinator pipeline (drain, prepared-row replay,
            // terminal flip) the gRPC binding would.
            var producerProvider = new LatticeSnapshotProvider(
                producerCluster.Client,
                new InMemoryWalCursorRegistry(),
                LatticeSnapshotProviderUnitTests.TestOptions());
            var transport = new LatticeRemoteSnapshotService(
                producerProvider,
                new StubReplicationContext(siteA, LatticeMergeMode.LwwRegister),
                NullLogger<LatticeRemoteSnapshotService>.Instance);
            ReceiverSiloConfigurator.Transport = transport;
            ReceiverSiloConfigurator.ClusterId = siteB;

            var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
            bBuilder.AddSiloBuilderConfigurator<ReceiverSiloConfigurator>();
            var receiverCluster = bBuilder.Build();
            await receiverCluster.DeployAsync();

            try
            {
                var producerLattice = producerCluster.Client.GetGrain<ILattice>(tree);
                var receiverLattice = receiverCluster.Client.GetGrain<ILattice>(tree);

                // Plan every saga's key set up front so the post-drain
                // check can recover saga membership from the key
                // namespace.
                var sagaKeys = new string[sagaCount + postLiveSagaCount][];
                for (var s = 0; s < sagaCount; s++)
                {
                    var keys = new string[keysPerSaga];
                    for (var k = 0; k < keysPerSaga; k++)
                    {
                        keys[k] = $"saga{s:D3}-k{k}";
                    }
                    sagaKeys[s] = keys;
                }

                // The sagas authored once the receiver is live have their
                // keys on distinct source shards, so each delivers
                // keysPerSaga separate terminals and the flip check reads
                // keysPerSaga - 1 partial-tally points of each.
                for (var s = sagaCount; s < sagaKeys.Length; s++)
                {
                    sagaKeys[s] = KeysOnDistinctShards($"saga{s:D3}", keysPerSaga);
                }

                // Construct the producer-to-receiver delivery pump. The
                // bootstrap snapshot covers state up to the snapshot
                // cut; the post-snapshot incremental stream covers
                // every saga whose terminal commit lands after the
                // cut. Without this pump, the receiver would see only
                // the snapshot's prepared rows for sagas that
                // committed after the cut - exactly the partial-saga
                // view atomic bootstrap visibility forbids.
                var producerOptions = BuildOptionsMonitor(siteA);
                var receiverOptions = BuildOptionsMonitor(siteB);
                var producerResolver = Substitute.For<ILatticeMergeModeResolver>();
                producerResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
                var producerFeed = new ChangeFeed(producerCluster.Client, producerOptions, producerResolver);
                var receiverApplier = new ReplicationApplier(
                    receiverCluster.Client,
                    receiverOptions,
                    replicationContext: new OverridesReplicationContext());

                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
                var pumpErrors = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
                var coordForProbe = receiverCluster.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
                var flipProbe = new SagaFlipProbe(receiverLattice, sagaKeys, async () => (await coordForProbe.GetStateAsync()).ToString());
                var pumpTask = Task.Run(() => RunPumpAsync(
                    producerFeed,
                    receiverApplier,
                    tree,
                    siteB,
                    flipProbe,
                    pumpErrors,
                    cts.Token));

                // Author every saga concurrently against the producer
                // while the receiver auto-bootstraps. The producer-side
                // SetManyAtomicAsync is itself a sequenced 2PC over
                // multiple shards, so each saga's prepared rows briefly
                // live in the per-leaf pending-tx buckets that the
                // snapshot exporter visits in its prepared pass.
                var authorTask = Task.Run(async () =>
                {
                    for (var s = 0; s < sagaCount && !cts.IsCancellationRequested; s++)
                    {
                        var entries = new List<KeyValuePair<string, byte[]>>(keysPerSaga);
                        foreach (var key in sagaKeys[s])
                        {
                            entries.Add(new KeyValuePair<string, byte[]>(key, new byte[] { (byte)s }));
                        }
                        await producerLattice.SetManyAtomicAsync(entries, cts.Token);
                    }
                }, cts.Token);

                // Kick off bootstrap on the receiver while authorship
                // is still streaming. The bootstrap coordinator owns
                // the drain + prepared-row replay + terminal flip
                // pipeline.
                var coord = receiverCluster.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
                await coord.BootstrapAsync(siteA, cts.Token);

                // Wait for authorship to drain so the post-snapshot
                // incremental WAL stream has a finite tail to deliver,
                // then for bootstrap to reach LiveIncremental so the
                // sample is taken at a true steady state.
                await authorTask;

                var deadline = Environment.TickCount64 + (long)TimeSpan.FromMinutes(1).TotalMilliseconds;
                LatticeBootstrapState state;
                do
                {
                    state = await coord.GetStateAsync(cts.Token);
                    if (state == LatticeBootstrapState.LiveIncremental)
                    {
                        break;
                    }
                    if (state == LatticeBootstrapState.Failed)
                    {
                        Assert.Fail("Bootstrap coordinator entered Failed during concurrent producer authorship. The default bootstrap retry budget must absorb transient enumerator-session drops caused by concurrent producer activity - if this assertion fires, widen LatticeReplicationOptions.DefaultBootstrapMaxAttempts instead of overriding the budget per test.");
                    }
                    await Task.Delay(100, cts.Token);
                }
                while (Environment.TickCount64 < deadline && !cts.IsCancellationRequested);

                Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                    $"Bootstrap must reach LiveIncremental within the convergence window when concurrent producer sagas are in flight. Last observed state: {state}.");

                // Sagas authored once the receiver is live reach it only
                // through the pump, so the pump delivers every one of their
                // terminals first and the flip check reads each partial
                // tally deterministically.
                for (var s = sagaCount; s < sagaKeys.Length; s++)
                {
                    await producerLattice.SetManyAtomicAsync(
                        sagaKeys[s].Select(key => new KeyValuePair<string, byte[]>(key, new byte[] { (byte)s })).ToList(),
                        cts.Token);
                }

                // Convergence: with the producer quiesced and the
                // receiver in LiveIncremental, the pump has a finite
                // tail to deliver. Every authored saga committed on the
                // producer, so each must end fully present here, and
                // every atomic read on the way must be all-or-nothing.
                await AssertConvergedAllPresentAsync(receiverLattice, sagaKeys, cts.Token);

                Assert.That(
                    flipProbe.Violations,
                    Is.Empty,
                    "An atomic read taken right after a source-shard terminal saw a strict subset of the saga's keys: "
                    + string.Join("; ", flipProbe.Violations));
                for (var s = sagaCount; s < sagaKeys.Length; s++)
                {
                    Assert.That(
                        flipProbe.SamplesFor(s),
                        Is.GreaterThanOrEqualTo(keysPerSaga - 1),
                        $"PRECONDITION: the flip check read every partial-tally point of post-live saga {s}.");
                }

                Assert.That(
                    pumpErrors,
                    Is.Empty,
                    $"Producer-to-receiver delivery pump surfaced {pumpErrors.Count} errors during the run. First: {(pumpErrors.TryPeek(out var first) ? first.Message : "<none>")}.");

                cts.Cancel();
                try { await pumpTask; } catch (OperationCanceledException) { }
            }
            finally
            {
                await receiverCluster.StopAllSilosAsync();
                await receiverCluster.DisposeAsync();
            }
        }
        finally
        {
            await producerCluster.StopAllSilosAsync();
            await producerCluster.DisposeAsync();
            ReceiverSiloConfigurator.Transport = null;
        }
    }

    private static string[] KeysOnDistinctShards(string prefix, int count)
    {
        var keys = new List<string>(count);
        var shards = new HashSet<int>();
        for (var i = 0; keys.Count < count; i++)
        {
            var key = $"{prefix}-k{i}";
            if (shards.Add(LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount)))
            {
                keys.Add(key);
            }
        }

        return keys.ToArray();
    }

    private static IOptionsMonitor<LatticeReplicationOptions> BuildOptionsMonitor(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        // The pump's ChangeFeed must read every partition the producer
        // silo wrote to. Leave ReplogPartitions at the package default
        // (LatticeReplicationOptions.DefaultReplogPartitions) so it
        // matches the silos' resolved WalPartitions; hardcoding =1 here
        // would cause the pump to read only partition 0 and miss every
        // entry hash-routed to a sibling partition, producing a
        // partial-saga visibility failure on the receiver that is an
        // artefact of the test wiring rather than a real invariant
        // violation.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = clusterId,
        };
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);
        return monitor;
    }

    private static async Task RunPumpAsync(
        ChangeFeed producerFeed,
        ReplicationApplier receiverApplier,
        string treeName,
        string receiverClusterId,
        SagaFlipProbe flipProbe,
        System.Collections.Concurrent.ConcurrentQueue<Exception> errors,
        CancellationToken cancellationToken)
    {
        // Saga index by transaction id, learned from the prepares the pump
        // delivers. The change feed yields every terminal after its saga's
        // prepares (issue #4511), so a terminal's saga is always known here.
        var sagaByTransaction = new Dictionary<Guid, int>();
        // Phase D1c: cursor shape is per-partition WAL offset.
        // Capture the producer's current cursor before the Subscribe
        // call so entries authored during our consume land in the
        // next poll iteration; entries committed before the capture
        // are streamed by the Subscribe call below.
        var cursor = ChangeFeedCursor.Initial;
        var pollInterval = TimeSpan.FromMilliseconds(50);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var nextCursor = await producerFeed
                    .GetCurrentCursorAsync(treeName, cancellationToken)
                    .ConfigureAwait(false);
                var deferred = false;
                await foreach (var entry in producerFeed
                    .Subscribe(treeName, cursor, includeLocalOrigin: true, cancellationToken)
                    .ConfigureAwait(false))
                {
                    if (string.Equals(entry.OriginClusterId, receiverClusterId, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (entry.IsPrepared && SagaFlipProbe.TryParseSaga(entry.Key, out var preparedSaga))
                    {
                        sagaByTransaction[entry.TransactionId] = preparedSaga;
                    }

                    var result = await receiverApplier.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
                    if (result.Deferred)
                    {
                        // The receiver deferred the entry (a fence, a gate, a
                        // full dead-letter queue): re-deliver this pass from
                        // the same cursor, as the real shipper keeps its
                        // cursor on a not-accepted ack. Applies are idempotent.
                        deferred = true;
                        break;
                    }

                    if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort
                        && sagaByTransaction.TryGetValue(entry.TransactionId, out var terminalSaga))
                    {
                        await flipProbe.SampleAfterTerminalAsync(terminalSaga, entry.ShardIndex, cancellationToken).ConfigureAwait(false);
                    }
                }

                if (!deferred)
                {
                    cursor = nextCursor;
                }

                await Task.Delay(pollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                errors.Enqueue(ex);
                try { await Task.Delay(pollInterval, cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
    }

    private static async Task AssertConvergedAllPresentAsync(
        ILattice receiverLattice,
        string[][] sagaKeys,
        CancellationToken cancellationToken)
    {
        // Allow a convergence window for the pump to deliver the
        // post-snapshot terminals. Each saga is read with one atomic
        // GetManyAsync, so a strict subset is a violation at any moment,
        // and the window ends only once every saga is fully present.
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        var notPresent = new List<string>();
        while (true)
        {
            notPresent.Clear();
            for (var s = 0; s < sagaKeys.Length; s++)
            {
                var present = await SagaFlipProbe.ReadPresentAsync(receiverLattice, sagaKeys[s], cancellationToken);
                Assert.That(
                    present == 0 || present == sagaKeys[s].Length,
                    Is.True,
                    $"Bootstrapped peer served a PARTIAL saga to one atomic read: saga={s}, {present}/{sagaKeys[s].Length} keys visible.");
                if (present != sagaKeys[s].Length)
                {
                    notPresent.Add($"saga={s}");
                }
            }

            if (notPresent.Count == 0)
            {
                return;
            }

            if (Environment.TickCount64 >= deadline)
            {
                Assert.Fail(
                    "Bootstrapped peer never made every committed saga visible within the convergence window. Absent sagas: "
                    + string.Join(", ", notPresent));
            }

            await Task.Delay(200, cancellationToken);
        }
    }

    /// <summary>
    /// Reads a saga's keys atomically right after the pump applies one of
    /// its source-shard terminals, and records any read that sees a strict
    /// subset of them.
    /// </summary>
    private sealed class SagaFlipProbe(ILattice receiverLattice, string[][] sagaKeys, Func<Task<string>> bootstrapState)
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<int, int> _samples = new();

        public System.Collections.Concurrent.ConcurrentQueue<string> Violations { get; } = new();

        public int SamplesFor(int saga) => _samples.TryGetValue(saga, out var count) ? count : 0;

        public async Task SampleAfterTerminalAsync(int saga, int sourceShard, CancellationToken cancellationToken)
        {
            var keys = sagaKeys[saga];
            var stateBefore = await bootstrapState();
            var values = await ReadAsync(receiverLattice, keys, cancellationToken);
            _samples.AddOrUpdate(saga, 1, static (_, count) => count + 1);
            if (values.Count != 0 && values.Count != keys.Length)
            {
                var stateAfter = await bootstrapState();
                var absent = string.Join(",", keys.Where(k => !values.ContainsKey(k)));
                Violations.Enqueue($"saga={saga} {values.Count}/{keys.Length} after the terminal of source shard {sourceShard} (absent: {absent}; bootstrap {stateBefore} -> {stateAfter})");
            }
        }

        public static bool TryParseSaga(string? key, out int saga)
        {
            saga = -1;
            return key is { Length: > 7 }
                && key.StartsWith("saga", StringComparison.Ordinal)
                && int.TryParse(key.AsSpan(4, 3), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out saga);
        }

        /// <summary>
        /// The number of <paramref name="keys"/> one atomic read finds
        /// present. Retries the refusals a bootstrapping receiver answers
        /// a read with while its import drains, and an unreachable registry.
        /// </summary>
        public static async Task<int> ReadPresentAsync(ILattice lattice, string[] keys, CancellationToken cancellationToken) =>
            (await ReadAsync(lattice, keys, cancellationToken)).Count;

        private static async Task<Dictionary<string, byte[]>> ReadAsync(ILattice lattice, string[] keys, CancellationToken cancellationToken)
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    return await lattice.GetManyAsync(keys.ToList(), cancellationToken);
                }
                catch (Exception ex) when (attempt < 200
                    && ex is LatticeTreeBootstrappingException or LatticeTransactionOutcomeUnavailableException)
                {
                    await Task.Delay(50, cancellationToken);
                }
            }
        }
    }

    private sealed class ProducerSiloConfigurator : ISiloConfigurator
    {
        public static string ClusterId { get; set; } = "";

        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class ReceiverSiloConfigurator : ISiloConfigurator
    {
        public static string ClusterId { get; set; } = "";
        public static IRemoteSnapshotTransport? Transport { get; set; }

        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            // Receiver uses the package-default bootstrap retry budget.
            // If the default proves insufficient under this workload,
            // widen LatticeReplicationOptions.DefaultBootstrapMaxAttempts
            // rather than overriding it here.
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            if (Transport is not null)
            {
                siloBuilder.Services.AddSingleton<IRemoteSnapshotTransport>(Transport);
            }
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }
}