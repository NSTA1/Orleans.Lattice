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
/// completes and the bootstrap reaches LiveIncremental, the
/// bootstrapped peer must observe every authored saga atomically:
/// all of the saga's keys present, or none of them. A strict-subset
/// view on any saga is a saga-atomicity violation.
/// </summary>
/// <remarks>
/// <para>
/// Sampling happens at convergence only, not mid-drain. The bootstrap
/// drain applies prepared and committed rows one leaf at a time, so a
/// polling sample taken between two leaves of the same terminal
/// commit can legitimately observe a strict subset of a saga's keys
/// without violating saga atomicity - that subset is a snapshot of
/// the inter-leaf apply window, not a snapshot of the receiver's
/// externally observable state. Per-saga atomicity of the apply
/// mechanism itself is proved separately by
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
                var sagaKeys = new string[sagaCount][];
                for (var s = 0; s < sagaCount; s++)
                {
                    var keys = new string[keysPerSaga];
                    for (var k = 0; k < keysPerSaga; k++)
                    {
                        keys[k] = $"saga{s:D3}-k{k}";
                    }
                    sagaKeys[s] = keys;
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
                    new LocalVectorClockCache(receiverCluster.Client));

                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
                var pumpErrors = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
                var pumpTask = Task.Run(() => RunPumpAsync(
                    producerFeed,
                    receiverApplier,
                    tree,
                    siteB,
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

                var deadline = DateTime.UtcNow.AddMinutes(1);
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
                while (DateTime.UtcNow < deadline && !cts.IsCancellationRequested);

                Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                    $"Bootstrap must reach LiveIncremental within the convergence window when concurrent producer sagas are in flight. Last observed state: {state}.");

                // Steady-state atomic-visibility sample. With the
                // producer quiesced and the receiver in LiveIncremental,
                // the producer-to-receiver pump has had a finite tail
                // to drain; every authored saga must be either fully
                // visible or fully absent on the receiver. We allow a
                // convergence window for the pump to deliver the
                // post-snapshot terminal records that flip prepared
                // rows to visible.
                await AssertConvergedAllOrNothingAsync(receiverLattice, sagaKeys, cts.Token);

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

    private static IOptionsMonitor<LatticeReplicationOptions> BuildOptionsMonitor(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var opts = new LatticeReplicationOptions
        {
            ClusterId = clusterId,
            ReplogPartitions = 1,
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
        System.Collections.Concurrent.ConcurrentQueue<Exception> errors,
        CancellationToken cancellationToken)
    {
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
                await foreach (var entry in producerFeed
                    .Subscribe(treeName, cursor, includeLocalOrigin: true, cancellationToken)
                    .ConfigureAwait(false))
                {
                    if (string.Equals(entry.OriginClusterId, receiverClusterId, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    await receiverApplier.ApplyAsync(entry, cancellationToken).ConfigureAwait(false);
                }

                cursor = nextCursor;
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

    private static async Task AssertConvergedAllOrNothingAsync(
        ILattice receiverLattice,
        string[][] sagaKeys,
        CancellationToken cancellationToken)
    {
        // Allow a convergence window so the producer-to-receiver pump
        // can deliver the post-snapshot terminal records that flip any
        // outstanding prepared rows. The predicate checked at
        // convergence is the per-saga all-or-nothing invariant: every
        // saga is either fully present or fully absent on the
        // receiver.
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            var anyPartial = false;
            for (var s = 0; s < sagaKeys.Length; s++)
            {
                var presentCount = await CountPresentAsync(receiverLattice, sagaKeys[s]);
                if (presentCount != 0 && presentCount != sagaKeys[s].Length)
                {
                    anyPartial = true;
                    break;
                }
            }
            if (!anyPartial)
            {
                // Steady-state sample is clean - assert it and return.
                for (var s = 0; s < sagaKeys.Length; s++)
                {
                    var keys = sagaKeys[s];
                    var presentCount = await CountPresentAsync(receiverLattice, keys);
                    Assert.That(
                        presentCount == 0 || presentCount == keys.Length,
                        Is.True,
                        $"Bootstrapped peer observed PARTIAL saga visibility for saga={s}: {presentCount}/{keys.Length} keys visible. Atomic visibility was violated.");
                }
                return;
            }
            await Task.Delay(200, cancellationToken);
        }

        // Convergence window exhausted - emit a precise failure
        // message that names every saga that is still partially
        // visible.
        var partials = new List<string>();
        for (var s = 0; s < sagaKeys.Length; s++)
        {
            var presentCount = await CountPresentAsync(receiverLattice, sagaKeys[s]);
            if (presentCount != 0 && presentCount != sagaKeys[s].Length)
            {
                partials.Add($"saga={s} ({presentCount}/{sagaKeys[s].Length})");
            }
        }
        Assert.Fail(
            "Bootstrapped peer never converged to per-saga all-or-nothing visibility within the convergence window. Partially visible sagas: "
            + string.Join(", ", partials));
    }

    private static async Task<int> CountPresentAsync(ILattice receiverLattice, string[] keys)
    {
        var presentCount = 0;
        foreach (var key in keys)
        {
            var value = await receiverLattice.GetAsync(key);
            if (value is not null)
            {
                presentCount++;
            }
        }
        return presentCount;
    }

    private sealed class ProducerSiloConfigurator : ISiloConfigurator
    {
        public static string ClusterId { get; set; } = "";

        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            // Pin WalPartitions=1 (and ReplogPartitions=1 below) so the
            // concurrent-saga bootstrap test stays deterministic under
            // loaded CI. Multi-partition fan-out is covered by its own
            // dedicated MultiPartition* integration suite.
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.ReplogPartitions = 1;
            });
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
            // Pin WalPartitions=1 alongside the producer's pin so the
            // single-partition shape this test was originally written
            // against is preserved on both sides.
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.UseInMemoryReminderService();
            // Receiver uses the package-default bootstrap retry budget.
            // If the default proves insufficient under this workload,
            // widen LatticeReplicationOptions.DefaultBootstrapMaxAttempts
            // rather than overriding it here.
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.ReplogPartitions = 1;
            });
            if (Transport is not null)
            {
                siloBuilder.Services.AddSingleton<IRemoteSnapshotTransport>(Transport);
            }
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }
}