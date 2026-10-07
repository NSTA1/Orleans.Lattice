using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    private sealed class GetManyProgressHarness
    {
        public required LatticeGrain Lattice { get; init; }
        public required TxRegistryGrain Registry { get; init; }
        public required ITxRegistryGrain RegistryProxy { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required BPlusLeafGrain[] Leaves { get; init; }
        public required List<string> Keys { get; init; }
        public required ShardMap Map { get; init; }
        public required Orleans.Lattice.Testing.ManualTimeProvider Clock { get; init; }
        public Guid Transaction { get; set; }
        public byte Round { get; set; }
        public bool Gated { get; set; }
        public int OptimisticReads { get; set; }
        public int RefusedCommits { get; set; }
        public Action? DuringGatedRead { get; set; }
        public TaskCompletionSource? AcquireBarrier { get; set; }

        public async Task PrepareAsync()
        {
            Transaction = Guid.NewGuid();
            Round++;
            for (var i = 0; i < Leaves.Length; i++)
                await PreparePendingSetAsync(Leaves[i], Transaction, Keys[i], [Round]);
        }
    }

    private static GetManyProgressHarness CreateGetManyProgressHarness()
    {
        const string tree = "getmany-progress";
        var options = new LatticeOptions { MaxScanRetries = 3, TxDecisionRetention = TimeSpan.FromMinutes(10) };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var factory = Substitute.For<IGrainFactory>();
        var registryContext = Substitute.For<IGrainContext>();
        registryContext.GrainId.Returns(GrainId.Create("tx-registry", tree));
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var registry = new TxRegistryGrain(
            registryContext, factory, monitor, NullLogger<TxRegistryGrain>.Instance,
            new FakePersistentState<TxRegistryState>()) { TimeProvider = clock };
        var proxy = Substitute.For<ITxRegistryGrain>();
        factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(proxy);
        var mark = Substitute.For<ITxRegistryHighWaterGrain>();
        mark.GetShardHighWaterAsync().Returns(Task.FromResult(0));
        factory.GetGrain<ITxRegistryHighWaterGrain>(Arg.Any<string>()).Returns(mark);

        var map = ShardMap.CreateDefault(2, 2);
        var keys = Enumerable.Range(0, 2).Select(shard =>
            Enumerable.Range(0, 1000).Select(i => $"key-{i}").First(key => map.Resolve(key) == shard)).ToList();
        var leaves = keys.Select((_, i) => CreateGrain(
            replicaId: $"progress-leaf-{i}",
            configureGrainFactory: f => f.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(proxy))).ToArray();
        var treeRegistry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(treeRegistry);
        treeRegistry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        treeRegistry.GetShardMapAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<ShardMap?>(map));
        treeRegistry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 2 }));
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", tree));
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(TimeProvider)).Returns(clock);
        var h = new GetManyProgressHarness
        {
            Lattice = new LatticeGrain(context, factory, monitor, TestOptionsResolver.ForFactory(factory, options),
                services, NullLogger<LatticeGrain>.Instance),
            Registry = registry, RegistryProxy = proxy, Factory = factory,
            Leaves = leaves, Keys = keys, Map = map, Clock = clock,
        };
        var snapshotCalls = 0;
        proxy.SnapshotWithRevisionAsync().Returns(async _ =>
        {
            if (snapshotCalls++ % 2 == 0) await h.PrepareAsync();
            return await registry.SnapshotWithRevisionAsync();
        });
        proxy.GetDecisionsRevisionAsync().Returns(_ => registry.GetDecisionsRevisionAsync());
        proxy.AcquireReadCaptureGateAsync(Arg.Any<Guid>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(async c =>
            {
                if (h.AcquireBarrier is { } barrier) await barrier.Task;
                await registry.AcquireReadCaptureGateAsync(c.Arg<Guid>(), c.Arg<TimeSpan>(), c.Arg<CancellationToken>());
                h.Gated = true;
                await h.PrepareAsync();
            });
        proxy.GetCaptureGateSnapshotAsync(Arg.Any<Guid>()).Returns(c => registry.GetCaptureGateSnapshotAsync(c.Arg<Guid>()));
        proxy.ReleaseCaptureGateAsync(Arg.Any<Guid>()).Returns(async c =>
        {
            h.Gated = false;
            return await registry.ReleaseCaptureGateAsync(c.Arg<Guid>());
        });

        TaskCompletionSource? firstRead = null;
        for (var i = 0; i < 2; i++)
        {
            var index = i;
            var shard = Substitute.For<IShardRootGrain>();
            factory.GetGrain<IShardRootGrain>($"{tree}/{i}", Arg.Any<string>()).Returns(shard);
            shard.GetManyAsync(Arg.Any<List<string>>()).Returns(async c =>
            {
                if (index == 0)
                {
                    firstRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    try
                    {
                        try
                        {
                            await registry.MarkCommittedAsync(h.Transaction);
                            await leaves[0].ApplyTxTerminalAsync(h.Transaction, committed: true);
                        }
                        catch (TxDecisionGateRefusedException ex) when (ex.Refusal == TxDecisionGateRefusal.DecisionGated)
                        {
                            h.RefusedCommits++;
                        }
                        var result = await leaves[0].GetManyAsync(c.Arg<List<string>>());
                        if (h.Gated) h.DuringGatedRead?.Invoke();
                        else h.OptimisticReads++;
                        return result;
                    }
                    finally
                    {
                        firstRead.SetResult();
                    }
                }
                await firstRead!.Task;
                return await leaves[1].GetManyAsync(c.Arg<List<string>>());
            });
        }
        return h;
    }

    [Test]
    public async Task GetManyAsync_continuous_commits_and_irreversible_drains_complete_atomically_under_a_bounded_gate()
    {
        var h = CreateGetManyProgressHarness();

        var values = await h.Lattice.GetManyAsync(h.Keys);

        Assert.Multiple(() =>
        {
            Assert.That(h.OptimisticReads, Is.EqualTo(3), "every optimistic attempt was invalidated");
            Assert.That(h.RefusedCommits, Is.EqualTo(1), "the fallback reached the real decision gate");
            Assert.That(values.Count, Is.EqualTo(2));
            Assert.That(values[h.Keys[0]], Is.EqualTo(new byte[] { 3 }));
            Assert.That(values[h.Keys[1]], Is.EqualTo(new byte[] { 3 }));
        });
        await h.Registry.MarkCommittedAsync(h.Transaction);
        await h.RegistryProxy.Received(1).ReleaseCaptureGateAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_gate_expiry_between_shard_reads_never_certifies_the_result()
    {
        var h = CreateGetManyProgressHarness();
        h.DuringGatedRead = () => h.Clock.Advance(LatticeGrain.GetManyDecisionGateLease + TimeSpan.FromSeconds(1));

        Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(() => h.Lattice.GetManyAsync(h.Keys));

        await h.Registry.MarkCommittedAsync(h.Transaction);
        await h.RegistryProxy.Received(1).ReleaseCaptureGateAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_gate_loss_before_D0_fails_with_the_retryable_read_fault_and_releases()
    {
        var h = CreateGetManyProgressHarness();
        h.RegistryProxy.GetCaptureGateSnapshotAsync(Arg.Any<Guid>())
            .Returns(Task.FromException<Dictionary<Guid, TxStatus>>(
                new TxDecisionGateRefusedException("getmany-progress", TxDecisionGateRefusal.GateLapsed, TimeSpan.Zero)));

        var fault = Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(() => h.Lattice.GetManyAsync(h.Keys));

        Assert.That(fault!.TreeId, Is.EqualTo("getmany-progress"));
        Assert.That(fault.InnerException, Is.TypeOf<TxDecisionGateRefusedException>());
        await h.Registry.MarkCommittedAsync(h.Transaction);
        await h.RegistryProxy.Received(1).ReleaseCaptureGateAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_topology_change_during_the_gated_read_fails_closed_and_releases()
    {
        var h = CreateGetManyProgressHarness();
        h.DuringGatedRead = () => h.Map.Version++;

        Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(() => h.Lattice.GetManyAsync(h.Keys));

        await h.Registry.MarkCommittedAsync(h.Transaction);
        await h.RegistryProxy.Received(1).ReleaseCaptureGateAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_cancellation_during_the_gated_read_releases_the_hold()
    {
        var h = CreateGetManyProgressHarness();
        using var stop = new CancellationTokenSource();
        h.DuringGatedRead = stop.Cancel;

        Assert.CatchAsync<OperationCanceledException>(() => h.Lattice.GetManyAsync(h.Keys, stop.Token));

        // The bounded wait can finish before the fan-out's finally. Its cleanup
        // still runs, and the lease bounds obstruction even if a callee stalls.
        await TestPoll.UntilAsync(() => !h.Gated, "decision gate released after cancellation", TimeSpan.FromSeconds(2));
        Assert.That(h.Gated, Is.False);
        await h.Registry.MarkCommittedAsync(h.Transaction);
    }

    [Test]
    public async Task GetManyAsync_shard_failure_during_the_gated_read_releases_the_hold()
    {
        var h = CreateGetManyProgressHarness();
        h.DuringGatedRead = () => throw new IOException("injected shard failure");

        Assert.ThrowsAsync<IOException>(() => h.Lattice.GetManyAsync(h.Keys));

        await h.Registry.MarkCommittedAsync(h.Transaction);
        await h.RegistryProxy.Received(1).ReleaseCaptureGateAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_registry_high_water_widening_during_the_gated_read_fails_closed()
    {
        var h = CreateGetManyProgressHarness();
        var mark = h.Factory.GetGrain<ITxRegistryHighWaterGrain>("getmany-progress");
        h.DuringGatedRead = () => mark.GetShardHighWaterAsync().Returns(Task.FromResult(1));

        Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(() => h.Lattice.GetManyAsync(h.Keys));

        await h.Registry.MarkCommittedAsync(h.Transaction);
    }

    [Test]
    public async Task GetManyAsync_stalled_acquisition_is_deadline_bounded_and_late_acquisition_is_cleaned_up()
    {
        var h = CreateGetManyProgressHarness();
        h.AcquireBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = h.Lattice.GetManyAsync(h.Keys);
        await TestPoll.UntilAsync(() => h.OptimisticReads == 3, "fallback acquisition reached");

        h.Clock.Advance(LatticeGrain.GetManyDecisionGateLease);
        Assert.ThrowsAsync<LatticeTransactionOutcomeUnavailableException>(() => read);

        h.AcquireBarrier.SetResult();
        await TestPoll.UntilAsync(() => h.RegistryProxy.ReceivedCalls()
            .Any(call => call.GetMethodInfo().Name == nameof(ITxRegistryGrain.ReleaseCaptureGateAsync)),
            "late acquisition released");
        await h.Registry.MarkCommittedAsync(h.Transaction);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Decision_gated_read_model_certifies_only_whole_batches_across_commit_drain_and_expiry_schedules(
        bool committedBeforeGate)
    {
        // Exhaust the two-leaf drain masks and lease-loss positions using the
        // real registry's gate and real leaf reads/terminals, not a simulated
        // assumption that "a drain follows a gated decision".
        var accepted = 0;
        var rejected = 0;
        for (var drainMask = 0; drainMask < 4; drainMask++)
        for (var expiryPosition = 0; expiryPosition < 3; expiryPosition++)
        {
            var h = CreateGetManyProgressHarness();
            await h.PrepareAsync();
            if (committedBeforeGate) await h.Registry.MarkCommittedAsync(h.Transaction);
            var token = Guid.NewGuid();
            await h.Registry.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, LatticeGrain.GetManyDecisionGateLease);
            var d0 = await h.Registry.GetCaptureGateSnapshotAsync(token);
            var observed = new bool[2];
            using (LatticeRegistrySnapshotContext.BeginScope(d0))
            {
                for (var leaf = 0; leaf < 2; leaf++)
                {
                    if (expiryPosition == leaf) h.Clock.Advance(LatticeGrain.GetManyDecisionGateLease + TimeSpan.FromSeconds(1));
                    try
                    {
                        await h.Registry.MarkCommittedAsync(h.Transaction);
                        if ((drainMask & (1 << leaf)) != 0)
                            await h.Leaves[leaf].ApplyTxTerminalAsync(h.Transaction, committed: true);
                    }
                    catch (TxDecisionGateRefusedException ex) when (ex.Refusal == TxDecisionGateRefusal.DecisionGated)
                    {
                    }
                    observed[leaf] = await h.Leaves[leaf].GetAsync(h.Keys[leaf]) is not null;
                }
            }
            if (await h.Registry.ReleaseCaptureGateAsync(token))
            {
                accepted++;
                Assert.That(observed[0], Is.EqualTo(observed[1]), $"drainMask={drainMask}, expiry={expiryPosition}");
                Assert.That(observed[0], Is.EqualTo(committedBeforeGate));
            }
            else rejected++;
        }
        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.EqualTo(4));
            Assert.That(rejected, Is.EqualTo(8));
        });
    }
}
