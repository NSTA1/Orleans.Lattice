using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Runs real rebuild and GC code over real in-memory WAL storage. Only grain
/// transport, source snapshots and persistence responses are controlled:
/// consumer queries delegate to the maintainer's actual published snapshot.
/// </summary>
[TestFixture]
public sealed class ViewMaintainerCandidateGcTests
{
    private const string Logical = "orders";
    private const string Bound = "orders-old";
    private const string Candidate = "orders-new";

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task Rebuild_real_gc_preserves_both_logs_until_durable_publication(int publicationFailure)
    {
        using var h = await Harness.CreateAsync();
        var boundaries = new List<string>();
        h.CandidateRegistry.RegisterAsync(Arg.Any<GrainId>()).Returns(async _ =>
        {
            h.Registered.Add(Candidate);
            boundaries.Add("registration");
            await AssertHeldAsync(h, candidateFloor: 0);
        });
        h.Reader.GetHeadOffsetAsync(Candidate, 0, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            boundaries.Add("capture");
            await AssertHeldAsync(h, candidateFloor: 0);
            return 2L;
        });
        h.Source.GetWithVersionAsync("k", Arg.Any<CancellationToken>()).Returns<Task<VersionedValue>>(async _ =>
        {
            boundaries.Add("scan");
            await h.AppendAsync(Candidate, 2);
            await AssertHeldAsync(h, candidateFloor: 2);
            return new VersionedValue { Value = [1], Version = Stamp(10) };
        });
        h.State.BeforeWrite = async () =>
        {
            boundaries.Add("publication");
            await AssertHeldAsync(h, candidateFloor: 2);
        };
        if (publicationFailure != 0)
        {
            if (publicationFailure == 1)
            {
                h.State.ThrowOnWrite = new TimeoutException("publication did not commit");
            }
            else
            {
                h.State.OnWriteState = _ => throw new TimeoutException("publication committed but response lost");
            }
            Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RebuildAsync());
        }
        else
        {
            await h.Grain.RebuildAsync();
        }

        Assert.That(boundaries, Is.EqualTo(new[] { "registration", "capture", "scan", "publication" }));
        await h.Gc.RunOnceAsync(Bound);
        await h.Gc.RunOnceAsync(Candidate);
        Assert.That(await h.OffsetsAsync(Bound), Is.EqualTo(publicationFailure != 0 ? new long[] { 1, 2 } : []),
            "only a successful checkpoint response retires the old entitlement; uncertainty retains it conservatively");
        Assert.That(await h.OffsetsAsync(Candidate), Is.EqualTo(new long[] { 2 }),
            "the entry appended during the scan remains replayable after either publication outcome");
        Assert.That(h.Logger.ReceivedCalls(), Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Rebuild_real_gc_candidate_hold_removal_control_trims_needed_entries(bool beforeCapture)
    {
        using var h = await Harness.CreateAsync();
        if (beforeCapture)
        {
            h.CandidateRegistry.RegisterAsync(Arg.Any<GrainId>()).Returns(async _ =>
            {
                h.Registered.Add(Candidate);
                await AssertHeldAsync(h, candidateFloor: 0);
                await RemoveCandidateHoldAsync(expectedTrimmed: 2);
            });
        }
        h.Source.GetWithVersionAsync("k", Arg.Any<CancellationToken>()).Returns<Task<VersionedValue>>(async _ =>
        {
            await h.AppendAsync(Candidate, 2);
            await AssertHeldAsync(h, candidateFloor: 2);
            await RemoveCandidateHoldAsync(expectedTrimmed: 1);
            throw new InvalidOperationException("the negative control must interrupt the rebuild");
        });

        Assert.ThrowsAsync<OperationCanceledException>(async () => await h.Grain.RebuildAsync());
        Assert.That(h.State.State.ActiveGeneration, Is.Zero);

        async Task RemoveCandidateHoldAsync(long expectedTrimmed)
        {
            // Remove only the candidate's offset answer. Keep its registration,
            // the old-bound answer and every other GC input identical.
            h.DropCandidateHold = true;
            var report = await h.Gc.RunOnceAsync(Candidate);
            Assert.That(report.EntriesTrimmed, Is.EqualTo(expectedTrimmed));
            Assert.That(await h.OffsetsAsync(Candidate), Is.Empty,
                "without the candidate hold, real GC removes entries needed by capture or scan replay");
            await h.Gc.RunOnceAsync(Bound);
            Assert.That(await h.OffsetsAsync(Bound), Is.EqualTo(new long[] { 1, 2 }));
            Assert.That(h.Logger.ReceivedCalls(), Is.Empty);
            throw new OperationCanceledException("stop after the destructive negative control");
        }
    }

    private static async Task AssertHeldAsync(Harness h, long candidateFloor)
    {
        await h.Gc.RunOnceAsync(Bound);
        await h.Gc.RunOnceAsync(Candidate);
        Assert.That(h.Logger.ReceivedCalls(), Is.Empty, "unreadable GC inputs must not silently retain the log");
        Assert.That(await h.OffsetsAsync(Bound), Is.EqualTo(new long[] { 1, 2 }),
            "GC must trim the eligible old prefix but preserve its persisted bound checkpoint suffix");
        Assert.That(await h.OffsetsAsync(Candidate),
            Is.EqualTo(candidateFloor == 0 ? new long[] { 0, 1 } : new long[] { 2 }),
            "registration/capture holds the entire candidate log; scan/publication holds its captured suffix");
    }

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private sealed class Harness : IDisposable
    {
        public required ViewMaintainerGrain Grain { get; init; }
        public required FakePersistentState<ViewCheckpointState> State { get; init; }
        public required IWalOffsetConsumerRegistryGrain CandidateRegistry { get; init; }
        public required ICommitLogReader Reader { get; init; }
        public required ILattice Source { get; init; }
        public required LatticeWalGc Gc { get; init; }
        public required InMemoryWalStorageProvider Provider { get; init; }
        public required ServiceProvider Services { get; init; }
        public required ILogger<LatticeWalGc> Logger { get; init; }
        public HashSet<string> Registered { get; } = [Bound];
        public bool DropCandidateHold { get; set; }

        public Task AppendAsync(string tree, long offset) => Provider.AppendBatchAsync(tree, 0,
            [new WalEntry
            {
                Offset = offset,
                Mutation = new LatticeMutation
                {
                    TreeId = tree, Key = $"k{offset}", Kind = MutationKind.Set,
                    Value = [1], Timestamp = Stamp(10), OriginClusterId = "site",
                },
            }], CancellationToken.None);

        public async Task<long[]> OffsetsAsync(string tree)
        {
            var offsets = new List<long>();
            await foreach (var entry in Provider.ReadAsync(tree, 0, -1, 100, CancellationToken.None))
            {
                offsets.Add(entry.Offset);
            }
            return offsets.ToArray();
        }

        public void Dispose() => Services.Dispose();

        public static async Task<Harness> CreateAsync()
        {
            var factory = Substitute.For<IGrainFactory>();
            var registry = Substitute.For<ILatticeRegistry>();
            registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
                new TreeRegistryEntry { PhysicalTreeId = Candidate, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
            factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
            var candidateRegistry = Substitute.For<IWalOffsetConsumerRegistryGrain>();
            var boundRegistry = Substitute.For<IWalOffsetConsumerRegistryGrain>();
            factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Candidate).Returns(candidateRegistry);
            factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Bound).Returns(boundRegistry);
            var source = Substitute.For<ILattice>();
            source.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                .Returns(call => Keys(call.ArgAt<string?>(0), true));
            var shadow = Substitute.For<ILattice>();
            shadow.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                .Returns(Keys(null, false));
            factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call => call.ArgAt<string>(0) == Candidate ? source : shadow);
            var root = Substitute.For<IShardRootGrain>();
            root.GetWithVersionAsync(Arg.Any<string>()).Returns(call =>
                source.GetWithVersionAsync(call.Arg<string>(), CancellationToken.None));
            factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(root);
            var projection = Substitute.For<ILatticeViewProjection>();
            projection.Project(Arg.Any<LatticeMutation>()).Returns(call =>
                new[] { ViewWrite.Upsert("k", [1], call.Arg<LatticeMutation>().Timestamp) });
            var catalog = Substitute.For<IViewCatalog>();
            catalog.TryGet("gc-view").Returns(new ViewRegistration("gc-view", Logical, projection));
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("viewmaintainer", "gc-view"));
            context.ActivationServices.Returns(Substitute.For<IServiceProvider>());
            var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
            viewOptions.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
            var state = new FakePersistentState<ViewCheckpointState>();
            state.State.BoundPhysicalTreeId = Bound;
            state.State.LegacyGenerationCeiling = 0;
            state.State.AppliedOffsets[0] = 0;
            var reader = Substitute.For<ICommitLogReader>();
            var grain = new ViewMaintainerGrain(context, factory, null!, NullLogger<ViewMaintainerGrain>.Instance,
                catalog, reader, Substitute.For<IWalSubscriber>(), Substitute.For<IWalCursorRegistry>(),
                TestOptionsResolver.ForFactory(factory, new LatticeOptions { WalPartitions = 1 }),
                viewOptions, null!, Substitute.For<ILatticeReplicationContext>(), null, null!, state);
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

            var provider = new InMemoryWalStorageProvider();
            var cursors = new InMemoryWalCursorRegistry();
            // GC uses an independent forward consumer, not the maintainer's
            // local HLC pin. Only the real offset-consumer path protects it.
            await cursors.ReportCursorAsync(Bound, "forward", Stamp(100));
            await cursors.ReportCursorAsync(Candidate, "forward", Stamp(100));
            var pins = Substitute.For<IWalMaterialiserPinGrain>();
            pins.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>()));
            pins.GetPinOffsetsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>()));
            factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pins);
            var consumer = Substitute.For<IWalOffsetConsumer>();
            var referenceRuntime = Substitute.For<IGrainReferenceRuntime>();
            referenceRuntime.Cast(Arg.Any<IAddressable>(), typeof(IWalOffsetConsumer)).Returns(consumer);
            var reference = new ConsumerReference(new GrainReferenceShared(context.GrainId.Type,
                GrainInterfaceType.Create("candidate-gc"), 0, referenceRuntime, default, null!, null!,
                context.ActivationServices), context.GrainId.Key);
            factory.GetGrain(context.GrainId).Returns(reference);
            var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            options.Get(Arg.Any<string>()).Returns(new LatticeOptions
                { WalPartitions = 1, WalDurabilityHoldCeilingBytes = 0 });
            var logger = Substitute.For<ILogger<LatticeWalGc>>();
            var services = new ServiceCollection().AddSingleton(logger).AddSingleton<IWalStorageProvider>(provider)
                .AddSingleton(factory).BuildServiceProvider();
            var h = new Harness
            {
                Grain = grain, State = state, CandidateRegistry = candidateRegistry, Reader = reader,
                Source = source, Provider = provider, Services = services, Logger = logger,
                Gc = new LatticeWalGc(services, cursors, options),
            };
            boundRegistry.GetConsumersAsync().Returns(_ => Task.FromResult<IReadOnlyList<GrainId>>(
                h.Registered.Contains(Bound) ? new[] { context.GrainId } : []));
            candidateRegistry.GetConsumersAsync().Returns(_ => Task.FromResult<IReadOnlyList<GrainId>>(
                h.Registered.Contains(Candidate) ? new[] { context.GrainId } : []));
            boundRegistry.UnregisterAsync(Arg.Any<GrainId>()).Returns(_ =>
            {
                h.Registered.Remove(Bound);
                return Task.CompletedTask;
            });
            candidateRegistry.RegisterAsync(Arg.Any<GrainId>()).Returns(_ =>
            {
                h.Registered.Add(Candidate);
                return Task.CompletedTask;
            });
            consumer.GetDurableReadPositionsAsync(Arg.Any<string>()).Returns(call =>
                h.DropCandidateHold && call.Arg<string>() == Candidate
                    ? Task.FromResult<long[]?>(null)
                    : grain.GetDurableReadPositionsAsync(call.Arg<string>()));
            reader.GetHeadOffsetAsync(Candidate, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(2L));
            for (var offset = 0; offset < 3; offset++) await h.AppendAsync(Bound, offset);
            for (var offset = 0; offset < 2; offset++) await h.AppendAsync(Candidate, offset);
            return h;
        }

        private static async IAsyncEnumerable<string> Keys(string? start, bool hasKey)
        {
            await Task.Yield();
            if (hasKey && (start is null || string.CompareOrdinal(start, "k") <= 0)) yield return "k";
        }

        private sealed class ConsumerReference(GrainReferenceShared shared, IdSpan key) : GrainReference(shared, key);
    }
}
