using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the dual-durability commit path on
/// <see cref="BPlusLeafGrain"/> introduced by the leaf write reorder.
/// Verifies the five steps (build / wal / apply / shadow / observer)
/// run in order, the WAL append is skipped when no adapter is
/// registered, the shadow persist is gated by the option, the
/// post-commit observer publish runs inside a commit-log scope, and
/// the per-step latency histogram emits one measurement per step.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string DualTreeId = "tree-dual";
    private const string DualReplicaId = "leaf-dual";

    private sealed record DualGrainHarness(
        BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        ICommitLogWriter? Writer,
        ILogger<BPlusLeafGrain>? Logger,
        RecordingMutationObserver Observer);

    private static DualGrainHarness CreateDualGrain(
        bool registerWriter = true,
        bool leafShadowWrites = true,
        ICommitLogWriter? writerOverride = null,
        Action<IServiceCollection>? configureServices = null,
        bool registerLogger = false)
    {
        var observer = new RecordingMutationObserver();

        var sc = new ServiceCollection();
        ICommitLogWriter? writer = null;
        if (registerWriter)
        {
            if (writerOverride is not null)
            {
                writer = writerOverride;
            }
            else
            {
                writer = Substitute.For<ICommitLogWriter>();
                writer.AppendAsync(Arg.Any<LatticeMutation>(), Arg.Any<CancellationToken>())
                    .Returns(callInfo => Task.FromResult(0L));
            }
            sc.AddSingleton(writer);
        }

        ILogger<BPlusLeafGrain>? loggerInstance = null;
        if (registerLogger)
        {
            loggerInstance = Substitute.For<ILogger<BPlusLeafGrain>>();
            var loggerFactory = Substitute.For<ILoggerFactory>();
            loggerFactory.CreateLogger(typeof(BPlusLeafGrain).FullName!).Returns(loggerInstance);
            sc.AddSingleton(loggerFactory);
        }

        configureServices?.Invoke(sc);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", DualReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = DualTreeId;

        var options = new LatticeOptions { LeafShadowWrites = leafShadowWrites };
        var grainFactory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            baseOptions: options,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(context, state, grainFactory, resolver, TestMutationObservers.With(observer));
        return new DualGrainHarness(grain, state, writer, loggerInstance, observer);
    }

    // --- WAL append --------------------------------------------------

    [Test]
    public async Task SetAsync_appends_to_commit_log_writer_when_resolved()
    {
        var harness = CreateDualGrain();

        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await harness.Writer!.Received(1).AppendAsync(
            Arg.Is<LatticeMutation>(m =>
                m.Kind == MutationKind.Set
                && m.Key == "k1"
                && m.TreeId == DualTreeId
                && !m.IsTombstone
                && m.Value!.SequenceEqual(Encoding.UTF8.GetBytes("v1"))),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SetAsync_does_not_append_when_writer_unresolved()
    {
        var harness = CreateDualGrain(registerWriter: false);

        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(harness.Writer, Is.Null);
        Assert.That(harness.State.WriteCount, Is.GreaterThan(0));
        var live = await harness.Grain.GetAsync("k1");
        Assert.That(live, Is.Not.Null);
    }

    [Test]
    public async Task DeleteAsync_appends_delete_mutation_when_writer_resolved()
    {
        var harness = CreateDualGrain();
        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        harness.Writer!.ClearReceivedCalls();

        var ok = await harness.Grain.DeleteAsync("k1");

        Assert.That(ok, Is.True);
        await harness.Writer!.Received(1).AppendAsync(
            Arg.Is<LatticeMutation>(m =>
                m.Kind == MutationKind.Delete
                && m.Key == "k1"
                && m.IsTombstone),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRangeAsync_appends_range_mutation_when_keys_match()
    {
        var harness = CreateDualGrain();
        await harness.Grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await harness.Grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await harness.Grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        harness.Writer!.ClearReceivedCalls();

        var result = await harness.Grain.DeleteRangeAsync("a", "c");

        Assert.That(result.Deleted, Is.EqualTo(2));
        await harness.Writer!.Received(1).AppendAsync(
            Arg.Is<LatticeMutation>(m =>
                m.Kind == MutationKind.DeleteRange
                && m.Key == "a"
                && m.EndExclusiveKey == "c"
                && m.IsTombstone),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DeleteRangeAsync_skips_wal_append_when_no_keys_match()
    {
        var harness = CreateDualGrain();
        await harness.Grain.SetAsync("z", Encoding.UTF8.GetBytes("1"));
        harness.Writer!.ClearReceivedCalls();

        var result = await harness.Grain.DeleteRangeAsync("a", "c");

        Assert.That(result.Deleted, Is.Zero);
        await harness.Writer!.DidNotReceive().AppendAsync(
            Arg.Any<LatticeMutation>(), Arg.Any<CancellationToken>());
    }

    // --- Shadow persist gating --------------------------------------

    [Test]
    public async Task SetAsync_persists_state_when_LeafShadowWrites_true()
    {
        var harness = CreateDualGrain(leafShadowWrites: true);

        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(harness.State.WriteCount, Is.GreaterThan(0));
        Assert.That(harness.State.State.Entries.ContainsKey("k1"), Is.True);
    }

    [Test]
    public async Task SetAsync_skips_state_persist_when_LeafShadowWrites_false()
    {
        var harness = CreateDualGrain(leafShadowWrites: false);

        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(harness.State.WriteCount, Is.Zero);
        // In-memory projection still applied.
        Assert.That(harness.State.State.Entries.ContainsKey("k1"), Is.True);
        var live = await harness.Grain.GetAsync("k1");
        Assert.That(live, Is.Not.Null);
    }

    [Test]
    public async Task DeleteAsync_skips_state_persist_when_LeafShadowWrites_false()
    {
        var harness = CreateDualGrain(leafShadowWrites: false);
        // Seed an entry in-memory by calling SetAsync (also without shadow persist).
        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var writesAfterSet = harness.State.WriteCount;

        var ok = await harness.Grain.DeleteAsync("k1");

        Assert.That(ok, Is.True);
        Assert.That(harness.State.WriteCount, Is.EqualTo(writesAfterSet));
    }

    // --- Crash-safety matrix ----------------------------------------

    [Test]
    public void SetAsync_surfaces_wal_exception_and_leaves_state_unchanged()
    {
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<LatticeMutation>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<long>(new InvalidOperationException("wal-down")));
        var harness = CreateDualGrain(writerOverride: writer);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1")));

        Assert.That(harness.State.State.Entries.ContainsKey("k1"), Is.False);
        Assert.That(harness.State.WriteCount, Is.Zero);
        Assert.That(harness.Observer.Mutations, Is.Empty);
    }

    [Test]
    public async Task SetAsync_swallows_shadow_failure_and_returns_success()
    {
        // FakePersistentState that throws on every WriteStateAsync.
        var state = new ThrowingPersistentState<LeafNodeState>();
        state.State.TreeId = DualTreeId;

        var observer = new RecordingMutationObserver();
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<LatticeMutation>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(0L));

        var loggerInstance = Substitute.For<ILogger<BPlusLeafGrain>>();
        var loggerFactory = Substitute.For<ILoggerFactory>();
        loggerFactory.CreateLogger(typeof(BPlusLeafGrain).FullName!).Returns(loggerInstance);

        var sc = new ServiceCollection();
        sc.AddSingleton(writer);
        sc.AddSingleton(loggerFactory);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", DualReplicaId));
        context.ActivationServices.Returns(services);

        var options = new LatticeOptions { LeafShadowWrites = true };
        var grainFactory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            baseOptions: options, maxLeafKeys: 128, shardCount: 1, factory: grainFactory);
        var grain = new BPlusLeafGrain(context, state, grainFactory, resolver, TestMutationObservers.With(observer));

        // Foreground call returns successfully despite shadow throw.
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True, "in-memory projection must be applied even when shadow throws");
        Assert.That(observer.Mutations, Has.Count.EqualTo(1), "observer publish runs after a swallowed shadow failure");
        loggerInstance.Received(1).Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<Exception>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    // --- Observer scope --------------------------------------------

    [Test]
    public async Task SetAsync_publishes_observer_inside_commit_log_scope()
    {
        bool? observedSourceFlag = null;
        var observer = new ScopeCapturingObserver(() => observedSourceFlag = LatticeCommitLogContext.Current);

        var sc = new ServiceCollection();
        var services = sc.BuildServiceProvider();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", DualReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = DualTreeId;
        var grainFactory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), maxLeafKeys: 128, shardCount: 1, factory: grainFactory);
        var grain = new BPlusLeafGrain(context, state, grainFactory, resolver, TestMutationObservers.With(observer));
        await grain.SetTreeIdAsync(DualTreeId);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(observedSourceFlag, Is.True, "observer must run inside a commit-log scope");
        Assert.That(LatticeCommitLogContext.Current, Is.False, "scope must restore on exit");
    }

    [Test]
    public async Task DeleteAsync_publishes_observer_inside_commit_log_scope()
    {
        bool? observedFlagOnDelete = null;
        var setSeen = false;
        var observer = new ScopeCapturingObserver(() =>
        {
            if (setSeen) observedFlagOnDelete = LatticeCommitLogContext.Current;
            setSeen = true;
        });

        var sc = new ServiceCollection();
        var services = sc.BuildServiceProvider();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", DualReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = DualTreeId;
        var grainFactory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), maxLeafKeys: 128, shardCount: 1, factory: grainFactory);
        var grain = new BPlusLeafGrain(context, state, grainFactory, resolver, TestMutationObservers.With(observer));
        await grain.SetTreeIdAsync(DualTreeId);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        Assert.That(observedFlagOnDelete, Is.True);
    }

    // --- Metrics ----------------------------------------------------

    [Test]
    public async Task SetAsync_emits_per_step_commit_duration_histogram()
    {
        var harness = CreateDualGrain();
        var measurements = new List<(string Step, double Value)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, lst) =>
            {
                if (ReferenceEquals(instrument.Meter, LatticeMetrics.Meter)
                    && instrument.Name == "orleans.lattice.leaf.commit.duration")
                {
                    lst.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<double>((inst, value, tags, _) =>
        {
            string? step = null;
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagStep) step = t.Value as string;
            }
            if (step is not null) lock (measurements) measurements.Add((step, value));
        });
        listener.Start();

        await harness.Grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var steps = measurements.Select(m => m.Step).ToHashSet();
        Assert.That(steps, Does.Contain("wal"));
        Assert.That(steps, Does.Contain("apply"));
        Assert.That(steps, Does.Contain("shadow"));
        Assert.That(steps, Does.Contain("observer"));
    }

    private sealed class ScopeCapturingObserver(Action capture) : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            capture();
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingPersistentState<T> : IPersistentState<T> where T : new()
    {
        public T State { get; set; } = new();
        public string Etag => string.Empty;
        public bool RecordExists => true;
        public Task ClearStateAsync() => Task.CompletedTask;
        public Task ReadStateAsync() => Task.CompletedTask;
        public Task WriteStateAsync() => throw new InvalidOperationException("storage-down");
    }
}

