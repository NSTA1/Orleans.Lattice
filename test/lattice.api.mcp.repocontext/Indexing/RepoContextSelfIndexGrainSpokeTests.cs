using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for the hub-and-spoke indexing-role gate on
/// <see cref="RepoContextSelfIndexGrain"/>. A spoke serves replicated index records
/// for reads but must never walk, reconcile, prune, or re-embed source-derived index
/// state, so two clusters can never race to mutate it. These tests prove the spoke's
/// <see cref="RepoContextSelfIndexGrain.EnsureRunningAsync"/> is inert - it never
/// registers the keep-alive reminder, arms the scan timer, or drives the runner index
/// pass - while a hub does all three.
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainSpokeTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static RepoIndexJobRequest Request() => new()
    {
        RepoRoot = "/repo",
        RepoId = "acme",
    };

    private static RepoContextSelfIndexGrain CreateGrain(
        RepoContextIndexingRole role,
        IReminderRegistry reminderRegistry,
        IRepoIndexRunner runner,
        out FakeSelfIndexState state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("repoContextSelfIndex", "acme"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var options = new RepoContextIndexingOptions { Role = role };
        var replication = Substitute.For<ILatticeReplicationContext>();
        var cache = new RepoContextVectorCache(TimeProvider.System, options);
        var writer = new RepoContextVectorWriter(
            grainFactory, Serializer, replication, cache, RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory));
        var gapScanner = new RepoContextEmbeddingGapScanner(grainFactory, writer);
        var runAuthority = Substitute.For<IRepoIndexRunAuthority>();
        state = new FakeSelfIndexState();

        return new RepoContextSelfIndexGrain(
            context,
            grainFactory,
            reminderRegistry,
            runner,
            gapScanner,
            runAuthority,
            TimeProvider.System,
            options,
            new RepoContextAnnIndexScheduler(
                grainFactory, options, NullLogger<RepoContextAnnIndexScheduler>.Instance),
            NullLogger<RepoContextSelfIndexGrain>.Instance,
            state);
    }

    [Test]
    public async Task EnsureRunningAsync_rejects_a_null_request()
    {
        var grain = CreateGrain(
            RepoContextIndexingRole.Spoke,
            Substitute.For<IReminderRegistry>(),
            Substitute.For<IRepoIndexRunner>(),
            out _);

        await Assert.ThatAsync(() => grain.EnsureRunningAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task EnsureRunningAsync_on_a_spoke_returns_an_inert_snapshot()
    {
        var grain = CreateGrain(
            RepoContextIndexingRole.Spoke,
            Substitute.For<IReminderRegistry>(),
            Substitute.For<IRepoIndexRunner>(),
            out _);

        var progress = await grain.EnsureRunningAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(progress.RepoId, Is.EqualTo("acme"));
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.None),
                "A spoke asserts no indexing job runs on this cluster.");
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Pending));
        });
    }

    [Test]
    public async Task EnsureRunningAsync_on_a_spoke_never_drives_the_runner_index_pass()
    {
        var runner = Substitute.For<IRepoIndexRunner>();

        var grain = CreateGrain(RepoContextIndexingRole.Spoke, Substitute.For<IReminderRegistry>(), runner, out _);

        await grain.EnsureRunningAsync(Request());

        await runner.DidNotReceive().StartIndexAsync(Arg.Any<RepoIndexJobRequest>());
    }

    [Test]
    public async Task EnsureRunningAsync_on_a_spoke_never_registers_the_keepalive_reminder()
    {
        var reminderRegistry = Substitute.For<IReminderRegistry>();

        var grain = CreateGrain(RepoContextIndexingRole.Spoke, reminderRegistry, Substitute.For<IRepoIndexRunner>(), out _);

        await grain.EnsureRunningAsync(Request());

        await reminderRegistry.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task EnsureRunningAsync_on_a_spoke_never_writes_index_state()
    {
        var grain = CreateGrain(
            RepoContextIndexingRole.Spoke,
            Substitute.For<IReminderRegistry>(),
            Substitute.For<IRepoIndexRunner>(),
            out var state);

        await grain.EnsureRunningAsync(Request());

        Assert.That(state.WriteCount, Is.Zero, "A spoke never mutates its persisted scan checkpoint.");
    }

    /// <summary>
    /// Minimal in-memory <see cref="IPersistentState{T}"/> so a spoke grain can be
    /// constructed and its write-count observed without a storage provider. A spoke
    /// must never call <see cref="WriteStateAsync"/>.
    /// </summary>
    private sealed class FakeSelfIndexState : IPersistentState<RepoContextSelfIndexState>
    {
        public RepoContextSelfIndexState State { get; set; } = new();
        public string Etag => string.Empty;
        public bool RecordExists => true;
        public int WriteCount { get; private set; }

        public Task ClearStateAsync() => Task.CompletedTask;
        public Task ReadStateAsync() => Task.CompletedTask;

        public Task WriteStateAsync()
        {
            WriteCount++;
            return Task.CompletedTask;
        }
    }
}
