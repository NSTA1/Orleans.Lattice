using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the durable, reminder-anchored coordinator that schedules an
/// approximate index build (#1872).
/// <para>
/// <b>The defect it replaces.</b> The build was armed by a declining query through
/// a fire-and-forget <c>Task.Run</c>, which put the work that makes queries fast
/// behind a query. It died with the process with nothing to resume it (the index
/// was measured reloading persisted state on one activation in nine), the first
/// query after a restart both paid the un-indexed cost and was the trigger, and a
/// repository nobody queried never indexed itself at all.
/// </para>
/// <para>
/// <b>What the assertions below are worth.</b> "The build converged and no query
/// was issued" is only meaningful in company: on its own it is satisfied by a test
/// that simply does not call search. Its pair is
/// <c>RepoContextAnnIndexSchedulingTests.A_declining_query_arms_no_build</c>, which
/// asserts the removed trigger really is removed - restore the old arming and that
/// test goes red, which is what makes "zero queries" a claim about the mechanism
/// rather than about the test's own restraint.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexBuildGrainTests
{
    private const string RepoId = AnnPlaneFixture.RepoId;

    /// <summary>
    /// A ceiling on pump ticks. Every tick is a real bounded build step, so for a
    /// given corpus and batch size the count is deterministic; reaching the ceiling
    /// means the build never converged.
    /// </summary>
    private const int MaxTicks = 512;

    private static EmbeddingSpaceTag Space => AnnPlaneFixture.Space;

    private static RepoContextAnnOptions PlaneOptions() => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 8,
        MaxItemsPerChunk = 8,
        RetrainAfterUpdateFraction = 0d,
    };

    /// <summary>
    /// One simulated process: a fresh registry and a fresh grain activation over
    /// durable state that outlives both. Disposing it is a process death - abrupt,
    /// with no graceful hand-off, exactly as a crash is.
    /// </summary>
    private sealed class Activation : IDisposable
    {
        public required RepoContextAnnIndexRegistry Registry { get; init; }

        public required RepoContextAnnIndexBuildGrain Grain { get; init; }

        public required IReminderRegistry Reminders { get; init; }

        public required IGrainContext Context { get; init; }

        public void Dispose() => Registry.Dispose();
    }

    /// <summary>
    /// The durable half of the rig: the index tree and the coordinator's persisted
    /// state, both of which survive every simulated restart.
    /// </summary>
    private sealed class Durable
    {
        public InMemoryAnnBackingFactory Backing { get; } = new();

        public FakeBuildState State { get; } = new();

        public RepoContextAnnOptions Plane { get; init; } = PlaneOptions();

        public RepoContextIndexingOptions Indexing { get; init; } = new();

        /// <summary>Starts a new process over the same durable state.</summary>
        public Activation Start()
        {
            var registry = new RepoContextAnnIndexRegistry(
                Backing, Plane, NullLogger<RepoContextAnnIndexRegistry>.Instance);

            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create(
                "repoContextAnnIndexBuild", RepoContextAnnIndexKeys.BuildGrainKey(RepoId, Space)));

            // The coordinator base class arms its phase timer through the
            // activation's service provider, so a test that drives arming
            // end-to-end needs a timer registry wired in.
            var services = Substitute.For<IServiceProvider>();
            services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
            context.ActivationServices.Returns(services);

            var reminders = Substitute.For<IReminderRegistry>();
            var grain = new RepoContextAnnIndexBuildGrain(
                context,
                reminders,
                registry,
                Backing,
                Indexing,
                NullLogger<RepoContextAnnIndexBuildGrain>.Instance,
                State);

            return new Activation
            {
                Registry = registry,
                Grain = grain,
                Reminders = reminders,
                Context = context,
            };
        }

        /// <summary>Seeds the store of record with a ring of unit vectors.</summary>
        public void SeedRing(int count)
        {
            var source = Backing.For(RepoId, Space).Source;
            for (var i = 0; i < count; i++)
            {
                var angle = 2d * Math.PI * i / count;
                var vector = new float[Space.Dimension];
                vector[0] = (float)Math.Cos(angle);
                vector[1] = (float)Math.Sin(angle);
                source.Set($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
            }
        }
    }

    /// <summary>
    /// The coordinator's persisted state, held in memory so it survives a simulated
    /// process death exactly as grain storage does.
    /// </summary>
    private sealed class FakeBuildState : IPersistentState<RepoContextAnnIndexBuildState>
    {
        public RepoContextAnnIndexBuildState State { get; set; } = new();

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

    /// <summary>
    /// Drives the coordinator's phase pump, which is what its grain timer does on
    /// every tick. Stops as soon as the coordinator reports itself converged.
    /// </summary>
    private static async Task<int> PumpAsync(Activation activation, int maxTicks = MaxTicks)
    {
        for (var tick = 1; tick <= maxTicks; tick++)
        {
            await activation.Grain.ProcessNextPhaseAsync();
            if (await activation.Grain.IsConvergedAsync())
            {
                return tick;
            }
        }

        return maxTicks;
    }

    [Test]
    public async Task An_idle_repository_converges_with_no_query_at_all()
    {
        // COLD CONVERGENCE. Nothing queries this repository at any point: the
        // coordinator is armed by the sweep, pumped by its timer, and reaches Ready
        // on its own. Under the previous design this repository would have stayed
        // unindexed forever, because the only thing that started a build was a query.
        var durable = new Durable();
        durable.SeedRing(64);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);
        var ticks = await PumpAsync(process);

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "the index must converge unaided");
            Assert.That(durable.State.State.Converged, Is.True,
                "Convergence must be recorded durably, not merely reached in memory.");
            Assert.That(
                process.Registry.TryGetProgress(RepoId, Space, out var progress)
                && progress.Phase == VectorIndexBuildPhase.Ready,
                Is.True,
                "the plane must actually be Ready, not just flagged");
        });
    }

    [Test]
    public async Task A_build_killed_part_way_resumes_after_a_restart_and_reaches_ready_with_no_query()
    {
        // THE HEADLINE. This is the behaviour the previous design failed: a build
        // interrupted by a process death was a Task that simply ceased to exist, and
        // nothing resumed it until a query happened to arrive.
        var durable = new Durable();
        durable.SeedRing(64);

        int partialTicks;
        using (var first = durable.Start())
        {
            await first.Grain.EnsureBuildingAsync(Space);

            // Advance a few slices and then kill the process mid-build. Two ticks is
            // deliberately short of convergence for this corpus and batch size.
            partialTicks = await PumpAsync(first, maxTicks: 2);
            Assert.That(durable.State.State.Converged, Is.False,
                "the fixture must actually stop mid-build, or the restart proves nothing");
        }

        // A new process over the same durable state. Nothing calls EnsureBuildingAsync
        // and nothing issues a query: this is precisely the keep-alive reminder
        // reactivating the coordinator, whose activation hook re-arms the pump.
        using var second = durable.Start();
        await ((IGrainBase)second.Grain).OnActivateAsync(CancellationToken.None);
        var ticks = await PumpAsync(second);

        Assert.Multiple(() =>
        {
            Assert.That(partialTicks, Is.EqualTo(2), "the first process must have done real, partial work");
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "A build interrupted by a process death must be resumed by the restart itself. Leaving it for the "
                + "next query is what made the first query after a restart both pay the un-indexed cost and be "
                + "the trigger.");
            Assert.That(durable.State.State.Converged, Is.True);
            Assert.That(
                second.Registry.TryGetProgress(RepoId, Space, out var progress)
                && progress.Phase == VectorIndexBuildPhase.Ready,
                Is.True);
        });
    }

    [Test]
    public async Task A_reactivated_coordinator_re_arms_its_pump_from_the_activation_hook()
    {
        // The perpetual-coordinator override. Without it the pump is started only by
        // the call that happened to activate the grain - and a reminder-driven
        // reactivation makes no such call, so the build would sit inert with a live
        // reminder ticking beside it.
        var durable = new Durable();
        durable.SeedRing(32);

        using (var first = durable.Start())
        {
            await first.Grain.EnsureBuildingAsync(Space);
            await PumpAsync(first, maxTicks: 1);
        }

        using var second = durable.Start();
        await ((IGrainBase)second.Grain).OnActivateAsync(CancellationToken.None);

        second.Context.ActivationServices.Received().GetService(typeof(ITimerRegistry));
    }

    [Test]
    public async Task Reaching_ready_unregisters_the_keepalive_and_deactivates()
    {
        // TERMINAL CLEANUP. A converged coordinator that kept its reminder would
        // reactivate itself once a minute forever, for every repository and every
        // embedding space on the box.
        var durable = new Durable();
        durable.SeedRing(32);

        var reminder = Substitute.For<IGrainReminder>();
        using var process = durable.Start();
        process.Reminders
            .GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(reminder));

        await process.Grain.EnsureBuildingAsync(Space);
        var ticks = await PumpAsync(process);

        Assert.That(ticks, Is.LessThan(MaxTicks), "the coordinator must converge for cleanup to be reachable");
        await process.Reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
        process.Context.Received().Deactivate(Arg.Any<DeactivationReason>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_converged_coordinator_still_opens_the_index_once_per_activation()
    {
        // The durable index is shared but the in-memory one the registry serves from
        // is per process, so an activation that trusted the persisted "converged"
        // flag and skipped its step would leave the registry with no open handle -
        // handing the next query exactly the cost this coordinator exists to have
        // already paid.
        var durable = new Durable();
        durable.SeedRing(32);

        using (var first = durable.Start())
        {
            await first.Grain.EnsureBuildingAsync(Space);
            Assert.That(await PumpAsync(first), Is.LessThan(MaxTicks));
        }

        using var second = durable.Start();
        Assert.That(second.Registry.TryGetProgress(RepoId, Space, out _), Is.False,
            "a fresh process starts with no open handle, which is the premise of this test");

        await ((IGrainBase)second.Grain).OnActivateAsync(CancellationToken.None);
        await PumpAsync(second, maxTicks: 4);

        Assert.That(
            second.Registry.TryGetProgress(RepoId, Space, out var progress)
            && progress.Phase == VectorIndexBuildPhase.Ready,
            Is.True,
            "The reload must happen on the coordinator's turn, with no query involved.");
    }

    [Test]
    public async Task The_scheduling_switch_being_off_arms_nothing()
    {
        var durable = new Durable
        {
            Indexing = new RepoContextIndexingOptions { AnnIndexScheduling = false },
        };
        durable.SeedRing(32);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);

        await process.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        Assert.That(durable.State.State.Space.IsSpecified, Is.False,
            "The off state is honest: nothing is scheduled and no intent is persisted, so a box with the switch "
            + "off answers every semantic query from the exact scan and maintains no index at all.");
    }

    [Test]
    public async Task Exact_retrieval_arms_nothing_because_such_a_host_maintains_no_index()
    {
        var durable = new Durable
        {
            Indexing = new RepoContextIndexingOptions
            {
                SemanticRetrieval = RepoContextSemanticRetrievalMode.Exact,
            },
        };
        durable.SeedRing(32);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);

        await process.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task A_space_that_does_not_match_the_coordinator_key_is_refused()
    {
        // The key is the identity. A coordinator that accepted another pair's space
        // would build an index under a prefix it does not own - and that index's own
        // recovery path range-deletes everything under its prefix.
        var durable = new Durable();
        using var process = durable.Start();

        await Assert.ThatAsync(
            () => process.Grain.EnsureBuildingAsync(new EmbeddingSpaceTag("other-model", 8, VectorNormalization.UnitL2)),
            Throws.ArgumentException);
    }

    [Test]
    public async Task An_unspecified_space_is_refused()
    {
        var durable = new Durable();
        using var process = durable.Start();

        await Assert.ThatAsync(() => process.Grain.EnsureBuildingAsync(default), Throws.ArgumentException);
    }
}
