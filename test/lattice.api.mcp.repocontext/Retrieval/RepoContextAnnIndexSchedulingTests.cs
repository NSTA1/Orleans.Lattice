using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the removal of the query-armed background build, and for the
/// scheduler seam that replaced it (#1872).
/// <para>
/// <c>A_declining_query_arms_no_build</c> is the load-bearing one. It is what makes
/// every "converged with no query" assertion elsewhere a claim about the mechanism
/// rather than about a test's own restraint: restore the old
/// <c>ArmBuild</c>/<c>Task.Run</c> pair in the registry and this test goes red,
/// because a declining query starts a build again.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexSchedulingTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static float[] Query()
    {
        var vector = new float[AnnPlaneFixture.Space.Dimension];
        vector[0] = 1f;
        return vector;
    }

    [Test]
    public async Task A_declining_query_arms_no_build()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);

        // Many declining queries. Under the previous design the first of these
        // started a background build through Task.Run, so the plane converged
        // shortly afterwards. It must not now: scheduling is the coordinator's job,
        // and a query is not a scheduler.
        for (var i = 0; i < 64; i++)
        {
            var outcome = await fixture.SearchAsync(Query(), 5, Ct);
            Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Bootstrapping));
        }

        // A one-sided detector, and deliberately a generous one. A reinstated
        // Task.Run is scheduled within microseconds and this rig's whole build is
        // in-memory and completes in milliseconds, so any background build starts
        // well inside this window - the wait exists to make the absence provable,
        // not to wait for something expected. Streaming the store of record is what
        // a build does first and cannot skip, so it is the earliest possible
        // evidence rather than a lagging one.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline && fixture.Source.FullEnumerations == 0)
        {
            await Task.Delay(10, Ct);
        }

        Assert.Multiple(() =>
        {
            Assert.That(fixture.Source.FullEnumerations, Is.Zero,
                "A query must not start a build. Arming from the traffic path is what made the mechanism that "
                + "accelerates queries reachable only from a query - not crash-safe, and never started at all on "
                + "a repository nobody queries. Streaming the store of record is the first thing a build does.");
            Assert.That(fixture.Store.RecordsWritten, Is.Zero,
                "Nothing may be persisted by a query either: a build that got as far as a flush would have "
                + "written records.");
            Assert.That(
                fixture.Registry.TryGetProgress(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress),
                Is.True,
                "a declining query still creates the handle, so progress is reportable");
            Assert.That(progress.Phase, Is.Not.EqualTo(VectorIndexBuildPhase.Ready),
                "and the index must be no closer to serving than it was before the queries arrived");
        });
    }

    [Test]
    public async Task The_scheduler_arms_the_coordinator_for_the_live_space()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var coordinator = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        grainFactory
            .GetGrain<IRepoContextAnnIndexBuildGrain>(RepoContextAnnIndexKeys.BuildGrainKey("acme", space))
            .Returns(coordinator);

        var scheduler = new RepoContextAnnIndexScheduler(
            grainFactory,
            new RepoContextIndexingOptions(),
            NullLogger<RepoContextAnnIndexScheduler>.Instance,
            StubEmbedder.Instance);

        Assert.That(scheduler.CanSchedule, Is.True);
        Assert.That(await scheduler.TryArmAsync("acme", Ct), Is.True);
        await coordinator.Received(1).EnsureBuildingAsync(space);
    }

    [Test]
    public async Task The_scheduler_arms_nothing_without_an_embedding_provider()
    {
        // No provider means nothing is ever embedded, so there is no corpus to index
        // and scheduling a build would spend a coordinator on an empty space.
        var grainFactory = Substitute.For<IGrainFactory>();
        var scheduler = new RepoContextAnnIndexScheduler(
            grainFactory, new RepoContextIndexingOptions(), NullLogger<RepoContextAnnIndexScheduler>.Instance);

        Assert.That(scheduler.CanSchedule, Is.False);
        Assert.That(await scheduler.TryArmAsync("acme", Ct), Is.False);
        grainFactory.DidNotReceive().GetGrain<IRepoContextAnnIndexBuildGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task The_scheduler_arms_nothing_when_the_switch_is_off()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var scheduler = new RepoContextAnnIndexScheduler(
            grainFactory,
            new RepoContextIndexingOptions { AnnIndexScheduling = false },
            NullLogger<RepoContextAnnIndexScheduler>.Instance,
            StubEmbedder.Instance);

        Assert.That(scheduler.CanSchedule, Is.False);
        Assert.That(await scheduler.TryArmAsync("acme", Ct), Is.False);
        grainFactory.DidNotReceive().GetGrain<IRepoContextAnnIndexBuildGrain>(Arg.Any<string>());
    }

    [Test]
    public void The_scheduler_rejects_its_null_arguments()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var options = new RepoContextIndexingOptions();
        var logger = NullLogger<RepoContextAnnIndexScheduler>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextAnnIndexScheduler(null!, options, logger), Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexScheduler(grainFactory, null!, logger), Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexScheduler(grainFactory, options, null!), Throws.ArgumentNullException);
            Assert.That(
                async () => await new RepoContextAnnIndexScheduler(grainFactory, options, logger)
                    .TryArmAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// A minimal embedding provider that advertises a space and nothing else. The
    /// scheduler only ever reads <see cref="IEmbeddingProvider.Space"/>.
    /// </summary>
    private sealed class StubEmbedder : IEmbeddingProvider
    {
        public static StubEmbedder Instance { get; } = new();

        public EmbeddingSpace Space { get; } = new("test-model", 8, normalized: true);

        public Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task<EmbeddingResult> EmbedAsync(
            IReadOnlyList<string> texts,
            EmbeddingTextType textType,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("The scheduler never embeds.");
    }
}
