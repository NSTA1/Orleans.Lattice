using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.SelfTests;

/// <summary>
/// The story framework's own proof (#5361): a story run of two stub chapters shows ordering,
/// dependant-blocking and the ledger hand-off; the isolated pipeline calls the contracts in its
/// documented order; and establishment runs once, after its consumes. No estate is needed.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class StoryRunnerSelfTests
{
    private static string ArtifactsDirectory => Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", "story-selftest");

    /// <summary>
    /// Given the plan consumer-first, the run still puts the producer first, both pass, and the
    /// consumer receives the producer's fact through the ledger.
    /// </summary>
    [Test]
    public async Task Story_run_orders_by_dependency_and_hands_the_ledger_on()
    {
        var runner = StubRunner(out var session, contracts: new ChapterContracts([]));

        var results = await runner.RunAllAsync();
        StoryReport.Publish(runner, "selftest-hand-off", "Story self-test: ordering and ledger hand-off (two stub chapters)");

        Assert.Multiple(() =>
        {
            Assert.That(runner.Order.Select(entry => entry.Id), Is.EqualTo(new[] { StubStory.Producer, StubStory.Consumer }), "the producer runs first although the plan named the consumer first");
            Assert.That(results.Select(result => result.Status), Is.All.EqualTo(ChapterStatus.Passed));
            Assert.That(session.Ledger.Get(StubStory.Trail), Is.EqualTo(new[] { StubStory.Producer, StubStory.Consumer }));
            Assert.That(session.Ledger.Get(StubStory.Received), Is.EqualTo("made by " + StubStory.Producer), "the consumer read the producer's fact");
            Assert.That(session.Ledger.WriterOf(StubStory.HandOff.Name), Is.EqualTo(StubStory.Producer));
            Assert.That(session.IsEstablished(StubStory.Consumer), Is.True, "a chapter that ran is marked produced");
        });
    }

    /// <summary>
    /// When the producer fails, the consumer never runs: it is blocked, by the producer's name,
    /// and never passes.
    /// </summary>
    [Test]
    public async Task A_failing_chapter_blocks_its_dependant_by_name()
    {
        var runner = StubRunner(out var session, contracts: new ChapterContracts([]));
        session.Ledger.Set(StubStory.FailProducer, true);

        var results = (await runner.RunAllAsync()).ToDictionary(result => result.Id);
        var report = StoryReport.Publish(runner, "selftest-blocked", "Story self-test: a failed chapter blocks its dependant (two stub chapters)");

        Assert.Multiple(() =>
        {
            Assert.That(results[StubStory.Producer].Status, Is.EqualTo(ChapterStatus.Failed));
            Assert.That(results[StubStory.Producer].Failure?.Message, Is.EqualTo(StubProducerChapter.FailureMessage));
            Assert.That(results[StubStory.Consumer].Status, Is.EqualTo(ChapterStatus.Blocked));
            Assert.That(results[StubStory.Consumer].BlockedBy, Is.EqualTo(new[] { StubStory.Producer }));
            Assert.That(results[StubStory.Consumer].Holds, Is.False, "a blocked chapter is never a pass");
            Assert.That(session.Ledger.Get(StubStory.Trail), Is.EqualTo(new[] { StubStory.Producer }), "the consumer never ran");
            Assert.That(session.Ledger.TryGet(StubStory.Received, out _), Is.False);
            Assert.That(report, Does.Contain("BLOCKED").And.Contain("blocked by " + StubStory.Producer));
        });
    }

    /// <summary>Asking for the consumer first still runs the producer first: a test per chapter may run in any order.</summary>
    [Test]
    public async Task Running_through_a_later_chapter_first_runs_its_consumes_first()
    {
        var runner = StubRunner(out var session, contracts: new ChapterContracts([]));

        var consumer = await runner.RunThroughAsync(StubStory.Consumer);
        var producer = await runner.RunThroughAsync(StubStory.Producer);

        Assert.Multiple(() =>
        {
            Assert.That(consumer.Status, Is.EqualTo(ChapterStatus.Passed));
            Assert.That(producer.Status, Is.EqualTo(ChapterStatus.Passed));
            Assert.That(session.Ledger.Get(StubStory.Trail), Is.EqualTo(new[] { StubStory.Producer, StubStory.Consumer }), "each chapter ran once, in order");
        });
    }

    /// <summary>A chapter whose journey does not exist is stood in by its contract, and its dependant runs on what the contract produced.</summary>
    [Test]
    public async Task A_missing_journey_is_stood_in_by_its_contract()
    {
        var producerContract = new DelegateContract(
            StubStory.Producer,
            context =>
            {
                context.Record(StubStory.HandOff, "made by the contract");
                return Task.CompletedTask;
            },
            context => context.Ledger.TryGet(StubStory.HandOff, out _) ? Task.CompletedTask : throw new InvalidOperationException("not established"));
        var contracts = new ChapterContracts([producerContract]);
        var plan = new[]
        {
            new StoryPlanEntry(StubStory.Producer, [], null),
            new StoryPlanEntry(StubStory.Consumer, [StubStory.Producer], typeof(StubConsumerChapter)),
        };
        var session = new StorySession(StubStory.Storyline, JourneyRunMode.Story, null, contracts, StoryPlan.Graph(plan), ArtifactsDirectory);
        var runner = new StoryRunner(session, plan, verifyContracts: false);

        var results = (await runner.RunAllAsync()).ToDictionary(result => result.Id);

        Assert.Multiple(() =>
        {
            Assert.That(results[StubStory.Producer].Status, Is.EqualTo(ChapterStatus.StoodIn));
            Assert.That(results[StubStory.Producer].Holds, Is.True);
            Assert.That(results[StubStory.Consumer].Status, Is.EqualTo(ChapterStatus.Passed));
            Assert.That(session.Ledger.Get(StubStory.Received), Is.EqualTo("made by the contract"));
        });
    }

    /// <summary>A stand-in with no contract fails, naming F6, and blocks its dependant.</summary>
    [Test]
    public async Task A_missing_journey_without_a_contract_fails_and_blocks()
    {
        var plan = new[]
        {
            new StoryPlanEntry(StubStory.Producer, [], null),
            new StoryPlanEntry(StubStory.Consumer, [StubStory.Producer], typeof(StubConsumerChapter)),
        };
        var session = new StorySession(StubStory.Storyline, JourneyRunMode.Story, null, new ChapterContracts([]), StoryPlan.Graph(plan), ArtifactsDirectory);

        var results = (await new StoryRunner(session, plan, verifyContracts: false).RunAllAsync()).ToDictionary(result => result.Id);

        Assert.Multiple(() =>
        {
            Assert.That(results[StubStory.Producer].Status, Is.EqualTo(ChapterStatus.Failed));
            Assert.That(results[StubStory.Producer].Failure, Is.TypeOf<MissingChapterContractException>());
            Assert.That(results[StubStory.Producer].Failure!.Message, Does.Contain("#5364"));
            Assert.That(results[StubStory.Consumer].Status, Is.EqualTo(ChapterStatus.Blocked));
        });
    }

    /// <summary>
    /// Isolated mode establishes the consumes, verifies them, runs the steps, then verifies the
    /// chapter's own produces - in that order.
    /// </summary>
    [Test]
    public async Task Isolated_run_establishes_and_verifies_consumes_then_verifies_the_chapter()
    {
        var log = new List<string>();
        var contracts = new ChapterContracts([DelegateContract.Logging(StubStory.Producer, log), DelegateContract.Logging(StubStory.Consumer, log)]);
        var session = new StorySession(StubStory.Storyline, JourneyRunMode.Isolated, null, contracts, StubGraph(), ArtifactsDirectory);
        session.Ledger.Set(StubStory.HandOff, "seeded");

        await StoryRunner.RunIsolatedAsync(session, StubStory.Consumer, new LoggingChapter(log));

        Assert.That(log, Is.EqualTo(new[]
        {
            "establish " + StubStory.Producer,
            "verify " + StubStory.Producer,
            "steps " + StubStory.Consumer,
            "verify " + StubStory.Consumer,
        }));
    }

    /// <summary>Establishing a chapter establishes its consumes first, and each chapter only once.</summary>
    [Test]
    public async Task Establishment_follows_consumes_and_runs_once()
    {
        var log = new List<string>();
        var graph = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            ["S1"] = [],
            ["S2"] = ["S1"],
            ["S3"] = ["S1", "S2"],
        };
        var contracts = new ChapterContracts(graph.Keys.Select(chapter => DelegateContract.Logging(chapter, log)));
        var session = new StorySession(StubStory.Storyline, JourneyRunMode.Isolated, null, contracts, graph, ArtifactsDirectory);

        await session.EstablishAsync("S3");
        await session.EstablishAsync("S2");
        await session.ContextFor("S3").EstablishAsync("S1");

        Assert.That(log, Is.EqualTo(new[] { "establish S1", "establish S2", "establish S3" }));
    }

    /// <summary>A contract that establishes its own chapter is refused instead of deadlocking.</summary>
    [Test]
    public void A_contract_establishing_its_own_chapter_is_refused()
    {
        var graph = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal) { ["S1"] = [] };
        var contracts = new ChapterContracts([new DelegateContract("S1", context => context.EstablishAsync("S1"), _ => Task.CompletedTask)]);
        var session = new StorySession(StubStory.Storyline, JourneyRunMode.Isolated, null, contracts, graph, ArtifactsDirectory);

        Assert.That(async () => await session.EstablishAsync("S1"), Throws.InvalidOperationException.With.Message.Contains("S1 <- S1"));
    }

    /// <summary>A plan with a cycle, a duplicate or an unknown consume is refused.</summary>
    [Test]
    public void A_malformed_plan_is_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => StoryPlan.Order([new StoryPlanEntry("S1", ["S2"], null), new StoryPlanEntry("S2", ["S1"], null)]),
                Throws.InvalidOperationException.With.Message.Contains("cycle"));
            Assert.That(() => StoryPlan.Order([new StoryPlanEntry("S1", [], null), new StoryPlanEntry("S1", [], null)]),
                Throws.InvalidOperationException.With.Message.Contains("twice"));
            Assert.That(() => StoryPlan.Order([new StoryPlanEntry("S1", ["S9"], null)]),
                Throws.InvalidOperationException.With.Message.Contains("S9"));
        });
    }

    /// <summary>The ledger refuses a missing fact with a reason, and a fact read as the wrong type.</summary>
    [Test]
    public void The_ledger_refuses_a_missing_fact_and_a_wrong_type()
    {
        var ledger = new StoryLedger();
        ledger.Set(new StoryKey<string>("selftest.text"), "value", "S1");

        Assert.Multiple(() =>
        {
            Assert.That(() => ledger.Get(StoryKeys.TaskBoardTree), Throws.TypeOf<KeyNotFoundException>().With.Message.Contains("taskboard.tree"));
            Assert.That(() => ledger.Get(new StoryKey<bool>("selftest.text")), Throws.TypeOf<InvalidCastException>());
            Assert.That(ledger.ToJson(), Does.Contain("\"writer\": \"S1\""));
        });
    }

    /// <summary>The run mode is read from LATTICE_E2E_MODE, isolated by default, and a typo fails.</summary>
    [Test]
    public void The_run_mode_defaults_to_isolated_and_refuses_a_typo()
    {
        Assert.Multiple(() =>
        {
            Assert.That(JourneyRunModes.Parse(null), Is.EqualTo(JourneyRunMode.Isolated));
            Assert.That(JourneyRunModes.Parse(" Story "), Is.EqualTo(JourneyRunMode.Story));
            Assert.That(JourneyRunModes.Parse("isolated"), Is.EqualTo(JourneyRunMode.Isolated));
            Assert.That(() => JourneyRunModes.Parse("stroy"), Throws.InvalidOperationException);
        });
    }

    /// <summary>
    /// A story leg tolerates only a failed contract stand-in that an open issue tracks; a real
    /// journey's failure, an untracked chapter, and a pass never resolve to an issue.
    /// </summary>
    [Test]
    public void Only_a_tracked_failed_stand_in_is_a_known_defect()
    {
        var known = new Dictionary<string, string>(StringComparer.Ordinal) { ["C1"] = "#1234" };
        var failed = new ChapterResult("C1", ChapterStatus.Failed, [], new TimeoutException("timed out"), TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(StoryRunFixture.KnownDefectFor(failed, isImplemented: false, known), Is.EqualTo("#1234"));
            Assert.That(StoryRunFixture.KnownDefectFor(failed, isImplemented: true, known), Is.Null, "a real journey's failure stays red");
            Assert.That(StoryRunFixture.KnownDefectFor(failed with { Id = "C2" }, isImplemented: false, known), Is.Null, "an untracked stand-in stays red");
            Assert.That(StoryRunFixture.KnownDefectFor(failed with { Status = ChapterStatus.StoodIn, Failure = null }, isImplemented: false, known), Is.Null);
        });
    }

    private static StoryRunner StubRunner(out StorySession session, ChapterContracts contracts)
    {
        var declared = DeclaredChapter.Discover(typeof(StoryRunnerSelfTests).Assembly)
            .Where(chapter => chapter.Storyline == StubStory.Storyline)
            .Select(chapter => new StoryPlanEntry(chapter.Id, chapter.After, chapter.Type))
            .OrderByDescending(entry => entry.Id, StringComparer.Ordinal)
            .ToList();
        Assert.That(declared.Select(entry => entry.Id), Is.EqualTo(new[] { StubStory.Consumer, StubStory.Producer }), "the plan is given consumer-first");
        session = new StorySession(StubStory.Storyline, JourneyRunMode.Story, null, contracts, StoryPlan.Graph(declared), ArtifactsDirectory);
        return new StoryRunner(session, declared, verifyContracts: false);
    }

    private static Dictionary<string, IReadOnlyList<string>> StubGraph() => new(StringComparer.Ordinal)
    {
        [StubStory.Producer] = [],
        [StubStory.Consumer] = [StubStory.Producer],
    };

    private sealed class LoggingChapter(List<string> log) : IStoryChapter
    {
        public Task RunAsync(ChapterContext context)
        {
            log.Add("steps " + context.Chapter);
            return Task.CompletedTask;
        }
    }
}
