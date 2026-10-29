using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.TraceValidation;

/// <summary>
/// Trace-validation spike for the atomic-commit protocol (issue #5159; write-up
/// in <c>spec/atomic-commit/TraceValidation.md</c>). Drives one real saga
/// through a test cluster with <see cref="AtomicCommitTrace.SinkForTest"/>
/// installed, maps the events the coordinator, the registry and the leaves
/// emitted onto <c>AtomicCommit.tla</c>'s actions, and asks TLC whether the
/// specification admits that behaviour. Deliberately wrong variants of the same
/// trace must be rejected with a message naming the event no action admits.
/// <para>
/// NOT A GATE, AND NEVER RUN BY CI. It needs a JVM and <c>tla2tools.jar</c>,
/// which only the TLC shards provision, and it is research, not a check
/// anything depends on. It is <see cref="ExplicitAttribute">explicit</see>, so
/// no category or name filter a CI lane uses selects it, and it carries the
/// <see cref="Category"/> category so it can be selected on purpose. The
/// write-up records how that exclusion was measured. Run it with:
/// <c>dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=TraceValidation"</c>
/// with <c>TLA_TOOLS_JAR</c> and <c>JAVA_HOME</c> set.
/// </para>
/// </summary>
[TestFixture]
[Explicit("Trace-validation spike (#5159): needs a JVM and tla2tools.jar; never run by CI.")]
[Category(Category)]
[Category("Integration")]
[NonParallelizable]
public sealed class AtomicCommitTraceValidationTests
{
    /// <summary>The category that selects this fixture, and only this fixture.</summary>
    public const string Category = "TraceValidation";

    private const string TreeName = "trace-validation-tree";

    private readonly ClusterFixture _fixture = new();
    private readonly AtomicCommitTraceRecorder _recorder = new();
    private string _java = string.Empty;
    private string _jar = string.Empty;
    private IReadOnlyList<SpecTraceEvent> _recorded = [];
    private IReadOnlyList<string> _raw = [];
    private bool _deployed;

    [OneTimeSetUp]
    public async Task RecordOneSagaAsync()
    {
        if (AtomicCommitTraceChecker.ResolveToolchain(out var missing) is not { } toolchain)
        {
            Assert.Ignore($"the TLA+ toolchain is unavailable: could not find {missing}.");
            return;
        }
        (_java, _jar) = toolchain;

        await _fixture.InitializeAsync();
        _deployed = true;
        AtomicCommitTrace.SinkForTest = _recorder;

        var tree = _fixture.Cluster.Client.GetGrain<ILattice>(TreeName);
        await tree.SetManyAtomicAsync(
        [
            new KeyValuePair<string, byte[]>("alpha", [1]),
            new KeyValuePair<string, byte[]>("beta", [2]),
        ]);

        var sagas = _recorder.SagasOn(TreeName);
        Assert.That(sagas, Has.Count.EqualTo(1), "exactly one saga should have run on the trace tree");

        // The client call returns once the saga completed, which includes its
        // registry cleanup. A leaf terminal is applied before the broadcast that
        // carried it returns, so the trace is complete here; the bounded wait
        // only absorbs an emission still in flight on another silo.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        do
        {
            _recorded = _recorder.MapSaga(sagas[0]);
            if (_recorded.Count >= 5)
            {
                break;
            }
            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        _raw = _recorder.DescribeSaga(sagas[0]).ToList();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        AtomicCommitTrace.SinkForTest = null;
        if (_deployed)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public void Recorded_commit_saga_covers_prepare_decide_every_terminal_and_cleanup()
    {
        Assert.That(
            _recorded.Select(e => e.Event),
            Is.EqualTo(new[] { "Prepare", "Decide", "Terminal", "Terminal", "Forget" }),
            "a committed two-key saga should emit one prepare outcome, one decision, one terminal per key and one cleanup");
    }

    [Test]
    public void Recorded_commit_saga_trace_is_accepted_by_AtomicCommit()
    {
        TestContext.Out.WriteLine("Raw events: " + string.Join(" ", _raw));
        TestContext.Out.WriteLine("Spec trace:");
        foreach (var e in _recorded)
        {
            TestContext.Out.WriteLine("  " + e.ToTla());
        }

        var result = AtomicCommitTraceChecker.Check(_java, _jar, _recorded);
        TestContext.Out.WriteLine($"TLC: {result.DistinctStates} distinct states for {result.TraceLength} events in {result.Elapsed.TotalSeconds:F2}s");

        Assert.That(result.Accepted, Is.True, result.Output);
    }

    [Test]
    public void Trace_with_a_terminal_before_the_decision_is_rejected_at_that_terminal()
    {
        // Broadcast-before-decision: the defect LinearizedTerminals exists to forbid.
        var perturbed = _recorded.ToList();
        var decide = perturbed.FindIndex(e => e.Event == "Decide");
        var terminal = perturbed.FindIndex(e => e.Event == "Terminal");
        (perturbed[decide], perturbed[terminal]) = (perturbed[terminal], perturbed[decide]);

        AssertRejectedAt(perturbed, decide + 1);
    }

    [Test]
    public void Trace_that_forgets_the_decision_before_the_last_terminal_is_rejected_at_the_forget()
    {
        // Early cleanup: the defect DecisionDurability's third conjunct forbids.
        var perturbed = _recorded.ToList();
        var forget = perturbed.FindIndex(e => e.Event == "Forget");
        var lastTerminal = perturbed.FindLastIndex(e => e.Event == "Terminal");
        var moved = perturbed[forget];
        perturbed.RemoveAt(forget);
        perturbed.Insert(lastTerminal, moved);

        AssertRejectedAt(perturbed, lastTerminal + 1);
    }

    [Test]
    public void Trace_that_commits_after_a_nack_is_rejected_at_the_decision()
    {
        // Commit without unanimity: the defect CommitIntegrity forbids.
        var perturbed = _recorded.ToList();
        var prepare = perturbed.FindIndex(e => e.Event == "Prepare");
        perturbed[prepare] = perturbed[prepare] with { Acks = perturbed[prepare].Keys![1..] };

        AssertRejectedAt(perturbed, perturbed.FindIndex(e => e.Event == "Decide") + 1);
    }

    private void AssertRejectedAt(IReadOnlyList<SpecTraceEvent> perturbed, int eventNumber)
    {
        var result = AtomicCommitTraceChecker.Check(_java, _jar, perturbed);
        TestContext.Out.WriteLine($"TLC rejected in {result.Elapsed.TotalSeconds:F2}s:");
        TestContext.Out.WriteLine(result.Output);

        Assert.Multiple(() =>
        {
            Assert.That(result.Accepted, Is.False, "the perturbed trace must not be accepted");
            Assert.That(result.Output, Does.Contain(AtomicCommitTraceChecker.RejectionMarker), result.Output);
            Assert.That(result.Output, Does.Match($@"{AtomicCommitTraceChecker.RejectionMarker}[^\n]*\n\s*{eventNumber},"), result.Output);
        });
    }
}
