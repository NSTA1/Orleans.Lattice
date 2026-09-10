using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detector for the <c>NoMixedTerminals</c> row of <c>spec/Refinement.md</c>
/// (issue #2552, epic #2556). The row claims that a saga records exactly one
/// <c>TxStatus</c>, <em>so</em> its per-leaf terminals are uniformly commit or
/// uniformly abort.
/// <para>
/// WHAT WAS ALREADY COVERED, AND WHAT WAS NOT. The consequent alone was
/// covered on both outcomes:
/// <c>CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard</c>
/// already pins a uniform abort fan-out, and its sibling pins the uniform
/// commit fan-out. Neither observes the registry, so neither can see the
/// antecedent: nothing asserted that the verdict every terminal carries is
/// <em>the same single decision</em> the saga recorded on the per-tree
/// <see cref="ITxRegistryGrain"/>. A refactor that reintroduced a second,
/// independently-derived verdict on the broadcast side would leave both of
/// those tests green for as long as the two verdicts happened to agree, which
/// is precisely the mechanism the row names. These tests close that: they
/// assert the recorded decision and the broadcast verdicts <em>together</em>,
/// over the same transaction id, on both outcomes.
/// </para>
/// <para>
/// NON-VACUITY IS LOAD-BEARING HERE. "Uniform" is unfalsifiable over a
/// single-element fan-out, and a "no leaf received the opposite verdict"
/// assertion is satisfied just as well by a fan-out that never happened. Both
/// tests therefore pin the touched-shard set at more than one shard and pin
/// the observed verdict count against it before asserting uniformity, and each
/// is the other's positive control: between them the harness is shown able to
/// observe both a <c>true</c> and a <c>false</c> terminal verdict.
/// </para>
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// How many distinct physical shards the mixed-terminal tests fan out
    /// across. Two would make "uniform" falsifiable; three leaves room for a
    /// perturbation that flips a single shard in the middle of the fan-out
    /// rather than only at an edge.
    /// </summary>
    private const int NoMixedTerminalsShardCount = 3;

    /// <summary>
    /// Records what one saga told the outside world about its outcome: the
    /// <c>committed</c> flag and transaction id of every
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> the broadcast
    /// issued, and the transaction id of every registry decision write.
    /// </summary>
    private sealed class TerminalVerdictLog
    {
        private readonly Lock _gate = new();
        private readonly List<bool> _broadcastVerdicts = [];
        private readonly List<Guid> _broadcastTxIds = [];
        private readonly List<Guid> _committedDecisions = [];
        private readonly List<Guid> _abortedDecisions = [];

        public void RecordBroadcast(Guid txid, bool committed)
        {
            lock (_gate)
            {
                _broadcastTxIds.Add(txid);
                _broadcastVerdicts.Add(committed);
            }
        }

        public void RecordCommitDecision(Guid txid)
        {
            lock (_gate) { _committedDecisions.Add(txid); }
        }

        public void RecordAbortDecision(Guid txid)
        {
            lock (_gate) { _abortedDecisions.Add(txid); }
        }

        public IReadOnlyList<bool> BroadcastVerdicts
        {
            get { lock (_gate) { return [.. _broadcastVerdicts]; } }
        }

        public IReadOnlyList<Guid> BroadcastTxIds
        {
            get { lock (_gate) { return [.. _broadcastTxIds]; } }
        }

        public IReadOnlyList<Guid> CommittedDecisions
        {
            get { lock (_gate) { return [.. _committedDecisions]; } }
        }

        public IReadOnlyList<Guid> AbortedDecisions
        {
            get { lock (_gate) { return [.. _abortedDecisions]; } }
        }
    }

    /// <summary>
    /// Builds an <see cref="AtomicWriteGrain"/> wired to an explicit
    /// <see cref="ITxRegistryGrain"/> substitute and a single shared
    /// <see cref="IShardRootGrain"/> substitute, with every terminal broadcast
    /// and every registry decision write recorded into the returned
    /// <see cref="TerminalVerdictLog"/>. Deliberately self-contained rather
    /// than reusing <c>CreateGrain</c> / <c>CreateGrainWithRegistry</c>:
    /// neither hands back both the <see cref="ILattice"/> substitute (needed to
    /// inject the abort) and the registry substitute (needed to observe the
    /// single recorded decision).
    /// </summary>
    private static (AtomicWriteGrain grain,
                     FakePersistentState<AtomicWriteState> state,
                     ILattice lattice,
                     TerminalVerdictLog log) CreateGrainRecordingTerminalVerdicts()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);

        var log = new TerminalVerdictLog();

        // One shared shard substitute behind every physical shard grain id, so
        // the recorded verdict list is the saga's whole terminal fan-out.
        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));
        shard.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(call => Task.FromResult(
                Enumerable.Repeat<LwwEntry?>(null, call.Arg<List<string>>().Count).ToList()));
        shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));
        shard.AppendTxTerminalAsync(
                Arg.Any<Guid>(),
                Arg.Any<bool>(),
                Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
                Arg.Any<CancellationToken>(),
                Arg.Any<bool>())
            .Returns(call =>
            {
                log.RecordBroadcast(call.Arg<Guid>(), (bool)call[1]);
                return Task.FromResult<WalRecord?>(null);
            });

        var registry = Substitute.For<ITxRegistryGrain>();
        grainFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);
        registry.MarkCommittedAsync(Arg.Any<Guid>())
            .Returns(call => { log.RecordCommitDecision(call.Arg<Guid>()); return Task.CompletedTask; });
        registry.MarkAbortedAsync(Arg.Any<Guid>())
            .Returns(call => { log.RecordAbortDecision(call.Arg<Guid>()); return Task.CompletedTask; });
        // Pin the late-pickup loop's participant fetch to a stable empty set so
        // the fan-out under assertion is exactly the touched-shard set.
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(new List<int>()));

        var routing = new RoutingInfo(TreeId, NoMixedTerminalsShardMap());
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(routing);
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(routing);

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var opts = new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = new FakePersistentState<AtomicWriteState>();
        var grain = new AtomicWriteGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            new LoggerFactory().CreateLogger<AtomicWriteGrain>(),
            state);
        return (grain, state, lattice, log);
    }

    private static ShardMap NoMixedTerminalsShardMap() =>
        ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);

    /// <summary>
    /// Builds a batch whose keys resolve to exactly
    /// <see cref="NoMixedTerminalsShardCount"/> distinct physical shards under
    /// the default routing map. Chosen by resolving candidate keys rather than
    /// hard-coded, so a change to the hash or to the default shard count
    /// re-derives the batch instead of silently collapsing it onto one shard -
    /// which would make every uniformity assertion below vacuously true.
    /// </summary>
    private static List<KeyValuePair<string, byte[]>> EntriesSpanningDistinctShards()
    {
        var map = NoMixedTerminalsShardMap();
        var seen = new HashSet<int>();
        var entries = new List<KeyValuePair<string, byte[]>>();

        for (var i = 0; entries.Count < NoMixedTerminalsShardCount && i < 10_000; i++)
        {
            var key = $"mixed-terminal-{i}";
            if (seen.Add(map.Resolve(key)))
            {
                entries.Add(new KeyValuePair<string, byte[]>(key, [(byte)i]));
            }
        }

        Assert.That(entries, Has.Count.EqualTo(NoMixedTerminalsShardCount),
            "The fixture could not build a batch spanning distinct shards, so the uniformity " +
            "assertions below would be vacuous.");
        return entries;
    }

    [Test]
    public async Task Aborting_saga_broadcasts_its_single_recorded_abort_verdict_to_every_touched_shard()
    {
        // spec/Refinement.md, property NoMixedTerminals. The compensation
        // path is the half that runs when something has already gone wrong,
        // so it is the half where a mixed fan-out would do the most damage:
        // a leaf handed a commit terminal for an aborted saga drains its
        // pending bucket and surfaces a value no other leaf holds.
        var (grain, state, lattice, log) = CreateGrainRecordingTerminalVerdicts();
        var entries = EntriesSpanningDistinctShards();
        var failingKey = entries[^1].Key;

        // Fail the shard bucket carrying the last key, so the saga pivots to
        // Compensate after at least one other bucket has already prepared -
        // the state in which a per-leaf verdict could plausibly diverge.
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(call =>
            {
                var slice = (List<KeyValuePair<string, byte[]>>)call[0];
                foreach (var entry in slice)
                {
                    if (string.Equals(entry.Key, failingKey, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException("simulated mid-batch failure");
                    }
                }
                return Task.CompletedTask;
            });

        var caught = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, entries));
        Assert.That(caught!.Message, Does.Contain("simulated mid-batch failure"));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed),
            "The saga must reach a terminal phase, or the fan-out under assertion never ran.");

        var verdicts = log.BroadcastVerdicts;
        var touched = state.State.TouchedShards;

        // Non-vacuity first. "Uniformly abort" says nothing over an empty or
        // single-element fan-out, and the DidNotReceive-shaped assertion below
        // would pass just as happily against a broadcast that never happened.
        Assert.That(touched, Has.Count.GreaterThan(1),
            "A single touched shard cannot exhibit a mixed terminal, so the uniformity " +
            "assertions would be vacuous.");
        Assert.That(verdicts, Has.Count.EqualTo(touched.Count),
            "Every touched shard must receive exactly one terminal.");

        Assert.Multiple(() =>
        {
            // The consequent: uniformly abort, and no leaf handed a commit.
            Assert.That(verdicts.Distinct().Count(), Is.EqualTo(1),
                $"Terminals were mixed across the fan-out: [{string.Join(", ", verdicts)}].");
            Assert.That(verdicts, Is.All.False,
                "An aborting saga must broadcast abort to every touched shard.");

            // The antecedent, and the link between them: exactly one TxStatus,
            // it is the abort, and every terminal carries that same txid. This
            // is what the sibling fixtures cannot see, because they never wire
            // the registry.
            Assert.That(log.AbortedDecisions, Has.Count.EqualTo(1),
                "The saga must record exactly one TxStatus.");
            Assert.That(log.CommittedDecisions, Is.Empty,
                "An aborting saga must not also record a commit decision.");
            Assert.That(log.BroadcastTxIds.Distinct().Count(), Is.EqualTo(1),
                "Every terminal in one saga must carry that saga's single transaction id.");
            // Stated as a set equality rather than by indexing either list: when
            // the property is broken one of these lists is empty, and indexing
            // it would abandon the run with an IndexOutOfRangeException instead
            // of reporting which half diverged.
            Assert.That(log.BroadcastTxIds.Distinct(), Is.EquivalentTo(log.AbortedDecisions),
                "The broadcast verdict must be the decision the saga recorded, not an " +
                "independently-derived second verdict.");
        });
    }

    [Test]
    public async Task Committing_saga_broadcasts_its_single_recorded_commit_verdict_to_every_touched_shard()
    {
        // The complement, and the positive control for the test above: it is
        // what proves this harness can observe a `true` terminal verdict and a
        // recorded commit at all. Without it, the abort test's "no leaf
        // received a commit" assertion could be satisfied by a harness that
        // can never record one.
        var (grain, state, _, log) = CreateGrainRecordingTerminalVerdicts();
        var entries = EntriesSpanningDistinctShards();

        await grain.ExecuteAsync(TreeId, entries);

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));

        var verdicts = log.BroadcastVerdicts;
        var touched = state.State.TouchedShards;

        Assert.That(touched, Has.Count.GreaterThan(1),
            "A single touched shard cannot exhibit a mixed terminal, so the uniformity " +
            "assertions would be vacuous.");
        Assert.That(verdicts, Has.Count.EqualTo(touched.Count),
            "Every touched shard must receive exactly one terminal.");

        Assert.Multiple(() =>
        {
            Assert.That(verdicts.Distinct().Count(), Is.EqualTo(1),
                $"Terminals were mixed across the fan-out: [{string.Join(", ", verdicts)}].");
            Assert.That(verdicts, Is.All.True,
                "A committing saga must broadcast commit to every touched shard.");

            Assert.That(log.CommittedDecisions, Has.Count.EqualTo(1),
                "The saga must record exactly one TxStatus.");
            Assert.That(log.AbortedDecisions, Is.Empty,
                "A committing saga must not also record an abort decision.");
            Assert.That(log.BroadcastTxIds.Distinct().Count(), Is.EqualTo(1),
                "Every terminal in one saga must carry that saga's single transaction id.");
            Assert.That(log.BroadcastTxIds.Distinct(), Is.EquivalentTo(log.CommittedDecisions),
                "The broadcast verdict must be the decision the saga recorded, not an " +
                "independently-derived second verdict.");
        });
    }
}
