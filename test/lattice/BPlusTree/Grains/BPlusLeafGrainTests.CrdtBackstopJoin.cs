using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4611: a committed CRDT value that reaches a leaf through the terminal's
/// backstop - the coordinator's committed-values backstop on a leaf holding no
/// bucket, or a stranded prepared key a split donor forwards - carries the staged
/// state, computed from a stage-time snapshot. Installing it last-writer-wins at a
/// dominating stamp overwrote every contribution the row gained after that
/// snapshot (an OR-Set add's dot, another replica's count). The backstop now joins
/// the state into the row through the registered <see cref="CrdtShape"/> on a tree
/// whose merge mode resolves to a CRDT, as the drain folds the delta.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string BackstopJoinTreeId = "crdt-backstop-tree";

    private static BPlusLeafGrain CreateBackstopJoinGrain(
        LatticeMergeMode mode,
        ICommitLogWriter? commitLog = null,
        ILatticeEnvelopeCodec? envelopeCodec = null)
    {
        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = BackstopJoinTreeId } };
        return CreateGrain(
            state,
            commitLog: commitLog,
            mergeModeResolver: new FixedMergeModeResolver(mode),
            envelopeCodec: envelopeCodec);
    }

    /// <summary>A serialized OR-Set state holding one element added by <paramref name="replicaId"/>.</summary>
    private static async Task<byte[]> OrSetStateWithAsync(string element, string replicaId)
    {
        var scratch = CreateCrdtGrain();
        await scratch.ApplyCrdtDeltaAsync("seed", LatticeMergeMode.OrSet, SingleAddOrSetDelta(element, replicaId));
        return scratch.EntriesForTest["seed"].Value!;
    }

    private static byte[] GCounterState(params (string Replica, long Count)[] increments)
    {
        var counter = new GCounter();
        foreach (var (replica, count) in increments)
            counter.Increment(replica, count);
        return CrdtShape.ForGCounter().SerializeState(counter);
    }

    private static byte[] GCounterDeltaBytes(string replica, long amount) =>
        CrdtShape.ForGCounter().SerializeDelta!(new GCounterDelta
        {
            Increments = new Dictionary<string, long> { [replica] = amount },
        });

    private static bool OrSetContains(BPlusLeafGrain grain, string key, string element) =>
        JsonLatticeSerializer<OrSet>.Default.Deserialize(grain.EntriesForTest[key].Value!)
            .Contains(Encoding.UTF8.GetBytes(element));

    private static Task CoordinatorBackstopAsync(BPlusLeafGrain grain, Guid txid, string key, byte[] state) =>
        grain.ApplyTxTerminalAsync(txid, committed: true, new Dictionary<string, byte[]> { [key] = state });

    [Test]
    public async Task A_committed_values_backstop_joins_an_or_set_state_into_the_row()
    {
        var grain = CreateBackstopJoinGrain(LatticeMergeMode.OrSet);
        // An add of y acknowledged after the saga staged x from an empty snapshot.
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, SingleAddOrSetDelta("y", "r2"));
        var staged = await OrSetStateWithAsync("x", "r1");

        await CoordinatorBackstopAsync(grain, Guid.NewGuid(), "k", staged);

        Assert.Multiple(() =>
        {
            Assert.That(OrSetContains(grain, "k", "x"), Is.True, "the saga's committed add");
            Assert.That(OrSetContains(grain, "k", "y"), Is.True, "the acknowledged add the stage-time snapshot never saw");
            Assert.That(grain.CacheForTest.GetMergeMode("k"), Is.EqualTo(LatticeMergeMode.OrSet),
                "a snapshot capture must label the joined key with its merge mode");
        });
    }

    [Test]
    public async Task A_committed_values_backstop_joins_a_g_counter_state_into_the_row()
    {
        var grain = CreateBackstopJoinGrain(LatticeMergeMode.GCounter);
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.GCounter, GCounterDeltaBytes("B", 1));

        await CoordinatorBackstopAsync(grain, Guid.NewGuid(), "k", GCounterState(("A", 1)));

        var counter = (GCounter)CrdtShape.ForGCounter().DeserializeState(grain.EntriesForTest["k"].Value!);
        Assert.Multiple(() =>
        {
            Assert.That(counter.Value, Is.EqualTo(2), "B's acknowledged increment must survive the backstop");
            Assert.That(counter.Increments["A"], Is.EqualTo(1));
            Assert.That(counter.Increments["B"], Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_backstop_join_into_an_absent_key_installs_the_staged_state()
    {
        var grain = CreateBackstopJoinGrain(LatticeMergeMode.OrSet);

        await CoordinatorBackstopAsync(grain, Guid.NewGuid(), "k", await OrSetStateWithAsync("x", "r1"));

        Assert.That(OrSetContains(grain, "k", "x"), Is.True);
    }

    [Test]
    public async Task A_backstop_join_is_durable_and_replays_to_the_same_state()
    {
        var log = new FakeCommitLogWriter();
        var grain = CreateBackstopJoinGrain(LatticeMergeMode.OrSet, commitLog: log);
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, SingleAddOrSetDelta("y", "r2"));
        await CoordinatorBackstopAsync(grain, Guid.NewGuid(), "k", await OrSetStateWithAsync("x", "r1"));

        var backstop = log.Appended.Single(r => r.Key == "k" && r.IsBackstop);
        Assert.Multiple(() =>
        {
            Assert.That(backstop.Mode, Is.EqualTo(LatticeMergeMode.OrSet), "the record names the convergence rule it was joined under");
            Assert.That(backstop.Delta, Is.Null, "a full-state record must keep its Value through the encoder");
            Assert.That(backstop.Value, Is.EqualTo(grain.EntriesForTest["k"].Value), "the record carries the joined state");
            Assert.That(backstop.Timestamp, Is.EqualTo(grain.EntriesForTest["k"].Timestamp));
        });

        var fresh = CreateBackstopJoinGrain(LatticeMergeMode.OrSet);
        var projection = (ILeafProjection)fresh;
        foreach (var record in log.Appended)
            projection.Apply(WalRecordConverter.FromWalRecord(record));

        Assert.Multiple(() =>
        {
            Assert.That(OrSetContains(fresh, "k", "x"), Is.True);
            Assert.That(OrSetContains(fresh, "k", "y"), Is.True);
        });
    }

    [Test]
    public async Task A_backstop_join_strips_the_envelope_from_an_enveloped_stored_state()
    {
        var codec = new FakeEnvelopeCodec();
        var grain = CreateBackstopJoinGrain(LatticeMergeMode.OrSet, envelopeCodec: codec);
        grain.CacheForTest.StoreRow("k", LwwValue<byte[]>.Create(
            FakeEnvelopeCodec.Encode(await OrSetStateWithAsync("y", "r2")),
            new HybridLogicalClock { WallClockTicks = 1 }));

        await CoordinatorBackstopAsync(grain, Guid.NewGuid(), "k", await OrSetStateWithAsync("x", "r1"));

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest["k"].Value![0], Is.Not.EqualTo(FakeEnvelopeCodec.Magic),
                "the joined state is written back as a raw body, as the drain fold writes it");
            Assert.That(OrSetContains(grain, "k", "x"), Is.True);
            Assert.That(OrSetContains(grain, "k", "y"), Is.True);
        });
    }

    [Test]
    public async Task A_stranded_crdt_prepare_forwarded_to_its_declaring_sibling_is_joined_there()
    {
        // The stranded forward hop on its own, between two real leaves: a split
        // after the prepare narrowed the donor, so the terminal re-delivers the
        // bucketed key to the sibling that declares it, as a backstop carrying the
        // stage-time state and no delta. The sibling inherited the donor's tree
        // binding, so it resolves the same CRDT mode and joins.
        var siblingState = new FakePersistentState<LeafNodeState> { State = { TreeId = BackstopJoinTreeId } };
        var sibling = CreateGrain(siblingState, mergeModeResolver: new FixedMergeModeResolver(LatticeMergeMode.OrSet));
        await sibling.ApplyCrdtDeltaAsync("x-key", LatticeMergeMode.OrSet, SingleAddOrSetDelta("y", "r2"));

        var donorState = new FakePersistentState<LeafNodeState> { State = { TreeId = BackstopJoinTreeId } };
        var donor = CreateGrain(
            donorState,
            mergeModeResolver: new FixedMergeModeResolver(LatticeMergeMode.OrSet),
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain> { [StrandedSuccessorId] = sibling });
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticeDeltaContext.With(SingleAddOrSetDelta("x", "r1")))
            using (LatticePreparedContext.BeginScope())
            {
                await donor.SetAsync("x-key", await OrSetStateWithAsync("x", "r1"));
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        NarrowTo(donorState, "m");

        await donor.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.Multiple(() =>
        {
            Assert.That(donor.EntriesForTest.ContainsKey("x-key"), Is.False, "precondition: the key was forwarded, not drained");
            Assert.That(OrSetContains(sibling, "x-key", "x"), Is.True, "the saga's committed add reached the sibling");
            Assert.That(OrSetContains(sibling, "x-key", "y"), Is.True,
                "the add the sibling acknowledged after the split must survive the forwarded backstop");
        });
    }

    [Test]
    public async Task A_backstop_on_a_tree_with_no_crdt_mode_still_installs_last_writer_wins()
    {
        // No merge mode resolves (a single-cluster tree): the drain installs the
        // staged value last-writer-wins too, the documented concurrent-writer caveat
        // on LatticeStagedCrdtWrite, so the backstop keeps matching it.
        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = BackstopJoinTreeId } };
        var grain = CreateGrain(state);
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("before"));

        await CoordinatorBackstopAsync(grain, Guid.NewGuid(), "k", Encoding.UTF8.GetBytes("saga"));

        Assert.That(Encoding.UTF8.GetString(grain.EntriesForTest["k"].Value!), Is.EqualTo("saga"));
    }
}
