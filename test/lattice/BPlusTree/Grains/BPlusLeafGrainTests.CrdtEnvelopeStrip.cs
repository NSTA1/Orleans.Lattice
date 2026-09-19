using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the CRDT fold's envelope-strip symmetry. The fold has
/// two byte inputs - the incoming delta and the stored state it folds into - and
/// both must have the version envelope stripped before the shape deserializes
/// them. Historically only the delta was stripped.
/// </summary>
/// <remarks>
/// <para>
/// The asymmetry is latent on an unversioned tree and on a key whose live typed
/// shadow is intact, which is why it survived: a CRDT apply that follows another
/// CRDT apply re-uses the shadow and never decodes the stored bytes at all. It
/// wakes when a <b>whole-value write</b> lands on a CRDT key of a tree opted into
/// schema versioning - the write interceptor stamps an envelope on the stored
/// value and the whole-value store evicts the typed shadow, so the next CRDT apply
/// must decode those enveloped bytes.
/// </para>
/// <para>
/// The failure is unusually bad: the envelope magic <c>0xFE</c> is never a valid
/// UTF-8 lead byte, so the state decode fails at byte zero, and because every
/// retry re-reads the same durable bytes the key becomes permanently unwritable
/// while still reading back cleanly. Each test below covers one distinct call
/// site, because a single arm cannot prove a multi-site clause.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private static BPlusLeafGrain CreateEnvelopedCrdtGrain(
        FakeEnvelopeCodec codec,
        FakePersistentState<LeafNodeState>? state = null)
    {
        state ??= new FakePersistentState<LeafNodeState>();
        if (string.IsNullOrEmpty(state.State.TreeId))
            state.State.TreeId = "test-tree";
        return CreateGrain(
            state,
            mergeModeResolver: new FixedMergeModeResolver(LatticeMergeMode.OrSet),
            envelopeCodec: codec);
    }

    /// <summary>An OR-Set add delta carrying a dot unique to <paramref name="element"/>.</summary>
    private static byte[] EnvelopeTestAddDelta(string element, string replicaId) =>
        JsonLatticeSerializer<OrSetDelta>.Default.Serialize(new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot
                {
                    Element = Encoding.UTF8.GetBytes(element),
                    ReplicaId = replicaId,
                    Counter = 1,
                },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        });

    /// <summary>
    /// Produces a genuine serialized OR-Set state containing one element, by
    /// folding it on a scratch grain. Using the real fold output rather than a
    /// hand-built blob keeps the seeded bytes byte-identical to what production
    /// stores.
    /// </summary>
    private static async Task<byte[]> PlainOrSetStateAsync(string element)
    {
        var scratch = CreateCrdtGrain();
        await scratch.ApplyCrdtDeltaAsync("seed", LatticeMergeMode.OrSet, EnvelopeTestAddDelta(element, "seed-r"));
        return scratch.EntriesForTest["seed"].Value!;
    }

    private static OrSet Observed(BPlusLeafGrain grain, string key) =>
        JsonLatticeSerializer<OrSet>.Default.Deserialize(grain.EntriesForTest[key].Value!);

    // ── site 1: BPlusLeafGrain.CrdtApply - live apply over a plain stored row ──

    [Test]
    public async Task CrdtApply_folds_into_an_enveloped_stored_state()
    {
        var codec = new FakeEnvelopeCodec();
        var grain = CreateEnvelopedCrdtGrain(codec);

        // A whole-value write of an already-enveloped state, exactly as the schema
        // write interceptor produces above the grain. This also evicts the typed
        // shadow, so the next apply is forced to decode the stored bytes.
        await grain.SetAsync("k", FakeEnvelopeCodec.Encode(await PlainOrSetStateAsync("apple")));
        Assert.That(grain.TryGetTypedShadowForTest<object>("k", out _), Is.False,
            "a whole-value write must evict the typed shadow, which is what forces the stored-byte decode");

        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, EnvelopeTestAddDelta("banana", "r2"));

        var observed = Observed(grain, "k");
        Assert.Multiple(() =>
        {
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True,
                "the enveloped stored state must be stripped before decode, not dropped");
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("banana")), Is.True);
        });
    }

    /// <summary>
    /// The fold output must be written back as a raw body, never re-enveloped:
    /// re-stamping at fold time would make apply-time and replay-time bytes differ
    /// and break the WAL-replay determinism contract on
    /// <see cref="ILatticeEnvelopeCodec"/>.
    /// </summary>
    [Test]
    public async Task CrdtApply_writes_the_folded_state_back_unenveloped()
    {
        var codec = new FakeEnvelopeCodec();
        var grain = CreateEnvelopedCrdtGrain(codec);

        await grain.SetAsync("k", FakeEnvelopeCodec.Encode(await PlainOrSetStateAsync("apple")));
        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, EnvelopeTestAddDelta("banana", "r2"));

        Assert.That(grain.EntriesForTest["k"].Value![0], Is.Not.EqualTo(FakeEnvelopeCodec.Magic));
    }

    // ── site 2: BPlusLeafGrain.CrdtApply - the defensive deferred-row branch ──

    /// <summary>
    /// The deferred-row branch is documented as unreachable (a deferred row always
    /// carries a live typed shadow), and it exists so correctness never depends on
    /// that invariant holding. This test constructs the invariant break deliberately
    /// so the branch is exercised rather than trusted - it is a separate call site
    /// and a fix applied only to the sibling branch would leave it defective.
    /// </summary>
    [Test]
    public async Task CrdtApply_folds_into_an_enveloped_deferred_row_without_a_shadow()
    {
        var codec = new FakeEnvelopeCodec();
        var grain = CreateEnvelopedCrdtGrain(codec);

        var enveloped = FakeEnvelopeCodec.Encode(await PlainOrSetStateAsync("apple"));

        // Whole-value write first, to evict the typed shadow; then install a
        // deferred row whose materialiser yields the enveloped bytes and
        // deliberately do not re-store the shadow.
        await grain.SetAsync("k", enveloped);
        var stored = grain.EntriesForTest["k"];
        var metadata = stored with { Value = null };
        grain.CacheForTest.StoreDeferredRow("k", metadata, () => enveloped, enveloped.Length);
        Assert.That(grain.TryGetTypedShadowForTest<object>("k", out _), Is.False);

        await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, EnvelopeTestAddDelta("banana", "r2"));

        var observed = Observed(grain, "k");
        Assert.Multiple(() =>
        {
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True);
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("banana")), Is.True);
        });
    }

    // ── site 3: BPlusLeafGrain.PendingTx - prepared-saga terminal-commit fold ──

    [Test]
    public async Task PreparedCrdtFold_folds_into_an_enveloped_stored_state()
    {
        var codec = new FakeEnvelopeCodec();
        var grain = CreateEnvelopedCrdtGrain(codec);
        var txid = Guid.NewGuid();

        await grain.SetAsync("k", FakeEnvelopeCodec.Encode(await PlainOrSetStateAsync("apple")));
        await PrepareCrdtWriteAsync(grain, txid, "k", EnvelopeTestAddDelta("banana", "r2"));

        await grain.ApplyTxTerminalAsync(txid, committed: true);

        var observed = Observed(grain, "k");
        Assert.Multiple(() =>
        {
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True,
                "the terminal-commit fold must strip the stored state, not only the prepared delta");
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("banana")), Is.True);
        });
    }

    /// <summary>
    /// Pins the property the whole clause rents: the strip must be a pure,
    /// version-agnostic header removal that never upcasts, so the same durable
    /// bytes fold identically at apply time and on every replay. Asserted here
    /// rather than assumed, because the correctness argument for stripping the
    /// stored state depends on it.
    /// </summary>
    [Test]
    public void EnvelopeStrip_is_version_agnostic_and_body_preserving()
    {
        var codec = new FakeEnvelopeCodec();
        var body = Encoding.UTF8.GetBytes("{\"a\":1}");

        var atV1 = codec.StripForFold(FakeEnvelopeCodec.Encode(body, version: 1));
        var atV7 = codec.StripForFold(FakeEnvelopeCodec.Encode(body, version: 7));

        Assert.Multiple(() =>
        {
            Assert.That(atV1, Is.EqualTo(body));
            Assert.That(atV7, Is.EqualTo(body), "the strip must not vary with the stamped version");
            Assert.That(codec.StripForFold(body), Is.SameAs(body),
                "an unenveloped body must be handed through by reference, so an unversioned tree pays nothing");
        });
    }
}
