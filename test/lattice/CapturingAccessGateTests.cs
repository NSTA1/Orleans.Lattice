namespace Orleans.Lattice.Tests;

/// <summary>
/// Acceptance-style unit test for issue #976: a capturing
/// <see cref="ILatticeAccessGate"/> fake can observe a
/// <see cref="LatticeAccessRequest"/> for representative single-key, batch,
/// range, CRDT, and lifecycle shapes with a correctly-populated
/// <see cref="LatticeAccessRequest.Operation"/> /
/// <see cref="LatticeAccessRequest.TreeId"/> / <see cref="LatticeAccessRequest.Key"/>.
/// Enforcement is not wired into grains by this issue, so the request is handed
/// to the gate directly rather than through a grain.
/// </summary>
[TestFixture]
public class CapturingAccessGateTests
{
    [Test]
    public async Task Gate_observes_a_single_key_write_request()
    {
        var gate = new CapturingAccessGate();
        var request = new LatticeAccessRequest("orders", LatticeOperation.Write, LatticeSubject.Anonymous, key: "k1");

        var decision = await gate.AuthorizeAsync(in request);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True);
            Assert.That(gate.Last.TreeId, Is.EqualTo("orders"));
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(gate.Last.Key, Is.EqualTo("k1"));
        });
    }

    [Test]
    public async Task Gate_observes_a_crdt_apply_request()
    {
        var gate = new CapturingAccessGate();
        var request = new LatticeAccessRequest("counters", LatticeOperation.CrdtApply, LatticeSubject.Anonymous, key: "hits");

        await gate.AuthorizeAsync(in request);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.CrdtApply));
        Assert.That(gate.Last.Key, Is.EqualTo("hits"));
    }

    [Test]
    public async Task Gate_observes_a_range_read_request()
    {
        var gate = new CapturingAccessGate();
        var request = new LatticeAccessRequest(
            "orders", LatticeOperation.RangeRead, LatticeSubject.Anonymous, rangeStart: "a", rangeEnd: "m");

        await gate.AuthorizeAsync(in request);

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.RangeRead));
            Assert.That(gate.Last.Key, Is.Null);
            Assert.That(gate.Last.RangeStart, Is.EqualTo("a"));
            Assert.That(gate.Last.RangeEnd, Is.EqualTo("m"));
        });
    }

    [Test]
    public async Task Gate_observes_an_atomic_batch_request()
    {
        var gate = new CapturingAccessGate();
        var request = new LatticeAccessRequest(
            "orders",
            LatticeOperation.AtomicWrite | LatticeOperation.Write | LatticeOperation.Delete,
            LatticeSubject.Anonymous);

        await gate.AuthorizeAsync(in request);

        Assert.That(gate.Last.Operation.HasFlag(LatticeOperation.AtomicWrite), Is.True);
        Assert.That(gate.Last.Operation.HasFlag(LatticeOperation.Write), Is.True);
        Assert.That(gate.Last.Operation.HasFlag(LatticeOperation.Delete), Is.True);
    }

    [Test]
    public async Task Gate_observes_a_lifecycle_bulk_load_request()
    {
        var gate = new CapturingAccessGate();
        var request = new LatticeAccessRequest("orders", LatticeOperation.BulkLoad, LatticeSubject.Anonymous);

        await gate.AuthorizeAsync(in request);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.BulkLoad));
        Assert.That(gate.Last.Key, Is.Null);
    }

    [Test]
    public async Task Gate_can_deny_a_request()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("nope"));
        var request = new LatticeAccessRequest("orders", LatticeOperation.Write, LatticeSubject.Anonymous, key: "k1");

        var decision = await gate.AuthorizeAsync(in request);

        Assert.That(decision.Allowed, Is.False);
        Assert.That(decision.Reason, Is.EqualTo("nope"));
    }

    /// <summary>
    /// A minimal capturing <see cref="ILatticeAccessGate"/> test double: records
    /// the last request it saw and returns a configurable decision (allow by
    /// default).
    /// </summary>
    private sealed class CapturingAccessGate(Func<LatticeAccessRequest, LatticeAccessDecision>? decide = null)
        : ILatticeAccessGate
    {
        private readonly Func<LatticeAccessRequest, LatticeAccessDecision> _decide =
            decide ?? (_ => LatticeAccessDecision.Allow());

        public LatticeAccessRequest Last { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            Last = request;
            return new ValueTask<LatticeAccessDecision>(_decide(request));
        }
    }
}
