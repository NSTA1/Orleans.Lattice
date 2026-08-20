namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeStateVisibilityFilter.CanReadAnyKeyAsync"/> -
/// the existence-hiding read signal that distinguishes a subject with a partial
/// (prefix) grant, which must keep a tree visible, from one with no grant at all.
/// Exercised directly against fakes so the probe-present and probe-absent
/// branches are asserted without standing up a cluster, including the fallback to
/// the plain per-tree decision when no <see cref="ILatticeReadGrantProbe"/> is
/// registered.
/// </summary>
[TestFixture]
public sealed class LatticeStateVisibilityFilterGrantProbeTests
{
    private static readonly LatticeSubject Named = new("alice");

    private sealed class FakeGate(Func<LatticeAccessRequest, LatticeAccessDecision> decide) : ILatticeAccessGate
    {
        public int CallCount { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return new ValueTask<LatticeAccessDecision>(decide(request));
        }
    }

    private sealed class FakeGrantProbe(bool grant) : ILatticeReadGrantProbe
    {
        public int CallCount { get; private set; }

        public LatticeOperation LastOperation { get; private set; }

        public ValueTask<bool> HasAnyGrantAsync(
            string treeId,
            LatticeSubject subject,
            LatticeOperation operation,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            LastOperation = operation;
            return new ValueTask<bool>(grant);
        }
    }

    private static FakeGate AllowGate() => new(static _ => LatticeAccessDecision.Allow());

    private static FakeGate DenyGate() => new(static _ => LatticeAccessDecision.Deny("no"));

    [Test]
    public async Task CanReadAnyKeyAsync_falls_back_to_the_gate_when_no_probe_is_registered()
    {
        var gate = AllowGate();
        var filter = new LatticeStateVisibilityFilter(gate, membership: null, LatticeStateApiReadVisibility.Auto);

        var can = await filter.CanReadAnyKeyAsync("tree", Named, CancellationToken.None);

        Assert.That(can, Is.True);
        Assert.That(gate.CallCount, Is.EqualTo(1), "the per-tree gate is the fallback when no read-grant probe exists");
    }

    [Test]
    public async Task CanReadAnyKeyAsync_fallback_denies_when_the_gate_denies()
    {
        var filter = new LatticeStateVisibilityFilter(DenyGate(), membership: null, LatticeStateApiReadVisibility.Auto);

        var can = await filter.CanReadAnyKeyAsync("tree", Named, CancellationToken.None);

        Assert.That(can, Is.False);
    }

    [Test]
    public async Task CanReadAnyKeyAsync_uses_the_probe_when_one_is_registered()
    {
        var gate = new FakeGate(static _ => throw new InvalidOperationException("gate must not be consulted when a probe exists"));
        var probe = new FakeGrantProbe(grant: true);
        var filter = new LatticeStateVisibilityFilter(
            gate,
            membership: null,
            LatticeStateApiReadVisibility.Auto,
            probe);

        var can = await filter.CanReadAnyKeyAsync("tree", Named, CancellationToken.None);

        Assert.That(can, Is.True);
        Assert.That(probe.CallCount, Is.EqualTo(1));
        Assert.That(probe.LastOperation, Is.EqualTo(LatticeOperation.RangeRead));
        Assert.That(gate.CallCount, Is.Zero, "the probe takes precedence over the plain per-tree decision");
    }

    [Test]
    public async Task CanReadAnyKeyAsync_probe_verdict_is_honoured_when_it_denies()
    {
        var probe = new FakeGrantProbe(grant: false);
        var filter = new LatticeStateVisibilityFilter(
            AllowGate(),
            membership: null,
            LatticeStateApiReadVisibility.Auto,
            probe);

        var can = await filter.CanReadAnyKeyAsync("tree", Named, CancellationToken.None);

        Assert.That(can, Is.False, "a registered probe's negative verdict must not be overridden by the per-tree gate");
    }

    [Test]
    public async Task CanReadAnyKeyAsync_short_circuits_anonymous_without_probe_or_gate()
    {
        var gate = new FakeGate(static _ => throw new InvalidOperationException("gate must not be consulted"));
        var probe = new FakeGrantProbe(grant: true);
        var filter = new LatticeStateVisibilityFilter(
            gate,
            membership: null,
            LatticeStateApiReadVisibility.Auto,
            probe);

        var can = await filter.CanReadAnyKeyAsync("tree", LatticeSubject.Anonymous, CancellationToken.None);

        Assert.That(can, Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(probe.CallCount, Is.Zero, "an anonymous subject is refused before the probe");
            Assert.That(gate.CallCount, Is.Zero, "an anonymous subject is refused before the gate");
        });
    }
}
