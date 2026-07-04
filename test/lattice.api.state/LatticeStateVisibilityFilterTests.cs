namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeStateVisibilityFilter"/> - the helper that
/// decides whether auth-backed read visibility is active, resolves the caller
/// subject, and evaluates per-tree read visibility. Exercised directly against a
/// fake gate and a spy membership context so the decisions are asserted without
/// standing up a cluster, including the zero-cost guarantee that no subject is
/// resolved when the filter is disabled.
/// </summary>
[TestFixture]
public sealed class LatticeStateVisibilityFilterTests
{
    private static readonly LatticeSubject Named = new("alice");

    private sealed class SpyMembership(LatticeSubject subject) : ILatticeMembershipContext
    {
        public int ResolveCount { get; private set; }

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        {
            ResolveCount++;
            return new ValueTask<LatticeSubject>(subject);
        }
    }

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

    private static FakeGate AllowGate() => new(static _ => LatticeAccessDecision.Allow());

    private static FakeGate DenyGate() => new(static _ => LatticeAccessDecision.Deny("no"));

    [Test]
    public void Enabled_is_false_for_the_null_gate()
    {
        var filter = new LatticeStateVisibilityFilter(
            new NullLatticeAccessGate(),
            new SpyMembership(Named),
            LatticeStateApiReadVisibility.Auto);

        Assert.That(filter.Enabled, Is.False);
    }

    [Test]
    public void Enabled_is_false_when_disabled_even_with_a_real_gate()
    {
        var filter = new LatticeStateVisibilityFilter(
            AllowGate(),
            new SpyMembership(Named),
            LatticeStateApiReadVisibility.Disabled);

        Assert.That(filter.Enabled, Is.False);
    }

    [Test]
    public void Enabled_is_true_for_a_real_gate_and_auto()
    {
        var filter = new LatticeStateVisibilityFilter(
            AllowGate(),
            new SpyMembership(Named),
            LatticeStateApiReadVisibility.Auto);

        Assert.That(filter.Enabled, Is.True);
    }

    [Test]
    public void Enabled_is_true_for_a_real_gate_and_enforced()
    {
        var filter = new LatticeStateVisibilityFilter(
            AllowGate(),
            new SpyMembership(Named),
            LatticeStateApiReadVisibility.Enforced);

        Assert.That(filter.Enabled, Is.True);
    }

    [Test]
    public async Task ResolveSubjectAsync_returns_null_and_never_touches_membership_when_disabled()
    {
        var membership = new SpyMembership(Named);
        var filter = new LatticeStateVisibilityFilter(
            new NullLatticeAccessGate(),
            membership,
            LatticeStateApiReadVisibility.Auto);

        var subject = await filter.ResolveSubjectAsync(CancellationToken.None);

        Assert.That(subject, Is.Null);
        Assert.That(membership.ResolveCount, Is.Zero, "no subject resolution on the zero-cost path");
    }

    [Test]
    public async Task ResolveSubjectAsync_resolves_the_subject_when_enabled()
    {
        var membership = new SpyMembership(Named);
        var filter = new LatticeStateVisibilityFilter(
            AllowGate(),
            membership,
            LatticeStateApiReadVisibility.Auto);

        var subject = await filter.ResolveSubjectAsync(CancellationToken.None);

        Assert.That(subject, Is.Not.Null);
        Assert.That(subject!.Value.SubjectId, Is.EqualTo("alice"));
        Assert.That(membership.ResolveCount, Is.EqualTo(1));
    }

    [Test]
    public async Task CanReadTreeAsync_returns_true_when_the_gate_allows()
    {
        var gate = AllowGate();
        var filter = new LatticeStateVisibilityFilter(gate, new SpyMembership(Named), LatticeStateApiReadVisibility.Auto);

        var can = await filter.CanReadTreeAsync("tree", Named, CancellationToken.None);

        Assert.That(can, Is.True);
        Assert.That(gate.CallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task CanReadTreeAsync_returns_false_when_the_gate_denies()
    {
        var gate = DenyGate();
        var filter = new LatticeStateVisibilityFilter(gate, new SpyMembership(Named), LatticeStateApiReadVisibility.Auto);

        var can = await filter.CanReadTreeAsync("tree", Named, CancellationToken.None);

        Assert.That(can, Is.False);
    }

    [Test]
    public async Task CanReadTreeAsync_short_circuits_anonymous_without_calling_the_gate()
    {
        var gate = new FakeGate(static _ => throw new InvalidOperationException("gate must not be consulted"));
        var filter = new LatticeStateVisibilityFilter(gate, new SpyMembership(Named), LatticeStateApiReadVisibility.Auto);

        var can = await filter.CanReadTreeAsync("tree", LatticeSubject.Anonymous, CancellationToken.None);

        Assert.That(can, Is.False);
        Assert.That(gate.CallCount, Is.Zero);
    }

    [Test]
    public void CanReadTreeAsync_probes_with_a_range_read_operation()
    {
        LatticeOperation observed = default;
        var gate = new FakeGate(request =>
        {
            observed = request.Operation;
            return LatticeAccessDecision.Allow();
        });
        var filter = new LatticeStateVisibilityFilter(gate, new SpyMembership(Named), LatticeStateApiReadVisibility.Auto);

        _ = filter.CanReadTreeAsync("tree", Named, CancellationToken.None).AsTask().GetAwaiter().GetResult();

        Assert.That(observed, Is.EqualTo(LatticeOperation.RangeRead));
    }

    [Test]
    public void DeniesAllReads_is_true_for_anonymous_and_false_for_a_named_subject()
    {
        Assert.That(LatticeStateVisibilityFilter.DeniesAllReads(LatticeSubject.Anonymous), Is.True);
        Assert.That(LatticeStateVisibilityFilter.DeniesAllReads(Named), Is.False);
    }
}
