namespace Orleans.Lattice.Tests;

/// <summary>
/// Add-only unit coverage for the fail-closed <see cref="LatticeAccessGateEnforcement"/>
/// primitive. Each enforcement method is driven with a hand-written
/// <see cref="ILatticeAccessGate"/> fake (NSubstitute cannot mock the <c>in</c>
/// parameter) so allow, deny, and filtered-allow decisions are exercised
/// deterministically in-process with a null membership context (which resolves to
/// <see cref="LatticeSubject.Anonymous"/>). The zero-cost short-circuits (null gate
/// and system-origin scope) are covered too.
/// </summary>
[TestFixture]
public class LatticeAccessGateEnforcementTests
{
    private const string Tree = "tree-1";

    private sealed class FakeGate(Func<LatticeAccessRequest, LatticeAccessDecision> decide) : ILatticeAccessGate
    {
        public int CallCount { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var copy = request;
            CallCount++;
            return new ValueTask<LatticeAccessDecision>(decide(copy));
        }
    }

    private static FakeGate Allowing() => new(_ => LatticeAccessDecision.Allow());

    private static FakeGate Denying() => new(_ => LatticeAccessDecision.Deny("nope"));

    private static FakeGate Filtering(Func<string, bool> filter) =>
        new(_ => LatticeAccessDecision.Filtered(filter));

    // ---- SkipsEnforcement ------------------------------------------------

    [Test]
    public void SkipsEnforcement_nullGate_returnsTrue()
    {
        Assert.That(LatticeAccessGateEnforcement.SkipsEnforcement(new NullLatticeAccessGate()), Is.True);
    }

    [Test]
    public void SkipsEnforcement_realGate_returnsFalse()
    {
        Assert.That(LatticeAccessGateEnforcement.SkipsEnforcement(Allowing()), Is.False);
    }

    [Test]
    public void SkipsEnforcement_systemOriginScope_returnsTrue()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(LatticeAccessGateEnforcement.SkipsEnforcement(Allowing()), Is.True);
        }
    }

    // ---- EnforcePointAsync ----------------------------------------------

    [Test]
    public async Task EnforcePointAsync_nullGate_doesNotThrowOrConsult()
    {
        await LatticeAccessGateEnforcement.EnforcePointAsync(
            new NullLatticeAccessGate(), membership: null, Tree, LatticeOperation.Write, "k", default);
        Assert.Pass();
    }

    [Test]
    public async Task EnforcePointAsync_systemOrigin_skipsRealGate()
    {
        var gate = Denying();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await LatticeAccessGateEnforcement.EnforcePointAsync(
                gate, membership: null, Tree, LatticeOperation.Write, "k", default);
        }

        Assert.That(gate.CallCount, Is.Zero);
    }

    [Test]
    public async Task EnforcePointAsync_allow_doesNotThrow()
    {
        var gate = Allowing();
        await LatticeAccessGateEnforcement.EnforcePointAsync(
            gate, membership: null, Tree, LatticeOperation.Write, "k", default);
        Assert.That(gate.CallCount, Is.EqualTo(1));
    }

    [Test]
    public void EnforcePointAsync_deny_throwsAuthorizationDenied()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforcePointAsync(
                Denying(), membership: null, Tree, LatticeOperation.Write, "k", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void EnforcePointAsync_filteredExcludesKey_throws()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforcePointAsync(
                Filtering(k => k == "other"), membership: null, Tree, LatticeOperation.Write, "k", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task EnforcePointAsync_filteredIncludesKey_doesNotThrow()
    {
        await LatticeAccessGateEnforcement.EnforcePointAsync(
            Filtering(k => k == "k"), membership: null, Tree, LatticeOperation.Write, "k", default);
        Assert.Pass();
    }

    // ---- EnforceManyPointsAsync -----------------------------------------

    [Test]
    public async Task EnforceManyPointsAsync_emptyKeys_doesNotConsultGate()
    {
        var gate = Denying();
        await LatticeAccessGateEnforcement.EnforceManyPointsAsync(
            gate, membership: null, Tree, LatticeOperation.Write, Array.Empty<string>(), default);
        Assert.That(gate.CallCount, Is.Zero);
    }

    [Test]
    public async Task EnforceManyPointsAsync_allAllowed_doesNotThrow()
    {
        var gate = Allowing();
        await LatticeAccessGateEnforcement.EnforceManyPointsAsync(
            gate, membership: null, Tree, LatticeOperation.Write, new[] { "a", "b", "c" }, default);
        Assert.That(gate.CallCount, Is.EqualTo(3));
    }

    [Test]
    public void EnforceManyPointsAsync_denySecond_throwsAndStops()
    {
        var gate = new FakeGate(r => r.Key == "b" ? LatticeAccessDecision.Deny("no b") : LatticeAccessDecision.Allow());

        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceManyPointsAsync(
                gate, membership: null, Tree, LatticeOperation.Write, new[] { "a", "b", "c" }, default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(gate.CallCount, Is.EqualTo(2));
    }

    // ---- EnforceRangeDeleteAsync ----------------------------------------

    [Test]
    public async Task EnforceRangeDeleteAsync_uniformAllow_doesNotThrow()
    {
        await LatticeAccessGateEnforcement.EnforceRangeDeleteAsync(
            Allowing(), membership: null, Tree, "a", "z", default);
        Assert.Pass();
    }

    [Test]
    public void EnforceRangeDeleteAsync_deny_throws()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceRangeDeleteAsync(
                Denying(), membership: null, Tree, "a", "z", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void EnforceRangeDeleteAsync_filteredAllow_throwsHardDeny()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceRangeDeleteAsync(
                Filtering(_ => true), membership: null, Tree, "a", "z", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- EnforceWholeTreeAsync ------------------------------------------

    [Test]
    public async Task EnforceWholeTreeAsync_allow_doesNotThrow()
    {
        await LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            Allowing(), membership: null, Tree, LatticeOperation.Admin, default);
        Assert.Pass();
    }

    [Test]
    public void EnforceWholeTreeAsync_deny_throws()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
                Denying(), membership: null, Tree, LatticeOperation.Admin, default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void EnforceWholeTreeAsync_filteredAllow_throwsHardDeny()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
                Filtering(_ => true), membership: null, Tree, LatticeOperation.BulkLoad, default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- EnforceUniformRangeReadAsync -----------------------------------

    [Test]
    public async Task EnforceUniformRangeReadAsync_allow_doesNotThrow()
    {
        await LatticeAccessGateEnforcement.EnforceUniformRangeReadAsync(
            Allowing(), membership: null, Tree, "a", "z", default);
        Assert.Pass();
    }

    [Test]
    public void EnforceUniformRangeReadAsync_deny_throws()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceUniformRangeReadAsync(
                Denying(), membership: null, Tree, "a", "z", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void EnforceUniformRangeReadAsync_filteredAllow_throwsHardDeny()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforceUniformRangeReadAsync(
                Filtering(_ => true), membership: null, Tree, "a", "z", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    // ---- ResolveRangeReadFilterAsync ------------------------------------

    [Test]
    public async Task ResolveRangeReadFilterAsync_nullGate_returnsNull()
    {
        var filter = await LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync(
            new NullLatticeAccessGate(), membership: null, Tree, "a", "z", default);
        Assert.That(filter, Is.Null);
    }

    [Test]
    public async Task ResolveRangeReadFilterAsync_deny_returnsRejectAll()
    {
        var filter = await LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync(
            Denying(), membership: null, Tree, "a", "z", default);
        Assert.That(filter, Is.Not.Null);
        Assert.That(filter!("anything"), Is.False);
    }

    [Test]
    public async Task ResolveRangeReadFilterAsync_plainAllow_returnsNull()
    {
        var filter = await LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync(
            Allowing(), membership: null, Tree, "a", "z", default);
        Assert.That(filter, Is.Null);
    }

    [Test]
    public async Task ResolveRangeReadFilterAsync_filteredAllow_returnsFilter()
    {
        var filter = await LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync(
            Filtering(k => k.StartsWith('a')), membership: null, Tree, "a", "z", default);
        Assert.That(filter, Is.Not.Null);
        Assert.That(filter!("apple"), Is.True);
        Assert.That(filter("banana"), Is.False);
    }

    [Test]
    public async Task ResolveRangeReadFilterAsync_systemOrigin_returnsNull()
    {
        Func<string, bool>? filter;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            filter = await LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync(
                Denying(), membership: null, Tree, "a", "z", default);
        }

        Assert.That(filter, Is.Null);
    }
}
