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

    // ---- EnforceWholeTreeControlAsync ------------------------------------

    [Test]
    public void EnforceWholeTreeControlAsync_nullGate_doesNotThrowOrConsult()
    {
        var enforce = LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
            new NullLatticeAccessGate(), membership: null, Tree, LatticeOperation.Admin, default);

        Assert.That(enforce.IsCompletedSuccessfully, Is.True,
            "the null gate short-circuits before the first await, so no subject is resolved");
    }

    [Test]
    public void EnforceWholeTreeControlAsync_systemOrigin_doesNotConsultTheGate()
    {
        var gate = Denying();

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var enforce = LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
                gate, membership: null, Tree, LatticeOperation.Admin, default);
            Assert.That(enforce.IsCompletedSuccessfully, Is.True);
        }

        Assert.That(gate.CallCount, Is.Zero,
            "an infrastructure turn is exempt and must never reach a denying gate");
    }

    [Test]
    public async Task EnforceWholeTreeControlAsync_unrestrictedAllow_proceeds()
    {
        var gate = Allowing();

        await LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
            gate, membership: null, Tree, LatticeOperation.Admin, default);

        Assert.That(gate.CallCount, Is.EqualTo(1));
    }

    [Test]
    public void EnforceWholeTreeControlAsync_deny_throws()
    {
        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
            await LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
                Denying(), membership: null, Tree, LatticeOperation.Admin, default));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.Admin));
        });
    }

    [Test]
    public void EnforceWholeTreeControlAsync_filteredAllow_throws()
    {
        // The motivating case for the method: there is no key to test the
        // filter against, and the authority being acquired is unbounded, so a
        // partial-coverage allow may never be widened into whole-tree control.
        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
            await LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
                Filtering(static _ => true), membership: null, Tree, LatticeOperation.Admin, default));

        Assert.That(ex!.TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public async Task EnforceWholeTreeControlAsync_requestCarriesNoKeyOrRange()
    {
        LatticeAccessRequest? seen = null;
        var gate = new FakeGate(r =>
        {
            seen = r;
            return LatticeAccessDecision.Allow();
        });

        await LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
            gate, membership: null, Tree, LatticeOperation.Admin, default);

        Assert.Multiple(() =>
        {
            Assert.That(seen!.Value.TreeId, Is.EqualTo(Tree));
            Assert.That(seen!.Value.Operation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(seen!.Value.Key, Is.Null, "the request covers the whole tree, not one key");
            Assert.That(seen!.Value.RangeStart, Is.Null);
            Assert.That(seen!.Value.RangeEnd, Is.Null);
        });
    }

    // ---- EnforcePointAsync ----------------------------------------------

    [Test]
    public void EnforcePointAsync_nullGate_doesNotThrowOrConsult()
    {
        // The "does not consult" half of the name is what makes the null-gate
        // path zero-cost, and it is only observable as synchronous completion:
        // the enforcement primitive short-circuits before its first await when
        // the gate is the null gate, so the ValueTask is already complete. A
        // regression that resolved a subject or called the gate would still not
        // throw here, but would no longer complete synchronously.
        var enforce = LatticeAccessGateEnforcement.EnforcePointAsync(
            new NullLatticeAccessGate(), membership: null, Tree, LatticeOperation.Write, "k", default);

        Assert.That(enforce.IsCompletedSuccessfully, Is.True,
            "a null gate must be short-circuited before the first await, so nothing is consulted");
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
        var gate = Filtering(k => k == "k");
        await LatticeAccessGateEnforcement.EnforcePointAsync(
            gate, membership: null, Tree, LatticeOperation.Write, "k", default);

        // Without this the test also passes when enforcement is skipped
        // entirely, which is exactly the fail-open regression it exists to
        // catch: the filter must have been consulted and admitted the key.
        Assert.That(gate.CallCount, Is.EqualTo(1));
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
        var gate = Allowing();
        await LatticeAccessGateEnforcement.EnforceRangeDeleteAsync(
            gate, membership: null, Tree, "a", "z", default);

        Assert.That(gate.CallCount, Is.EqualTo(1),
            "a uniform allow must still have been asked for - a skipped enforcement fails open");
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
        var gate = Allowing();
        await LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            gate, membership: null, Tree, LatticeOperation.Admin, default);

        Assert.That(gate.CallCount, Is.EqualTo(1),
            "a whole-tree allow must still have been asked for - a skipped enforcement fails open");
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
        var gate = Allowing();
        await LatticeAccessGateEnforcement.EnforceUniformRangeReadAsync(
            gate, membership: null, Tree, "a", "z", default);

        Assert.That(gate.CallCount, Is.EqualTo(1),
            "a uniform range-read allow must still have been asked for - a skipped enforcement fails open");
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
