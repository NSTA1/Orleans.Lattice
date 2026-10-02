namespace Orleans.Lattice.Tests;

/// <summary>
/// Coverage for <see cref="LatticeAccessGateEnforcement.EnforcePrefixAsync"/>: a prefix-scoped
/// all-or-nothing operation is authorized as a range over the whole prefix with
/// hard-deny semantics (issue #4278).
/// </summary>
public partial class LatticeAccessGateEnforcementTests
{
    // ---- EnforcePrefixAsync ----------------------------------------------

    [Test]
    public async Task EnforcePrefixAsync_uniformAllow_issuesARangeRequestOverThePrefix()
    {
        LatticeAccessRequest captured = default;
        var gate = new FakeGate(r =>
        {
            captured = r;
            return LatticeAccessDecision.Allow();
        });

        await LatticeAccessGateEnforcement.EnforcePrefixAsync(
            gate, membership: null, Tree, LatticeOperation.Backup, "tenant-a/", default);

        Assert.Multiple(() =>
        {
            Assert.That(gate.CallCount, Is.EqualTo(1));
            Assert.That(captured.Operation, Is.EqualTo(LatticeOperation.Backup));
            Assert.That(captured.Key, Is.Null, "a prefix is a range, never a point at its root");
            Assert.That(captured.RangeStart, Is.EqualTo("tenant-a/"));
            Assert.That(captured.RangeEnd, Is.EqualTo("tenant-a0"));
        });
    }

    [Test]
    public void EnforcePrefixAsync_deny_throws()
    {
        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await LatticeAccessGateEnforcement.EnforcePrefixAsync(
                Denying(), membership: null, Tree, LatticeOperation.Restore, "p", default));

        Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.Restore));
    }

    [Test]
    public void EnforcePrefixAsync_filteredAllow_throwsHardDeny()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforcePrefixAsync(
                Filtering(_ => true), membership: null, Tree, LatticeOperation.Backup, "p", default),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "a prefix drain runs system-origin and cannot be narrowed key-by-key");
    }

    [Test]
    public async Task EnforcePrefixAsync_nullGateOrSystemOrigin_doesNotConsult()
    {
        await LatticeAccessGateEnforcement.EnforcePrefixAsync(
            new NullLatticeAccessGate(), membership: null, Tree, LatticeOperation.Backup, "p", default);

        var gate = Denying();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await LatticeAccessGateEnforcement.EnforcePrefixAsync(
                gate, membership: null, Tree, LatticeOperation.Backup, "p", default);
        }

        Assert.That(gate.CallCount, Is.Zero);
    }

    [Test]
    public void EnforcePrefixAsync_nullPrefix_throws()
    {
        Assert.That(
            async () => await LatticeAccessGateEnforcement.EnforcePrefixAsync(
                Allowing(), membership: null, Tree, LatticeOperation.Backup, null!, default),
            Throws.ArgumentNullException);
    }
}
