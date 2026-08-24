using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="LockAdmissionCore"/> - the
/// shared correctness core the production distributed-lock grain
/// (<c>LatticeLockGrain</c>) and the Coyote lock model both execute to decide
/// fencing-token minting, admission, and lease reclamation. These pin the exact
/// truth table (fencing monotonicity, stale-token rejection, FIFO admission
/// gate, lease reclamation) so a change to the rule is caught here (and by the
/// Coyote model) rather than only by a slow integration run.
/// </summary>
[TestFixture]
public sealed class LockAdmissionCoreTests
{
    private const long Lease = 1000;

    // --- NextFencingToken ---

    [Test]
    public void NextFencingToken_returns_strict_successor()
    {
        Assert.That(LockAdmissionCore.NextFencingToken(0), Is.EqualTo(1));
        Assert.That(LockAdmissionCore.NextFencingToken(41), Is.EqualTo(42));
    }

    [Test]
    public void NextFencingToken_rejects_negative_last_issued()
    {
        Assert.That(() => LockAdmissionCore.NextFencingToken(-1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void NextFencingToken_overflows_at_long_max()
    {
        Assert.That(() => LockAdmissionCore.NextFencingToken(long.MaxValue),
            Throws.InstanceOf<OverflowException>());
    }

    // --- Grant ---

    [Test]
    public void Grant_mints_first_token_one_and_marks_held()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, nowTicks: 100, leaseTicks: Lease);

        Assert.That(token, Is.EqualTo(1));
        Assert.That(state.IsHeld, Is.True);
        Assert.That(state.HolderToken, Is.EqualTo(1));
        Assert.That(state.FencingCounter, Is.EqualTo(1));
        Assert.That(state.LeaseExpiresAtTicks, Is.EqualTo(100 + Lease));
    }

    [Test]
    public void Grant_mints_strictly_increasing_tokens_across_cycles()
    {
        var state = new LockCoreState();

        var t1 = LockAdmissionCore.Grant(ref state, 0, Lease);
        LockAdmissionCore.Release(ref state, t1);
        var t2 = LockAdmissionCore.Grant(ref state, 0, Lease);
        LockAdmissionCore.Release(ref state, t2);
        var t3 = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(t1, Is.LessThan(t2));
        Assert.That(t2, Is.LessThan(t3));
    }

    [Test]
    public void Grant_rejects_negative_lease()
    {
        var state = new LockCoreState();
        Assert.That(() => LockAdmissionCore.Grant(ref state, 0, -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // --- IsLeaseExpired ---

    [Test]
    public void IsLeaseExpired_is_false_when_free()
    {
        var state = new LockCoreState();
        Assert.That(LockAdmissionCore.IsLeaseExpired(state, nowTicks: long.MaxValue), Is.False);
    }

    [Test]
    public void IsLeaseExpired_is_true_at_or_after_expiry()
    {
        var state = new LockCoreState();
        LockAdmissionCore.Grant(ref state, nowTicks: 0, leaseTicks: Lease);

        Assert.That(LockAdmissionCore.IsLeaseExpired(state, Lease - 1), Is.False);
        Assert.That(LockAdmissionCore.IsLeaseExpired(state, Lease), Is.True);
        Assert.That(LockAdmissionCore.IsLeaseExpired(state, Lease + 1), Is.True);
    }

    // --- IsCurrentHolder ---

    [Test]
    public void IsCurrentHolder_is_false_when_free()
    {
        var state = new LockCoreState();
        Assert.That(LockAdmissionCore.IsCurrentHolder(state, LockAdmissionCore.NoToken), Is.False);
        Assert.That(LockAdmissionCore.IsCurrentHolder(state, 1), Is.False);
    }

    [Test]
    public void IsCurrentHolder_matches_only_the_current_token()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(LockAdmissionCore.IsCurrentHolder(state, token), Is.True);
        Assert.That(LockAdmissionCore.IsCurrentHolder(state, token + 1), Is.False);
        Assert.That(LockAdmissionCore.IsCurrentHolder(state, LockAdmissionCore.NoToken), Is.False);
    }

    // --- Decide ---

    [Test]
    public void Decide_grants_when_free()
    {
        var state = new LockCoreState();
        Assert.That(LockAdmissionCore.Decide(state, 0), Is.EqualTo(LockAdmissionDecision.Grant));
    }

    [Test]
    public void Decide_holds_while_lease_is_live()
    {
        var state = new LockCoreState();
        LockAdmissionCore.Grant(ref state, 0, Lease);
        Assert.That(LockAdmissionCore.Decide(state, Lease - 1), Is.EqualTo(LockAdmissionDecision.Hold));
    }

    [Test]
    public void Decide_grants_when_lease_expired()
    {
        var state = new LockCoreState();
        LockAdmissionCore.Grant(ref state, 0, Lease);
        Assert.That(LockAdmissionCore.Decide(state, Lease), Is.EqualTo(LockAdmissionDecision.Grant));
    }

    // --- Release ---

    [Test]
    public void Release_frees_the_current_holder()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(LockAdmissionCore.Release(ref state, token), Is.True);
        Assert.That(state.IsHeld, Is.False);
        Assert.That(state.HolderToken, Is.EqualTo(LockAdmissionCore.NoToken));
    }

    [Test]
    public void Release_is_a_no_op_for_a_stale_token()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(LockAdmissionCore.Release(ref state, token + 1), Is.False);
        Assert.That(state.IsHeld, Is.True);
        Assert.That(state.HolderToken, Is.EqualTo(token));
    }

    [Test]
    public void Release_is_idempotent()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(LockAdmissionCore.Release(ref state, token), Is.True);
        Assert.That(LockAdmissionCore.Release(ref state, token), Is.False);
    }

    // --- Renew ---

    [Test]
    public void Renew_extends_the_current_holder_lease()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(LockAdmissionCore.Renew(ref state, token, nowTicks: 500, leaseTicks: Lease), Is.True);
        Assert.That(state.LeaseExpiresAtTicks, Is.EqualTo(500 + Lease));
        Assert.That(state.HolderToken, Is.EqualTo(token));
    }

    [Test]
    public void Renew_rejects_a_stale_token()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);
        var expiry = state.LeaseExpiresAtTicks;

        Assert.That(LockAdmissionCore.Renew(ref state, token + 1, 500, Lease), Is.False);
        Assert.That(state.LeaseExpiresAtTicks, Is.EqualTo(expiry));
    }

    [Test]
    public void Renew_rejects_negative_lease()
    {
        var state = new LockCoreState();
        var token = LockAdmissionCore.Grant(ref state, 0, Lease);
        Assert.That(() => LockAdmissionCore.Renew(ref state, token, 0, -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // --- ReclaimIfExpired ---

    [Test]
    public void ReclaimIfExpired_is_a_no_op_when_free()
    {
        var state = new LockCoreState();
        Assert.That(LockAdmissionCore.ReclaimIfExpired(ref state, long.MaxValue), Is.False);
    }

    [Test]
    public void ReclaimIfExpired_is_a_no_op_while_lease_is_live()
    {
        var state = new LockCoreState();
        LockAdmissionCore.Grant(ref state, 0, Lease);
        Assert.That(LockAdmissionCore.ReclaimIfExpired(ref state, Lease - 1), Is.False);
        Assert.That(state.IsHeld, Is.True);
    }

    [Test]
    public void ReclaimIfExpired_frees_an_expired_lease_and_preserves_the_counter()
    {
        var state = new LockCoreState();
        var t1 = LockAdmissionCore.Grant(ref state, 0, Lease);

        Assert.That(LockAdmissionCore.ReclaimIfExpired(ref state, Lease), Is.True);
        Assert.That(state.IsHeld, Is.False);
        Assert.That(state.FencingCounter, Is.EqualTo(t1));

        // The next grant after reclamation still mints a strictly greater token.
        var t2 = LockAdmissionCore.Grant(ref state, Lease, Lease);
        Assert.That(t2, Is.GreaterThan(t1));
    }
}
