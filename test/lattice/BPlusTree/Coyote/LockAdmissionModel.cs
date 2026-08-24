using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// A Coyote concurrency model of the distributed lock's fencing safety, driving
/// the <b>production</b> <see cref="LockAdmissionCore"/> under systematic schedule
/// exploration. It reproduces the classic Kleppmann fencing race: holder <c>A</c>
/// is granted the lock, its lease expires (a GC pause or activation move that
/// outlived the lease), and then three events race in an order the runtime
/// explores via <see cref="ICoyoteRuntime.RandomBoolean()"/>:
/// <list type="bullet">
///   <item><description>
///     the lock reclaims <c>A</c>'s expired lease and grants the next waiter
///     <c>B</c> a strictly-greater fencing token;
///   </description></item>
///   <item><description><c>A</c> wakes and issues a stale <c>Release</c> with its old token;</description></item>
///   <item><description><c>A</c> wakes and issues a stale <c>Renew</c> with its old token.</description></item>
/// </list>
/// <para>
/// The model asserts two safety properties after every delivered event, for every
/// explored order:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <b>FencingMonotonic:</b> <c>B</c>'s fencing token is strictly greater than
///     <c>A</c>'s whenever <c>B</c> is granted.
///   </description></item>
///   <item><description>
///     <b>StaleTokenRejection / MutualExclusion:</b> once <c>B</c> holds the lock,
///     no stale-token operation from <c>A</c> may dislodge <c>B</c> - the current
///     holder stays <c>B</c>.
///   </description></item>
/// </list>
/// The <paramref name="useBrokenTokenCheck"/> toggle chooses the release / renew
/// rule:
/// <list type="bullet">
///   <item><description>
///     <c>false</c> - the proven core. <see cref="LockAdmissionCore.Release"/> and
///     <see cref="LockAdmissionCore.Renew"/> honour an operation only for the
///     current holder's token, so a stale op is a no-op and the properties hold on
///     every schedule.
///   </description></item>
///   <item><description>
///     <c>true</c> - the regression: a release that frees the lock without checking
///     the presented token matches the current holder. Coyote explores an order
///     that reclaims-and-grants <c>B</c> and <i>then</i> delivers <c>A</c>'s stale
///     release, which frees <c>B</c>'s lock and trips the assertion.
///   </description></item>
/// </list>
/// </summary>
internal sealed class LockAdmissionModel(bool useBrokenTokenCheck) : ICoyoteModel
{
    public void Run(ICoyoteRuntime runtime)
    {
        var state = new LockCoreState();
        const long lease = 10;

        // Setup: grant A at t = 0.
        var tA = LockAdmissionCore.Grant(ref state, nowTicks: 0, leaseTicks: lease);

        // Time is now well past A's lease: A is a presumed-dead holder.
        const long now = 100;

        // The explored events racing against A's expiry:
        //   0 = reclaim A's expired lease and grant the next waiter B
        //   1 = A wakes and issues a stale Release(tA)
        //   2 = A wakes and issues a stale Renew(tA)
        const int eventCount = 3;
        var delivered = new bool[eventCount];

        var bGranted = false;
        long tB = 0;

        for (var step = 0; step < eventCount; step++)
        {
            var pick = SelectNextUndelivered(runtime, delivered);
            delivered[pick] = true;

            switch (pick)
            {
                case 0:
                    // Reclaim an expired lease, then grant the next waiter only when
                    // the admission gate says the lock is free - exactly what the
                    // production grain does before handing a waiter the lock.
                    LockAdmissionCore.ReclaimIfExpired(ref state, now);
                    if (LockAdmissionCore.Decide(state, now) == LockAdmissionDecision.Grant)
                    {
                        tB = LockAdmissionCore.Grant(ref state, now, lease);
                        bGranted = true;
                        Specification.Assert(
                            tB > tA,
                            $"fencing monotonicity violated: tB={tB} not strictly greater than tA={tA}");
                    }

                    break;

                case 1:
                    if (useBrokenTokenCheck)
                    {
                        BrokenRelease(ref state);
                    }
                    else
                    {
                        LockAdmissionCore.Release(ref state, tA);
                    }

                    break;

                case 2:
                    if (useBrokenTokenCheck)
                    {
                        BrokenRenew(ref state, now, lease);
                    }
                    else
                    {
                        LockAdmissionCore.Renew(ref state, tA, now, lease);
                    }

                    break;
            }

            // Once B has been granted, no stale-token A-op may dislodge B.
            if (bGranted)
            {
                Specification.Assert(
                    state.IsHeld && state.HolderToken == tB,
                    "a stale-token operation dislodged the current fenced holder B");
            }
        }
    }

    /// <summary>
    /// Picks the next undelivered event, driving the choice through the runtime's
    /// controlled nondeterminism so the harness explores distinct delivery orders.
    /// Always returns a valid undelivered index.
    /// </summary>
    private static int SelectNextUndelivered(ICoyoteRuntime runtime, bool[] delivered)
    {
        var firstUndelivered = -1;
        for (var i = 0; i < delivered.Length; i++)
        {
            if (delivered[i])
            {
                continue;
            }

            if (firstUndelivered < 0)
            {
                firstUndelivered = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return firstUndelivered;
    }

    /// <summary>
    /// The BROKEN release rule (guard fixture only): frees the lock without
    /// checking the presented token is the current holder's, so a stale holder can
    /// dislodge the current fenced holder. Coyote's schedule exploration surfaces
    /// the order in which this trips the safety assertion.
    /// </summary>
    private static void BrokenRelease(ref LockCoreState state)
    {
        state.IsHeld = false;
        state.HolderToken = LockAdmissionCore.NoToken;
        state.LeaseExpiresAtTicks = 0;
    }

    /// <summary>
    /// The BROKEN renew rule (guard fixture only): extends whichever holder is
    /// current without checking the presented token. Included for parity with the
    /// broken release so the guard fixture exercises both stale operations.
    /// </summary>
    private static void BrokenRenew(ref LockCoreState state, long now, long lease)
    {
        if (state.IsHeld)
        {
            state.LeaseExpiresAtTicks = now + lease;
        }
    }
}
