namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The single source of truth for the cross-tenant grant lifecycle: which
/// <see cref="TenantGrantState"/> transitions are legal, which state authorizes
/// anything, and how two concurrently-written states converge. Both the
/// tenant-admin grant operations (offer / approve / reject / revoke) and the
/// CRDT merge in <see cref="TenantGrantSlot.Merge"/> consult these rules, so
/// they live in exactly one place - the same shape as the sibling
/// <see cref="TenantRegionLifecycle"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The transition set.</b> A grant is offered into
/// <see cref="TenantGrantState.Pending"/> by the granting tenant, and only the
/// grantee tenant moves it on: <see cref="TenantGrantState.Pending"/> -&gt;
/// <see cref="TenantGrantState.Active"/> (approve) or
/// <see cref="TenantGrantState.Pending"/> -&gt;
/// <see cref="TenantGrantState.Rejected"/> (reject). Either party may then take
/// <see cref="TenantGrantState.Active"/> -&gt;
/// <see cref="TenantGrantState.Revoked"/> (revoke).
/// <see cref="TenantGrantState.Rejected"/> and
/// <see cref="TenantGrantState.Revoked"/> are terminal: a further agreement for
/// the same grantee and scope needs a fresh offer, which starts a new slot
/// generation rather than reviving the closed one.
/// </para>
/// <para>
/// <b>The merge join never widens access.</b> The stamps on a
/// <see cref="TenantGrantSlot"/> are a total order, so a plain last-writer-wins
/// merge could let a stale approve from one replica beat a revoke written on
/// another whose clock happened to trail - silently reinstating access after a
/// party walked away. <see cref="Join"/> closes that by ordering the states by
/// restrictiveness and keeping the most restrictive of the two, so a terminal
/// state can never be lost to a concurrent non-terminal one and convergence can
/// only ever narrow what a grant authorizes.
/// </para>
/// </remarks>
public static class TenantGrantLifecycle
{
    /// <summary>
    /// Returns <c>true</c> when <paramref name="state"/> is the one state that
    /// authorizes anything (<see cref="TenantGrantState.Active"/>). Every other
    /// state - offered but unapproved, declined, or withdrawn - resolves to a
    /// denial.
    /// </summary>
    /// <param name="state">The grant state to classify.</param>
    /// <returns><c>true</c> when the grant is in force.</returns>
    public static bool Authorizes(TenantGrantState state) => state == TenantGrantState.Active;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="state"/> is terminal: the
    /// agreement is closed and no transition leads out of it. A new agreement for
    /// the same grantee and scope requires a fresh offer.
    /// </summary>
    /// <param name="state">The grant state to classify.</param>
    /// <returns><c>true</c> when the state is <see cref="TenantGrantState.Rejected"/> or <see cref="TenantGrantState.Revoked"/>.</returns>
    public static bool IsTerminal(TenantGrantState state) =>
        state is TenantGrantState.Rejected or TenantGrantState.Revoked;

    /// <summary>
    /// Returns <c>true</c> when moving a grant from <paramref name="from"/> to
    /// <paramref name="to"/> is one of the three legal lifecycle transitions:
    /// approve (<see cref="TenantGrantState.Pending"/> -&gt;
    /// <see cref="TenantGrantState.Active"/>), reject
    /// (<see cref="TenantGrantState.Pending"/> -&gt;
    /// <see cref="TenantGrantState.Rejected"/>), and revoke
    /// (<see cref="TenantGrantState.Active"/> -&gt;
    /// <see cref="TenantGrantState.Revoked"/>). Every other pair - including the
    /// identity pair, which callers handle as an idempotent no-op rather than a
    /// write - is illegal.
    /// </summary>
    /// <param name="from">The grant's current state.</param>
    /// <param name="to">The candidate next state.</param>
    /// <returns><c>true</c> when the transition is legal.</returns>
    public static bool IsLegalTransition(TenantGrantState from, TenantGrantState to) =>
        (from, to) switch
        {
            (TenantGrantState.Pending, TenantGrantState.Active) => true,
            (TenantGrantState.Pending, TenantGrantState.Rejected) => true,
            (TenantGrantState.Active, TenantGrantState.Revoked) => true,
            _ => false,
        };

    /// <summary>
    /// Returns <c>true</c> when a fresh offer may be made over a grant currently
    /// in <paramref name="current"/>: there is no live agreement
    /// (<see cref="TenantGrantState.Rejected"/> or
    /// <see cref="TenantGrantState.Revoked"/>, which starts a new generation), or
    /// the offer is still unanswered (<see cref="TenantGrantState.Pending"/>,
    /// which amends it in place). Offering over an
    /// <see cref="TenantGrantState.Active"/> grant is refused: the grantee
    /// approved a specific operation set on a specific scope, and the granting
    /// tenant must not be able to redefine a live agreement without the grantee
    /// approving it again.
    /// </summary>
    /// <param name="current">The grant's current state.</param>
    /// <returns><c>true</c> when an offer is legal from the current state.</returns>
    public static bool IsLegalOffer(TenantGrantState current) =>
        current is TenantGrantState.Pending || IsTerminal(current);

    /// <summary>
    /// Joins two concurrently-written states for the same grant generation into
    /// the one they converge on, keeping the <b>more restrictive</b> of the two so
    /// convergence can never widen access. The join is commutative, associative,
    /// and idempotent - it is the maximum over a fixed restrictiveness order - so
    /// it is a valid CRDT merge regardless of the order replicas exchange states.
    /// </summary>
    /// <remarks>
    /// The order is <see cref="TenantGrantState.Pending"/> &lt;
    /// <see cref="TenantGrantState.Active"/> &lt;
    /// <see cref="TenantGrantState.Rejected"/> &lt;
    /// <see cref="TenantGrantState.Revoked"/>. Approval outranks a stale pending
    /// so an approve is never lost, but both terminal states outrank it, so a
    /// concurrent approve and revoke converges on
    /// <see cref="TenantGrantState.Revoked"/> - the terminal, access-denying
    /// outcome - rather than on whichever replica's clock happened to run ahead.
    /// The two terminal states are ordered against each other only so the join is
    /// deterministic; both deny.
    /// </remarks>
    /// <param name="left">One state.</param>
    /// <param name="right">The other state.</param>
    /// <returns>The state the two converge on.</returns>
    public static TenantGrantState Join(TenantGrantState left, TenantGrantState right)
    {
        var leftRank = Restrictiveness(left);
        var rightRank = Restrictiveness(right);
        if (leftRank != rightRank)
        {
            return rightRank > leftRank ? right : left;
        }

        // Equal rank means either the same state (the join is idempotent) or two
        // distinct values this build does not recognise, both ranked above every
        // known state. Ordering those by their numeric value keeps the join a
        // maximum over a total order, so it stays commutative and associative.
        return (int)right > (int)left ? right : left;
    }

    /// <summary>
    /// Ranks a state by how much it restricts access, ascending. Used only by
    /// <see cref="Join"/>; deliberately independent of the enum's own numeric
    /// values, which are fixed by the wire format and put
    /// <see cref="TenantGrantState.Active"/> at zero.
    /// </summary>
    private static int Restrictiveness(TenantGrantState state) => state switch
    {
        TenantGrantState.Pending => 0,
        TenantGrantState.Active => 1,
        TenantGrantState.Rejected => 2,
        TenantGrantState.Revoked => 3,

        // An unrecognised value can only arrive from a newer peer. Rank it above
        // every state this build knows so it can never lose the join to an
        // Active written here, which would be the access-widening direction.
        _ => int.MaxValue,
    };
}
