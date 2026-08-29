namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A single element of a tenant's LWW-element-map of cross-tenant grants: the
/// grant payload plus its presence bit, stamped with the clock and writer that
/// last set it, keyed in the map by <see cref="CrossTenantGrant.GrantId"/>. A
/// grant is live when its winning slot is <see cref="Present"/>.
/// <see cref="Merge"/> keeps the slot with the higher stamp in the
/// <see cref="TenantClock"/> total order, so a concurrent grant update and
/// revoke converge deterministically to a single payload and presence.
/// </summary>
/// <remarks>
/// <para>
/// <b>A merge returns one of its two inputs verbatim.</b> It never grafts one
/// slot's lifecycle state onto the other's terms, because that would publish a
/// (terms, state) pair no writer ever wrote - and the dangerous direction is
/// concrete: a still-<see cref="TenantGrantState.Pending"/> offer carrying
/// <em>widened</em> terms, published under a concurrent slot's
/// <see cref="TenantGrantState.Active"/> state, would hand the grantee an
/// authorization it never approved. So the winner is chosen and returned whole.
/// </para>
/// <para>
/// <b>The lifecycle state outranks the stamp.</b> The two parties to a grant write
/// from different replicas, and the stamp order says nothing about which intent
/// should survive: a stale approve whose clock happened to run ahead would
/// otherwise beat a revoke and silently reinstate access. The winner is therefore
/// the slot whose state <see cref="TenantGrantLifecycle.Join"/> keeps - the more
/// restrictive of the two - and only when both slots carry that same state does
/// the <see cref="TenantClock"/> stamp decide. Merging can consequently only ever
/// narrow what a grant authorizes.
/// </para>
/// <para>
/// <b>Generations let a closed agreement be re-opened.</b> Terminal states outrank
/// everything, so without a generation a revoked grant could never be offered
/// again for the same grantee and scope - they share a
/// <see cref="CrossTenantGrant.GrantId"/> and so a single slot. Every offer that
/// writes therefore starts a new <see cref="Generation"/>, and a generation
/// difference is resolved outright ahead of the state order, so a new agreement
/// supersedes a closed one while two writes within one generation still converge
/// restrictively.
/// </para>
/// <para>
/// The three comparisons are a single lexicographic order over
/// (<see cref="Generation"/>, state restrictiveness, stamp), so the merge is a
/// maximum over a total order and is commutative, associative, and idempotent by
/// construction.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantGrantSlot)]
[Immutable]
public readonly record struct TenantGrantSlot
{
    /// <summary>The grant payload this slot carries.</summary>
    [Id(0)]
    public CrossTenantGrant Grant { get; init; }

    /// <summary><c>true</c> when the grant is live (issued); <c>false</c> when revoked.</summary>
    [Id(1)]
    public bool Present { get; init; }

    /// <summary>The clock this slot was written at.</summary>
    [Id(2)]
    public HybridLogicalClock Clock { get; init; }

    /// <summary>The id of the writer that last wrote this slot (may be <c>null</c>).</summary>
    [Id(3)]
    public string? WriterId { get; init; }

    /// <summary>
    /// The agreement generation this slot belongs to. Starts at zero and is
    /// advanced only when a write re-opens or re-states an agreement, so a new
    /// agreement supersedes whatever the older generation concluded. A grant
    /// persisted before this field existed reads back as generation zero, which is
    /// the generation every first offer uses.
    /// </summary>
    [Id(4)]
    public long Generation { get; init; }

    /// <summary>
    /// Merges two slots for the same grant id, returning <b>one of them
    /// verbatim</b>. A slot from a later <see cref="Generation"/> wins outright,
    /// because it is a newer agreement that replaced whatever the older one
    /// concluded. Within one generation the winner is the slot carrying the state
    /// <see cref="TenantGrantLifecycle.Join"/> keeps - the more restrictive of the
    /// two - so a terminal state can never be lost to a concurrent non-terminal
    /// one; only when both slots carry that state does the
    /// <see cref="TenantClock"/> total order decide. Commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <param name="left">One slot.</param>
    /// <param name="right">The other slot.</param>
    /// <returns>Whichever of the two slots wins, unmodified.</returns>
    public static TenantGrantSlot Merge(TenantGrantSlot left, TenantGrantSlot right)
    {
        if (left.Generation != right.Generation)
        {
            return left.Generation > right.Generation ? left : right;
        }

        // A tombstone carries no lifecycle opinion - a blind remove of a grant a
        // replica has never seen has no payload to carry one - so presence is
        // ranked ahead of the state instead of letting a synthesized state decide.
        // Removal is the bluntest close of all, so it outranks every state; a
        // later write that re-establishes the grant advances the generation, which
        // is resolved above, so this is a precedence rule rather than a permanent
        // block.
        if (left.Present != right.Present)
        {
            return left.Present ? right : left;
        }

        if (left.Present)
        {
            var state = TenantGrantLifecycle.Join(left.Grant.State, right.Grant.State);
            var leftCarriesIt = left.Grant.State == state;
            var rightCarriesIt = right.Grant.State == state;

            if (leftCarriesIt != rightCarriesIt)
            {
                return leftCarriesIt ? left : right;
            }
        }

        return TenantClock.Supersedes(right.Clock, right.WriterId, left.Clock, left.WriterId)
            ? right
            : left;
    }
}
