namespace Orleans.Lattice;

/// <summary>
/// The decision an <see cref="ILatticeAccessGate"/> returns for a
/// <see cref="LatticeAccessRequest"/>: allow, deny (with a reason), or allow
/// but with a per-key <see cref="KeyFilter"/> the enforcement point applies to
/// prune keys the caller may not observe (for example on a range read).
/// </summary>
/// <remarks>
/// <para>
/// This is an in-process decision value. It is deliberately a plain
/// <c>readonly struct</c> (not a <c>record struct</c>) because it carries a
/// <see cref="KeyFilter"/> delegate, which is neither value-comparable nor
/// serializable; the type must never cross a grain boundary and carries no
/// Orleans serialization attributes.
/// </para>
/// <para>
/// The <see cref="Allow()"/> factory returns a cached singleton value so the
/// default no-op gate (<see cref="NullLatticeAccessGate"/>) produces a decision
/// without allocating.
/// </para>
/// </remarks>
public readonly struct LatticeAccessDecision
{
    private static readonly LatticeAccessDecision AllowDecision = new(true, reason: null, keyFilter: null);

    private LatticeAccessDecision(bool allowed, string? reason, Func<string, bool>? keyFilter)
    {
        Allowed = allowed;
        Reason = reason;
        KeyFilter = keyFilter;
    }

    /// <summary>
    /// <c>true</c> when the request is authorized (possibly subject to
    /// <see cref="KeyFilter"/>); <c>false</c> when it is denied.
    /// </summary>
    public bool Allowed { get; }

    /// <summary>
    /// A human-readable reason for the decision. Set for a denial (the cause);
    /// optionally set for a filtered allow; <c>null</c> for a plain allow.
    /// </summary>
    public string? Reason { get; }

    /// <summary>
    /// An optional per-key predicate the enforcement point applies to keep only
    /// the keys the caller may observe (returning <c>true</c> keeps a key).
    /// <c>null</c> when no per-key filtering is required. Only meaningful when
    /// <see cref="Allowed"/> is <c>true</c>.
    /// </summary>
    public Func<string, bool>? KeyFilter { get; }

    /// <summary>
    /// The cached "allow, no filter" decision. Allocation-free.
    /// </summary>
    /// <returns>A decision whose <see cref="Allowed"/> is <c>true</c>.</returns>
    public static LatticeAccessDecision Allow() => AllowDecision;

    /// <summary>
    /// Creates a denial decision carrying the supplied reason.
    /// </summary>
    /// <param name="reason">The reason the request is denied. Must not be <c>null</c> or empty.</param>
    /// <returns>A decision whose <see cref="Allowed"/> is <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <c>null</c> or empty.</exception>
    public static LatticeAccessDecision Deny(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        return new LatticeAccessDecision(false, reason, keyFilter: null);
    }

    /// <summary>
    /// Creates an allow decision that additionally prunes observable keys
    /// through the supplied <paramref name="predicate"/>.
    /// </summary>
    /// <param name="predicate">The per-key filter (returning <c>true</c> keeps a key). Must not be <c>null</c>.</param>
    /// <param name="reason">An optional human-readable note explaining the filter, or <c>null</c>.</param>
    /// <returns>A decision whose <see cref="Allowed"/> is <c>true</c> and whose <see cref="KeyFilter"/> is set.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <c>null</c>.</exception>
    public static LatticeAccessDecision Filtered(Func<string, bool> predicate, string? reason = null)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return new LatticeAccessDecision(true, reason, predicate);
    }
}
