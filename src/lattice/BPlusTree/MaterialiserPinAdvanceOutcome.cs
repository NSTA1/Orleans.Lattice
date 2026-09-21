namespace Orleans.Lattice.BPlusTree;

using Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// What one merged leaf-materialiser pin report actually moved: the complete
/// two-by-two truth table of the two axes a pin merge advances independently -
/// the HLC frontier and the durable checkpoint offset.
/// <para>
/// <b>The arms are a partition, and that is the whole point of this type.</b>
/// The three tag values this enum replaces were assigned by a first-match
/// ternary (<c>offsetAdvanced ? offset : frontierAdvanced ? frontier_only :
/// none</c>), so the <c>offset</c> arm absorbed every merge that advanced the
/// offset regardless of what the frontier did. A reader could therefore never
/// ask "did the frontier advance?" of a tree whose offset was advancing, and -
/// worse - an absent <c>frontier_only</c> series did <i>not</i> mean the
/// frontier was flat. It meant only that the frontier never advanced
/// <em>alone</em>, which is a different and much weaker claim than the
/// instrument's own description implied (issue #3163).
/// </para>
/// <para>
/// Because the four members below enumerate both axes rather than short-circuit
/// on one, each marginal is still recoverable by summing - offset advanced is
/// <see cref="OffsetOnly"/> + <see cref="Both"/>, frontier advanced is
/// <see cref="FrontierOnly"/> + <see cref="Both"/> - while the joint
/// distribution the first-match form destroyed is now readable directly. That
/// asymmetry is why a partition was chosen over two independent per-axis
/// counters, which would have exported the marginals and nothing else.
/// </para>
/// <para>
/// Diagnostic only. Classification happens after the merge has already taken
/// its monotonic maxima and never feeds back into it, so a pin advances exactly
/// as far as it would have before this type existed, and the WAL GC trim floor
/// is untouched.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(WalMaterialiserPinGrain),
    "orleans.lattice.materialiser.pin.advances",
    LatticeMetrics.TagOutcome)]
internal enum MaterialiserPinAdvanceOutcome
{
    /// <summary>
    /// Neither axis moved: a report at or behind the stored pin on both the HLC
    /// frontier and the checkpoint offset, fully coalesced away. Still counted,
    /// because a report that achieved nothing is an observation about the
    /// consumer rather than an absence of one.
    /// </summary>
    None = 0,

    /// <summary>
    /// The HLC frontier advanced and the checkpoint offset did not, so the pin
    /// was rewritten at the same offset. Retention is unchanged: the WAL GC
    /// offset floor is a minimum over durable checkpoint offsets and this merge
    /// moved none of them.
    /// </summary>
    FrontierOnly = 1,

    /// <summary>
    /// The checkpoint offset advanced and the HLC frontier did not.
    /// <para>
    /// This is the arm that was previously unobservable, and it is the one worth
    /// reading against issue #3094. The offset floor can only <b>lower</b> a
    /// trim point that the HLC clauses have already authorised - it never
    /// authorises one itself - so a consumer that advances only here makes real
    /// progress that can never release a single WAL entry. Before the split this
    /// arm and <see cref="Both"/> were reported under one indistinguishable
    /// <c>offset</c> tag, so that shape was invisible.
    /// </para>
    /// </summary>
    OffsetOnly = 2,

    /// <summary>
    /// Both axes advanced in the same merge - the healthy shape for a consumer
    /// that is draining. Separated from <see cref="OffsetOnly"/> because the two
    /// were the masked pair: collapsing them is what made "the offset is moving"
    /// unable to answer "and is the frontier moving with it?".
    /// </summary>
    Both = 3,
}
