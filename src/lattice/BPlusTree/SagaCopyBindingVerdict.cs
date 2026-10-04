namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What an atomic-write saga does with its binding to a physical copy
/// immediately before it records its commit decision. See
/// <see cref="SagaCopyBinding.BeforeDecision"/>.
/// </summary>
internal enum SagaCopyBindingVerdict
{
    /// <summary>The tree still resolves to the bound copy: record the decision.</summary>
    Commit,

    /// <summary>
    /// The tree moved off the bound copy, but the bound copy mirrors everything it
    /// takes into the copy the tree now resolves to: stay bound and record the
    /// decision, so the batch commits whole on both copies (#4369).
    /// </summary>
    StayBound,

    /// <summary>The tree moved and the bound copy does not mirror into it: re-bind and re-dispatch the batch.</summary>
    Rebind,
}
