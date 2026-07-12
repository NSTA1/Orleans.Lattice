namespace Orleans.Lattice;

/// <summary>
/// The disposition an <see cref="ILatticeMergeObserver"/> returns for a
/// completed merge: <see cref="Accept"/> (leave the merged bytes unchanged),
/// <see cref="AcceptTransformed"/> (replace the canonical merged bytes -
/// LWW-only), or <see cref="AcceptWithEvent"/> (leave the bytes unchanged but
/// surface a non-mutating annotation).
/// </summary>
/// <remarks>
/// <para>
/// This is an in-process decision value (mirroring
/// <see cref="LatticeAccessDecision"/>): a plain <c>readonly struct</c> that
/// never crosses a grain boundary and carries no Orleans serialization
/// attributes. The <see cref="Accept()"/> factory returns a cached singleton so
/// the default no-op observer (<see cref="NullLatticeMergeObserver"/>) produces
/// an outcome without allocating.
/// </para>
/// </remarks>
public readonly struct LatticeMergeOutcome
{
    private static readonly LatticeMergeOutcome AcceptOutcome =
        new(MergeOutcomeKind.Accept, transformedValue: null, eventReason: null);

    private LatticeMergeOutcome(MergeOutcomeKind kind, byte[]? transformedValue, string? eventReason)
    {
        Kind = kind;
        TransformedValue = transformedValue;
        EventReason = eventReason;
    }

    /// <summary>The disposition this outcome represents.</summary>
    public MergeOutcomeKind Kind { get; }

    /// <summary>
    /// The replacement canonical merged bytes when <see cref="Kind"/> is
    /// <see cref="MergeOutcomeKind.AcceptTransformed"/>; otherwise <c>null</c>.
    /// </summary>
    public byte[]? TransformedValue { get; }

    /// <summary>
    /// The non-mutating annotation reason when <see cref="Kind"/> is
    /// <see cref="MergeOutcomeKind.AcceptWithEvent"/>; otherwise <c>null</c>.
    /// </summary>
    public string? EventReason { get; }

    /// <summary>
    /// The cached "accept, no change" outcome. Allocation-free.
    /// </summary>
    /// <returns>An outcome whose <see cref="Kind"/> is <see cref="MergeOutcomeKind.Accept"/>.</returns>
    public static LatticeMergeOutcome Accept() => AcceptOutcome;

    /// <summary>
    /// Creates an outcome that replaces the canonical merged bytes with
    /// <paramref name="mergedValue"/>. Permitted only for
    /// <see cref="LatticeMergeMode.LwwRegister"/> records; the merge-observer
    /// wiring throws <see cref="System.InvalidOperationException"/> if this is
    /// returned for any other mode.
    /// </summary>
    /// <param name="mergedValue">The replacement canonical merged bytes. Must not be <c>null</c>.</param>
    /// <returns>An outcome whose <see cref="Kind"/> is <see cref="MergeOutcomeKind.AcceptTransformed"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="mergedValue"/> is <c>null</c>.</exception>
    public static LatticeMergeOutcome AcceptTransformed(byte[] mergedValue)
    {
        ArgumentNullException.ThrowIfNull(mergedValue);
        return new LatticeMergeOutcome(MergeOutcomeKind.AcceptTransformed, mergedValue, eventReason: null);
    }

    /// <summary>
    /// Creates an outcome that keeps the merged bytes unchanged and surfaces a
    /// non-mutating annotation carrying <paramref name="reason"/>.
    /// </summary>
    /// <param name="reason">The annotation reason. Must not be <c>null</c> or empty.</param>
    /// <returns>An outcome whose <see cref="Kind"/> is <see cref="MergeOutcomeKind.AcceptWithEvent"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="reason"/> is <c>null</c> or empty.</exception>
    public static LatticeMergeOutcome AcceptWithEvent(string reason)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        return new LatticeMergeOutcome(MergeOutcomeKind.AcceptWithEvent, transformedValue: null, reason);
    }
}
