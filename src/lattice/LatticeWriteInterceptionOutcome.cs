namespace Orleans.Lattice;

/// <summary>
/// The internal result of applying an <see cref="ILatticeWriteInterceptor"/>
/// decision to a single write at the <c>LatticeGrain</c> choke point: whether
/// the write should proceed to the WAL, and the effective value bytes to persist
/// when it does.
/// </summary>
/// <remarks>
/// A <see cref="LatticeWriteDecisionKind.Reject"/> decision never produces an
/// outcome - it throws <see cref="LatticeWriteRejectedException"/> before this
/// value is constructed. A <see cref="LatticeWriteDecisionKind.DeadLetter"/>
/// decision yields <see cref="Proceed"/> = <c>false</c> so the choke point skips
/// the durable write. Accept and accept-transformed both yield
/// <see cref="Proceed"/> = <c>true</c>, carrying the original or replacement
/// bytes respectively.
/// </remarks>
internal readonly struct LatticeWriteInterceptionOutcome
{
    private LatticeWriteInterceptionOutcome(bool proceed, byte[] value)
    {
        Proceed = proceed;
        Value = value;
    }

    /// <summary>
    /// <c>true</c> when the write should be applied to the WAL; <c>false</c> when
    /// it was dead-lettered and must not become durable at the target key.
    /// </summary>
    public bool Proceed { get; }

    /// <summary>
    /// The effective value bytes to persist when <see cref="Proceed"/> is
    /// <c>true</c> (the original value for an accept, the replacement for a
    /// transform). Undefined when <see cref="Proceed"/> is <c>false</c>.
    /// </summary>
    public byte[] Value { get; }

    /// <summary>Creates a proceed outcome carrying the effective value bytes.</summary>
    /// <param name="value">The value to persist.</param>
    public static LatticeWriteInterceptionOutcome Write(byte[] value) => new(true, value);

    /// <summary>Creates a drop (dead-letter) outcome; the write is not made durable.</summary>
    public static LatticeWriteInterceptionOutcome Drop() => new(false, System.Array.Empty<byte>());
}
