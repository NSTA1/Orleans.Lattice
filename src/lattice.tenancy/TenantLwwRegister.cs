namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A last-writer-wins register over a single value of type <typeparamref name="T"/>,
/// built on the public <see cref="HybridLogicalClock"/> primitive. Each write
/// stamps the value with a clock and a writer id; <see cref="Merge"/> keeps the
/// value with the higher stamp in the total order defined by
/// <see cref="TenantClock"/>. The merge is commutative, associative, and
/// idempotent, so concurrent updates from any number of writers converge to the
/// same value regardless of the order they are applied.
/// </summary>
/// <typeparam name="T">The registered value type.</typeparam>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantLwwRegister)]
[Immutable]
public readonly record struct TenantLwwRegister<T>
{
    /// <summary>The currently-held value.</summary>
    [Id(0)]
    public T Value { get; init; }

    /// <summary>The clock the current <see cref="Value"/> was written at.</summary>
    [Id(1)]
    public HybridLogicalClock Clock { get; init; }

    /// <summary>
    /// The id of the writer that wrote the current <see cref="Value"/>, used to
    /// break clock ties deterministically. May be <c>null</c>.
    /// </summary>
    [Id(2)]
    public string? WriterId { get; init; }

    /// <summary>
    /// Creates a register holding <paramref name="value"/> stamped with
    /// <paramref name="clock"/> and <paramref name="writerId"/>.
    /// </summary>
    /// <param name="value">The value to hold.</param>
    /// <param name="clock">The clock to stamp the value with.</param>
    /// <param name="writerId">The writer id to stamp the value with (may be <c>null</c>).</param>
    /// <returns>The constructed register.</returns>
    public static TenantLwwRegister<T> Create(T value, HybridLogicalClock clock, string? writerId) =>
        new() { Value = value, Clock = clock, WriterId = writerId };

    /// <summary>
    /// Returns a register holding <paramref name="value"/> stamped with
    /// <paramref name="clock"/> and <paramref name="writerId"/> when that stamp
    /// supersedes this register's stamp; otherwise returns this register
    /// unchanged. Writing an older or equal stamp is a no-op.
    /// </summary>
    /// <param name="value">The candidate value.</param>
    /// <param name="clock">The candidate stamp's clock.</param>
    /// <param name="writerId">The candidate stamp's writer id (may be <c>null</c>).</param>
    /// <returns>The updated register, or this register when the write does not win.</returns>
    public TenantLwwRegister<T> Set(T value, HybridLogicalClock clock, string? writerId) =>
        TenantClock.Supersedes(clock, writerId, Clock, WriterId)
            ? new TenantLwwRegister<T> { Value = value, Clock = clock, WriterId = writerId }
            : this;

    /// <summary>
    /// Merges two registers, returning the one whose stamp wins the
    /// <see cref="TenantClock"/> total order. Commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <param name="left">One register.</param>
    /// <param name="right">The other register.</param>
    /// <returns>The register with the winning stamp.</returns>
    public static TenantLwwRegister<T> Merge(TenantLwwRegister<T> left, TenantLwwRegister<T> right) =>
        TenantClock.Supersedes(right.Clock, right.WriterId, left.Clock, left.WriterId) ? right : left;
}
