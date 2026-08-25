using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by the public <see cref="ILattice"/> write guard when a
/// locally-authored write is refused because the target tree has reached a
/// configured per-tree admission-control cap - either
/// <see cref="LatticeOptions.MaxLiveKeys"/> (the <see cref="Dimension"/> is
/// <c>keys</c>) or <see cref="LatticeOptions.MaxEstimatedBytes"/> (the
/// <see cref="Dimension"/> is <c>bytes</c>). Admission control is strictly
/// opt-in: both caps default to <see langword="null"/> (unbounded), so a tree
/// that has not configured a cap never sees this exception.
/// <para>
/// <b>Caller contract.</b> This is a recoverable back-off signal, not a bug.
/// A caller observing it should either reduce the tree's live footprint (delete
/// keys, let TTLs expire and compaction reap tombstones) or, if the ceiling is
/// genuinely too low, raise the cap. Unlike a validation
/// <see cref="System.ArgumentException"/>, retrying the identical write only
/// succeeds once the tree drops back under its cap. The cap is evaluated
/// against a cached, eventually-consistent per-tree aggregate, so it is
/// best-effort / approximate: concurrent cross-shard writes can overshoot it
/// slightly before the aggregate refreshes, and enforcement fails open (no
/// rejection) until the tree's first sample lands after activation.
/// </para>
/// <para>
/// Replication and atomic-write-saga apply paths bypass admission control (they
/// re-enter the write surface under a foreign origin or prepared scope), so an
/// incoming replicated write is never refused with this exception.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing
/// catch handlers that match on <see cref="System.InvalidOperationException"/>
/// continue to absorb it, while callers that care about admission back-pressure
/// can catch the typed slot and read <see cref="TreeId"/>,
/// <see cref="Dimension"/>, <see cref="Current"/>, and <see cref="Limit"/>
/// without parsing the exception message.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeQuotaExceeded)]
public sealed class LatticeQuotaExceededException : InvalidOperationException
{
    /// <summary>The <see cref="Dimension"/> value for a live-key cap breach.</summary>
    public const string KeysDimension = "keys";

    /// <summary>The <see cref="Dimension"/> value for an estimated-byte cap breach.</summary>
    public const string BytesDimension = "bytes";

    /// <summary>
    /// Logical tree id whose admission cap was breached. Empty on the
    /// parameterless constructor; populated on the production overload.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// The quota dimension that was breached: <see cref="KeysDimension"/>
    /// (<see cref="LatticeOptions.MaxLiveKeys"/>) or <see cref="BytesDimension"/>
    /// (<see cref="LatticeOptions.MaxEstimatedBytes"/>). Empty on the
    /// parameterless constructor.
    /// </summary>
    [Id(1)]
    public string Dimension { get; }

    /// <summary>
    /// The tree's observed value on the breached <see cref="Dimension"/> (the
    /// cached live-key count or estimated-byte footprint) at the moment the
    /// write was refused. Zero on the parameterless constructor.
    /// </summary>
    [Id(2)]
    public long Current { get; }

    /// <summary>
    /// The configured cap on the breached <see cref="Dimension"/> that the
    /// write would have exceeded. Zero on the parameterless constructor.
    /// </summary>
    [Id(3)]
    public long Limit { get; }

    /// <summary>
    /// The id of the tenant whose aggregate quota was breached, when the refusal
    /// originates from the per-tenant usage-accounting layer; the empty string
    /// otherwise (a per-tree admission-cap breach carries no tenant). Populated by
    /// the tenancy add-on's admission controller.
    /// </summary>
    [Id(4)]
    public string TenantId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty / zero
    /// diagnostic context. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use the overload that
    /// carries the tree id, dimension, current value, and limit.
    /// </summary>
    public LatticeQuotaExceededException()
    {
        TreeId = string.Empty;
        Dimension = string.Empty;
        TenantId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty / zero diagnostic context.
    /// </summary>
    /// <param name="message">Diagnostic context describing which write was refused and why.</param>
    public LatticeQuotaExceededException(string message) : base(message)
    {
        TreeId = string.Empty;
        Dimension = string.Empty;
        TenantId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty / zero diagnostic context.
    /// </summary>
    /// <param name="message">Diagnostic context describing which write was refused and why.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeQuotaExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
        Dimension = string.Empty;
        TenantId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the full admission-control diagnostic
    /// context. The primary production throw shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing which write was refused and why.</param>
    /// <param name="treeId">Logical tree id whose admission cap was breached.</param>
    /// <param name="dimension">The breached dimension: <see cref="KeysDimension"/> or <see cref="BytesDimension"/>.</param>
    /// <param name="current">The tree's observed value on the breached dimension.</param>
    /// <param name="limit">The configured cap the write would have exceeded.</param>
    public LatticeQuotaExceededException(string message, string treeId, string dimension, long current, long limit)
        : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(dimension);
        TreeId = treeId;
        Dimension = dimension;
        Current = current;
        Limit = limit;
        TenantId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the full admission-control diagnostic
    /// context <em>and</em> the id of the tenant whose aggregate quota was
    /// breached. Used by the tenancy add-on's per-tenant usage-accounting layer so
    /// a caller can attribute the refusal to a tenant without parsing the message.
    /// </summary>
    /// <param name="message">Diagnostic context describing which write was refused and why.</param>
    /// <param name="treeId">Logical tree id the refused write targeted.</param>
    /// <param name="dimension">The breached dimension.</param>
    /// <param name="current">The tenant's observed usage on the breached dimension.</param>
    /// <param name="limit">The configured cap the write would have exceeded.</param>
    /// <param name="tenantId">The id of the tenant whose quota was breached.</param>
    public LatticeQuotaExceededException(string message, string treeId, string dimension, long current, long limit, string tenantId)
        : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(dimension);
        ArgumentNullException.ThrowIfNull(tenantId);
        TreeId = treeId;
        Dimension = dimension;
        Current = current;
        Limit = limit;
        TenantId = tenantId;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeQuotaExceededException"/>. Orleans deep-copies a grain result
/// across an in-process (co-located) boundary instead of serialising it, and the
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide - so a same-silo throw would fail with an opaque <c>KeyNotFoundException</c>
/// ("Could not find a base type copier for ...") and mask the real, actionable fault.
/// An exception is immutable once constructed, so returning the same instance is a
/// correct deep copy and keeps the typed exception intact (the cross-silo serialise
/// path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LatticeQuotaExceededExceptionCopier : IDeepCopier<LatticeQuotaExceededException>
{
    /// <inheritdoc />
    public LatticeQuotaExceededException DeepCopy(LatticeQuotaExceededException input, CopyContext context) => input;
}
