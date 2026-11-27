using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth;

/// <summary>
/// What the admin audit trail observed of an anchor record at one moment.
/// </summary>
/// <param name="Version">
/// The anchor's version, or <see langword="null"/> when it is absent or its tree does
/// not expose versions (<see cref="Versioned"/> is <see langword="false"/>).
/// </param>
/// <param name="Hash">An xxHash64 of the anchor's value, <c>0</c> when absent.</param>
/// <param name="Versioned">Whether the anchor tree exposes versions and per-key history.</param>
internal readonly record struct AdminAuditAnchorObservation(HybridLogicalClock? Version, long Hash, bool Versioned);
