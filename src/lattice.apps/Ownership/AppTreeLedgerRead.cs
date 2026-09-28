using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps;

/// <summary>A versioned read of one tree ownership ledger entry.</summary>
/// <param name="Claim">The stored claim, or <c>null</c> when absent.</param>
/// <param name="Version">The stored version, or <see cref="HybridLogicalClock.Zero"/> when absent.</param>
internal readonly record struct AppTreeLedgerRead(AppTreeClaim? Claim, HybridLogicalClock Version);
