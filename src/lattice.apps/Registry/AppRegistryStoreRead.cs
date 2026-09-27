using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps;

/// <summary>A versioned read of one app registry record.</summary>
/// <param name="Record">The stored record, or <c>null</c> when absent.</param>
/// <param name="Version">The stored version, or <see cref="HybridLogicalClock.Zero"/> when absent.</param>
internal readonly record struct AppRegistryStoreRead(AppRegistryRecord? Record, HybridLogicalClock Version);
