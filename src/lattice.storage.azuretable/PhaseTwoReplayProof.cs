using Azure;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The outcome of a phase-2 idempotent-replay proof (issues #5520, #5810): whether every
/// effect of the transaction is resident, and the ETag of the <c>TAIL</c> row read while
/// proving it, when that row holds exactly the value the transaction wrote.
/// </summary>
/// <param name="Proven">Whether every effect of the transaction is resident.</param>
/// <param name="TailETag">The <c>TAIL</c> row's ETag, or <see langword="null"/> when it holds another value or the transaction wrote none.</param>
internal readonly record struct PhaseTwoReplayProof(bool Proven, ETag? TailETag);
