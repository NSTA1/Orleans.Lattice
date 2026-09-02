namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The result of a search that may have had to fetch part of the index first:
/// how many hits were written, and which retrieval path answered.
/// </summary>
/// <param name="Count">The number of results written into the caller's span.</param>
/// <param name="Mode">
/// <see cref="VectorSearchMode.Approximate"/> when the trained partitioning
/// answered, and <see cref="VectorSearchMode.Exhaustive"/> when every resident
/// vector was scored and the answer is therefore exact. Surface this verbatim;
/// an exhaustive answer from a warming index is exact, not degraded.
/// </param>
public readonly record struct VectorSearchOutcome(int Count, VectorSearchMode Mode);
