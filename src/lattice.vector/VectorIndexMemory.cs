namespace Orleans.Lattice.Vector;

/// <summary>
/// The memory accounting a <see cref="VectorIndex"/> reports through
/// <see cref="VectorIndexStatus.BytesPerVector"/>. The figures are exact for the
/// index's own contiguous blocks; they deliberately exclude the key-to-location
/// dictionary and the per-cell array headers, whose cost is a small constant that
/// varies with the runtime's own layout.
/// </summary>
public static class VectorIndexMemory
{
    /// <summary>Bytes of contiguous storage per reserved vector slot, excluding the vector body.</summary>
    /// <remarks>
    /// One <see cref="float"/> cached norm and one <see cref="long"/> key, both
    /// held in the owning cell's side arrays.
    /// </remarks>
    public const int SideBytesPerSlot = sizeof(float) + sizeof(long);

    /// <summary>
    /// Returns the total bytes the contiguous blocks occupy for the given shape:
    /// the cells' vector blocks, their per-slot side arrays, and the centroid
    /// block.
    /// </summary>
    /// <param name="capacity">The total number of reserved vector slots across every cell.</param>
    /// <param name="dimensions">The vector dimensionality.</param>
    /// <param name="partitionCount">The number of trained partitions, or <c>0</c> when untrained.</param>
    /// <exception cref="ArgumentOutOfRangeException">Any argument is negative.</exception>
    public static long Bytes(int capacity, int dimensions, int partitionCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        ArgumentOutOfRangeException.ThrowIfNegative(dimensions);
        ArgumentOutOfRangeException.ThrowIfNegative(partitionCount);

        long vectorBlocks = (long)capacity * dimensions * sizeof(float);
        long sideArrays = (long)capacity * SideBytesPerSlot;
        long centroidBlock = (long)partitionCount * dimensions * sizeof(float);
        return vectorBlocks + sideArrays + centroidBlock;
    }
}
