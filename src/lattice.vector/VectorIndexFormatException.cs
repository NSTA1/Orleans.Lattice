namespace Orleans.Lattice.Vector;

/// <summary>
/// Thrown when a persisted <see cref="VectorIndex"/> snapshot cannot be read:
/// its marker is wrong, its format version is one this build does not support,
/// it is truncated, or a chunk contradicts the header it belongs to.
/// <para>
/// This derives directly from <see cref="Exception"/> so that a consumer which
/// later makes it serializable does not need a hand-written deep copier.
/// </para>
/// </summary>
public sealed class VectorIndexFormatException : Exception
{
    /// <summary>Creates the exception with a message describing the defect.</summary>
    /// <param name="message">A description of why the snapshot could not be read.</param>
    public VectorIndexFormatException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and an underlying cause.</summary>
    /// <param name="message">A description of why the snapshot could not be read.</param>
    /// <param name="innerException">The underlying cause.</param>
    public VectorIndexFormatException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
