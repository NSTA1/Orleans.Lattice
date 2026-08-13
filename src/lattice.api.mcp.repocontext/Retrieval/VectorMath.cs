namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The similarity kernels used by the in-box exact k-nearest-neighbour search:
/// the dot product and the cosine similarity of two equal-length vectors. When
/// stored vectors are already L2-normalized (see
/// <see cref="VectorNormalization.UnitL2"/>) a dot product is exactly the cosine
/// similarity, so the ranker uses <see cref="Dot"/> on the fast path and
/// <see cref="Cosine"/> only when a space is un-normalized.
/// </summary>
internal static class VectorMath
{
    /// <summary>
    /// Returns the dot product of two equal-length vectors.
    /// </summary>
    /// <param name="left">The first vector.</param>
    /// <param name="right">The second vector.</param>
    /// <exception cref="ArgumentException">The vectors differ in length.</exception>
    internal static double Dot(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        if (left.Length != right.Length)
        {
            throw new ArgumentException(
                $"Cannot compute a dot product of vectors of different lengths ({left.Length} vs {right.Length}).");
        }

        double sum = 0;
        for (var i = 0; i < left.Length; i++)
        {
            sum += (double)left[i] * right[i];
        }

        return sum;
    }

    /// <summary>
    /// Returns the cosine similarity of two equal-length vectors, or <c>0</c> when
    /// either vector has zero magnitude (so a degenerate vector never produces a
    /// not-a-number score).
    /// </summary>
    /// <param name="left">The first vector.</param>
    /// <param name="right">The second vector.</param>
    /// <exception cref="ArgumentException">The vectors differ in length.</exception>
    internal static double Cosine(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        if (left.Length != right.Length)
        {
            throw new ArgumentException(
                $"Cannot compute a cosine similarity of vectors of different lengths ({left.Length} vs {right.Length}).");
        }

        double dot = 0;
        double leftMagnitude = 0;
        double rightMagnitude = 0;
        for (var i = 0; i < left.Length; i++)
        {
            double l = left[i];
            double r = right[i];
            dot += l * r;
            leftMagnitude += l * l;
            rightMagnitude += r * r;
        }

        if (leftMagnitude == 0 || rightMagnitude == 0)
        {
            return 0;
        }

        return dot / (Math.Sqrt(leftMagnitude) * Math.Sqrt(rightMagnitude));
    }
}
