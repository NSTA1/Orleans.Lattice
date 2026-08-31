using System.Numerics.Tensors;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Vector;

/// <summary>
/// The vectorised similarity kernels a <see cref="VectorIndex"/> ranks with,
/// exposed so a consumer can compute an exact score with the identical
/// arithmetic the index uses.
/// <para>
/// Every kernel delegates to <see cref="TensorPrimitives"/>, which dispatches to
/// the widest SIMD width the running hardware supports. All of them are
/// allocation-free and operate over spans, so a caller can score directly out of
/// a pooled or stack-allocated buffer.
/// </para>
/// </summary>
public static class VectorSimilarity
{
    /// <summary>
    /// Returns the dot product of two equal-length vectors.
    /// </summary>
    /// <param name="left">The first vector.</param>
    /// <param name="right">The second vector.</param>
    /// <exception cref="ArgumentException">The vectors differ in length.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float Dot(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        RequireSameLength(left.Length, right.Length);
        return TensorPrimitives.Dot(left, right);
    }

    /// <summary>
    /// Returns the Euclidean (L2) norm of a vector.
    /// </summary>
    /// <param name="vector">The vector to measure.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float Norm(ReadOnlySpan<float> vector) => TensorPrimitives.Norm(vector);

    /// <summary>
    /// Returns the cosine similarity of two equal-length vectors, or <c>0</c>
    /// when either has zero magnitude, so a degenerate vector never yields a
    /// not-a-number score.
    /// </summary>
    /// <param name="left">The first vector.</param>
    /// <param name="right">The second vector.</param>
    /// <exception cref="ArgumentException">The vectors differ in length.</exception>
    public static float Cosine(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        RequireSameLength(left.Length, right.Length);
        var leftNorm = TensorPrimitives.Norm(left);
        var rightNorm = TensorPrimitives.Norm(right);
        return Scale(TensorPrimitives.Dot(left, right), leftNorm, rightNorm);
    }

    /// <summary>
    /// Scales a raw dot product by two precomputed norms to produce a cosine
    /// similarity, returning <c>0</c> when either norm is zero. The index uses
    /// this on its hot path, where the stored norm was cached at insertion and
    /// the query norm was computed once for the whole search.
    /// </summary>
    /// <param name="dot">The dot product of the two vectors.</param>
    /// <param name="leftNorm">The Euclidean norm of the first vector.</param>
    /// <param name="rightNorm">The Euclidean norm of the second vector.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float Scale(float dot, float leftNorm, float rightNorm)
    {
        var denominator = leftNorm * rightNorm;
        return denominator == 0f ? 0f : dot / denominator;
    }

    /// <summary>
    /// Scales a vector in place to unit Euclidean length. A zero-magnitude vector
    /// is left untouched, since it has no direction to preserve.
    /// </summary>
    /// <param name="vector">The vector to normalise in place.</param>
    /// <returns>The norm the vector had before scaling.</returns>
    public static float Normalize(Span<float> vector)
    {
        var norm = TensorPrimitives.Norm(vector);
        if (norm != 0f)
        {
            TensorPrimitives.Divide(vector, norm, vector);
        }

        return norm;
    }

    private static void RequireSameLength(int left, int right)
    {
        if (left != right)
        {
            throw new ArgumentException(
                $"Vectors must have equal length to be compared ({left} vs {right}).");
        }
    }
}
