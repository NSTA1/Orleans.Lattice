namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="VectorMath"/>: the dot product and cosine kernels,
/// including the zero-magnitude guard and the length-mismatch rejection.
/// </summary>
[TestFixture]
public sealed class VectorMathTests
{
    [Test]
    public void Dot_multiplies_and_sums_componentwise()
        => Assert.That(VectorMath.Dot(new[] { 1f, 2f, 3f }, new[] { 4f, 5f, 6f }), Is.EqualTo(32d).Within(1e-9));

    [Test]
    public void Dot_rejects_length_mismatch()
        => Assert.Throws<ArgumentException>(() => VectorMath.Dot(new[] { 1f }, new[] { 1f, 2f }));

    [Test]
    public void Cosine_of_parallel_unit_vectors_is_one()
        => Assert.That(VectorMath.Cosine(new[] { 1f, 0f }, new[] { 1f, 0f }), Is.EqualTo(1d).Within(1e-9));

    [Test]
    public void Cosine_of_orthogonal_vectors_is_zero()
        => Assert.That(VectorMath.Cosine(new[] { 1f, 0f }, new[] { 0f, 1f }), Is.EqualTo(0d).Within(1e-9));

    [Test]
    public void Cosine_ignores_magnitude()
        => Assert.That(VectorMath.Cosine(new[] { 1f, 1f }, new[] { 3f, 3f }), Is.EqualTo(1d).Within(1e-9));

    [Test]
    public void Cosine_returns_zero_for_a_zero_magnitude_vector()
        => Assert.That(VectorMath.Cosine(new[] { 0f, 0f }, new[] { 1f, 1f }), Is.EqualTo(0d));

    [Test]
    public void Cosine_rejects_length_mismatch()
        => Assert.Throws<ArgumentException>(() => VectorMath.Cosine(new[] { 1f }, new[] { 1f, 2f }));
}
