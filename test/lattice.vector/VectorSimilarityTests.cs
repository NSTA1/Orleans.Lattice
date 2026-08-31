namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the vectorised similarity kernels the index ranks with.
/// </summary>
[TestFixture]
public sealed class VectorSimilarityTests
{
    [Test]
    public void Dot_returns_the_sum_of_componentwise_products()
    {
        float[] left = [1f, 2f, 3f];
        float[] right = [4f, -5f, 6f];

        Assert.That(VectorSimilarity.Dot(left, right), Is.EqualTo(12f).Within(1e-5f));
    }

    [Test]
    public void Dot_over_an_empty_pair_returns_zero()
    {
        Assert.That(VectorSimilarity.Dot([], []), Is.EqualTo(0f));
    }

    [Test]
    public void Dot_rejects_vectors_of_different_lengths()
    {
        var thrown = Assert.Throws<ArgumentException>(() => VectorSimilarity.Dot([1f, 2f], [1f]));
        Assert.That(thrown!.Message, Does.Contain("equal length"));
    }

    [Test]
    public void Norm_returns_the_euclidean_length()
    {
        Assert.That(VectorSimilarity.Norm([3f, 4f]), Is.EqualTo(5f).Within(1e-5f));
    }

    [Test]
    public void Norm_of_a_zero_vector_is_zero()
    {
        Assert.That(VectorSimilarity.Norm([0f, 0f, 0f]), Is.EqualTo(0f));
    }

    [Test]
    public void Cosine_of_identical_directions_is_one()
    {
        Assert.That(VectorSimilarity.Cosine([1f, 1f], [7f, 7f]), Is.EqualTo(1f).Within(1e-5f));
    }

    [Test]
    public void Cosine_of_opposed_directions_is_minus_one()
    {
        Assert.That(VectorSimilarity.Cosine([1f, 0f], [-4f, 0f]), Is.EqualTo(-1f).Within(1e-5f));
    }

    [Test]
    public void Cosine_of_orthogonal_vectors_is_zero()
    {
        Assert.That(VectorSimilarity.Cosine([1f, 0f], [0f, 1f]), Is.EqualTo(0f).Within(1e-5f));
    }

    [Test]
    public void Cosine_against_a_zero_magnitude_vector_is_zero_not_nan()
    {
        var score = VectorSimilarity.Cosine([0f, 0f], [1f, 1f]);

        Assert.That(float.IsNaN(score), Is.False);
        Assert.That(score, Is.EqualTo(0f));
    }

    [Test]
    public void Cosine_rejects_vectors_of_different_lengths()
    {
        Assert.Throws<ArgumentException>(() => VectorSimilarity.Cosine([1f], [1f, 2f]));
    }

    [Test]
    public void Scale_divides_a_dot_product_by_the_product_of_the_norms()
    {
        Assert.That(VectorSimilarity.Scale(dot: 6f, leftNorm: 2f, rightNorm: 3f), Is.EqualTo(1f).Within(1e-6f));
    }

    [Test]
    public void Scale_returns_zero_when_either_norm_is_zero()
    {
        Assert.That(VectorSimilarity.Scale(6f, 0f, 3f), Is.EqualTo(0f));
        Assert.That(VectorSimilarity.Scale(6f, 2f, 0f), Is.EqualTo(0f));
    }

    [Test]
    public void Normalize_scales_a_vector_to_unit_length_and_returns_the_previous_norm()
    {
        float[] vector = [3f, 4f];

        var previous = VectorSimilarity.Normalize(vector);

        Assert.That(previous, Is.EqualTo(5f).Within(1e-5f));
        Assert.That(VectorSimilarity.Norm(vector), Is.EqualTo(1f).Within(1e-5f));
        Assert.That(vector[0], Is.EqualTo(0.6f).Within(1e-5f));
    }

    [Test]
    public void Normalize_leaves_a_zero_vector_untouched()
    {
        float[] vector = [0f, 0f];

        var previous = VectorSimilarity.Normalize(vector);

        Assert.That(previous, Is.EqualTo(0f));
        Assert.That(vector, Is.EqualTo(new[] { 0f, 0f }));
    }

    [Test]
    public void Cosine_of_normalised_vectors_equals_their_dot_product()
    {
        float[] left = [1f, 2f, 3f, 4f];
        float[] right = [-2f, 5f, 1f, 0.5f];
        VectorSimilarity.Normalize(left);
        VectorSimilarity.Normalize(right);

        Assert.That(
            VectorSimilarity.Cosine(left, right),
            Is.EqualTo(VectorSimilarity.Dot(left, right)).Within(1e-5f));
    }
}
