namespace Orleans.Lattice.Embedding.Onnx.Tests;

/// <summary>
/// Covers the pooling numerics that turn a transformer's last hidden state into
/// the single vector the client stores, without needing the ONNX model.
/// </summary>
[TestFixture]
public sealed class EmbeddingPoolingTests
{
    [Test]
    public void MeanPool_averages_over_the_token_rows()
    {
        // Two tokens, three dimensions: [1,2,3] and [3,4,5] -> mean [2,3,4].
        float[] hidden = [1, 2, 3, 3, 4, 5];
        var destination = new float[3];

        EmbeddingPooling.MeanPool(hidden, tokenCount: 2, hiddenSize: 3, normalize: false, destination);

        Assert.That(destination, Is.EqualTo(new[] { 2f, 3f, 4f }).Within(1e-5f));
    }

    [Test]
    public void MeanPool_ignores_padding_rows_beyond_the_token_count()
    {
        // The third row is padding and must not contribute; the mean of the
        // first two rows is unchanged by whatever the padding contains.
        float[] hidden = [1, 1, 3, 3, 999, 999];
        var destination = new float[2];

        EmbeddingPooling.MeanPool(hidden, tokenCount: 2, hiddenSize: 2, normalize: false, destination);

        Assert.That(destination, Is.EqualTo(new[] { 2f, 2f }).Within(1e-5f));
    }

    [Test]
    public void MeanPool_l2_normalizes_when_requested()
    {
        float[] hidden = [3, 4];
        var destination = new float[2];

        EmbeddingPooling.MeanPool(hidden, tokenCount: 1, hiddenSize: 2, normalize: true, destination);

        Assert.Multiple(() =>
        {
            Assert.That(destination[0], Is.EqualTo(0.6f).Within(1e-5f));
            Assert.That(destination[1], Is.EqualTo(0.8f).Within(1e-5f));
            Assert.That(Magnitude(destination), Is.EqualTo(1f).Within(1e-5f));
        });
    }

    [Test]
    public void MeanPool_leaves_a_zero_vector_unchanged_rather_than_producing_nan()
    {
        // Dividing by a zero norm would emit NaN and poison the vector index.
        float[] hidden = [0, 0, 0, 0];
        var destination = new float[2];

        EmbeddingPooling.MeanPool(hidden, tokenCount: 2, hiddenSize: 2, normalize: true, destination);

        Assert.That(destination, Is.EqualTo(new[] { 0f, 0f }));
        Assert.That(destination.Any(float.IsNaN), Is.False);
    }

    [Test]
    public void MeanPool_produces_a_unit_vector_for_a_realistic_dimension()
    {
        const int HiddenSize = 768;
        const int Tokens = 37;

        var hidden = new float[Tokens * HiddenSize];
        var random = new Random(1234);
        for (var i = 0; i < hidden.Length; i++)
        {
            hidden[i] = (float)((random.NextDouble() * 2) - 1);
        }

        var destination = new float[HiddenSize];
        EmbeddingPooling.MeanPool(hidden, Tokens, HiddenSize, normalize: true, destination);

        Assert.That(Magnitude(destination), Is.EqualTo(1f).Within(1e-4f));
    }

    [Test]
    public void MeanPool_rejects_a_destination_that_is_too_short()
    {
        float[] hidden = [1, 2, 3, 4];

        Assert.Throws<ArgumentException>(() =>
        {
            var destination = new float[1];
            EmbeddingPooling.MeanPool(hidden, 2, 2, false, destination);
        });
    }

    [Test]
    public void MeanPool_rejects_hidden_states_too_short_for_the_declared_shape()
    {
        Assert.Throws<ArgumentException>(() =>
        {
            float[] hidden = [1, 2];
            var destination = new float[2];
            EmbeddingPooling.MeanPool(hidden, 4, 2, false, destination);
        });
    }

    [TestCase(0, 2)]
    [TestCase(2, 0)]
    [TestCase(-1, 2)]
    public void MeanPool_rejects_non_positive_shape_arguments(int tokenCount, int hiddenSize)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            float[] hidden = [1, 2, 3, 4];
            var destination = new float[4];
            EmbeddingPooling.MeanPool(hidden, tokenCount, hiddenSize, false, destination);
        });
    }

    private static double Magnitude(float[] vector) =>
        Math.Sqrt(vector.Sum(value => (double)value * value));
}
