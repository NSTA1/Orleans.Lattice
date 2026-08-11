namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for the embedding seam value types: <see cref="EmbeddingSpace"/>
/// and <see cref="EmbeddingResult"/>. They prove the space validates its identity
/// and that the result's factories enforce the fail-closed contract (a success
/// carries vectors; a failure carries an error and no vectors).
/// </summary>
[TestFixture]
public sealed class EmbeddingSeamTests
{
    [Test]
    public void EmbeddingSpace_exposes_its_identity()
    {
        var space = new EmbeddingSpace("acme/embed", 384, normalized: true);

        Assert.Multiple(() =>
        {
            Assert.That(space.ModelId, Is.EqualTo("acme/embed"));
            Assert.That(space.Dimension, Is.EqualTo(384));
            Assert.That(space.Normalized, Is.True);
        });
    }

    [Test]
    public void EmbeddingSpace_with_the_same_identity_is_equal()
    {
        var a = new EmbeddingSpace("acme/embed", 384, true);
        var b = new EmbeddingSpace("acme/embed", 384, true);

        Assert.That(a, Is.EqualTo(b));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void EmbeddingSpace_rejects_a_missing_model_id(string? modelId)
        => Assert.Throws<ArgumentException>(() => new EmbeddingSpace(modelId!, 8, true));

    [TestCase(0)]
    [TestCase(-1)]
    public void EmbeddingSpace_rejects_a_non_positive_dimension(int dimension)
        => Assert.Throws<ArgumentException>(() => new EmbeddingSpace("acme/embed", dimension, true));

    [Test]
    public void EmbeddingResult_Success_carries_the_vectors_and_no_error()
    {
        var space = new EmbeddingSpace("acme/embed", 2, true);
        var vectors = new ReadOnlyMemory<float>[] { new float[] { 1f, 0f } };

        var result = EmbeddingResult.Success(space, vectors);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.True);
            Assert.That(result.Error, Is.Null);
            Assert.That(result.Space, Is.SameAs(space));
            Assert.That(result.Vectors, Is.SameAs(vectors));
        });
    }

    [Test]
    public void EmbeddingResult_Failure_carries_the_error_and_no_vectors()
    {
        var space = new EmbeddingSpace("acme/embed", 2, true);

        var result = EmbeddingResult.Failure(space, "provider unreachable");

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(result.Error, Is.EqualTo("provider unreachable"));
            Assert.That(result.Space, Is.SameAs(space));
            Assert.That(result.Vectors, Is.Empty);
        });
    }

    [Test]
    public void EmbeddingResult_Success_rejects_null_arguments()
    {
        var space = new EmbeddingSpace("acme/embed", 2, true);

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => EmbeddingResult.Success(null!, Array.Empty<ReadOnlyMemory<float>>()));
            Assert.Throws<ArgumentNullException>(() => EmbeddingResult.Success(space, null!));
        });
    }

    [Test]
    public void EmbeddingResult_Failure_rejects_a_missing_space_or_error()
    {
        var space = new EmbeddingSpace("acme/embed", 2, true);

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => EmbeddingResult.Failure(null!, "boom"));
            Assert.Throws<ArgumentException>(() => EmbeddingResult.Failure(space, "  "));
        });
    }
}
