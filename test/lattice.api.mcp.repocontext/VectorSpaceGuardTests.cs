namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="VectorSpaceGuard"/>: the fail-closed embedding-space match
/// guard used at the future retrieval seam. A same-space query is accepted; a
/// mismatched model, dimension, or normalization convention is rejected with a
/// clear <see cref="EmbeddingSpaceMismatchException"/>.
/// </summary>
[TestFixture]
public sealed class VectorSpaceGuardTests
{
    private static EmbeddingSpaceTag Space(
        string model = "m", int dimension = 768, VectorNormalization norm = VectorNormalization.UnitL2)
        => new(model, dimension, norm);

    [Test]
    public void Matching_spaces_are_accepted()
    {
        var stored = Space();
        var query = Space();
        Assert.Multiple(() =>
        {
            Assert.That(VectorSpaceGuard.Matches(stored, query), Is.True);
            Assert.That(() => VectorSpaceGuard.EnsureMatch(stored, query), Throws.Nothing);
        });
    }

    [Test]
    public void A_mismatched_model_is_rejected()
    {
        var stored = Space(model: "model-a");
        var query = Space(model: "model-b");

        Assert.Multiple(() =>
        {
            Assert.That(VectorSpaceGuard.Matches(stored, query), Is.False);
            var ex = Assert.Throws<EmbeddingSpaceMismatchException>(
                () => VectorSpaceGuard.EnsureMatch(stored, query));
            Assert.That(ex!.Message, Does.Contain("model"));
        });
    }

    [Test]
    public void A_mismatched_dimension_is_rejected()
    {
        var stored = Space(dimension: 768);
        var query = Space(dimension: 384);

        Assert.Multiple(() =>
        {
            Assert.That(VectorSpaceGuard.Matches(stored, query), Is.False);
            var ex = Assert.Throws<EmbeddingSpaceMismatchException>(
                () => VectorSpaceGuard.EnsureMatch(stored, query));
            Assert.That(ex!.Message, Does.Contain("dimension"));
        });
    }

    [Test]
    public void A_mismatched_normalization_is_rejected()
    {
        var stored = Space(norm: VectorNormalization.UnitL2);
        var query = Space(norm: VectorNormalization.None);

        Assert.Multiple(() =>
        {
            Assert.That(VectorSpaceGuard.Matches(stored, query), Is.False);
            var ex = Assert.Throws<EmbeddingSpaceMismatchException>(
                () => VectorSpaceGuard.EnsureMatch(stored, query));
            Assert.That(ex!.Message, Does.Contain("normalization"));
        });
    }

    [Test]
    public void The_provider_facing_overload_projects_and_matches()
    {
        var stored = Space(model: "m", dimension: 3, norm: VectorNormalization.UnitL2);
        var query = new EmbeddingSpace("m", 3, normalized: true);

        Assert.Multiple(() =>
        {
            Assert.That(VectorSpaceGuard.Matches(stored, query), Is.True);
            Assert.That(() => VectorSpaceGuard.EnsureMatch(stored, query), Throws.Nothing);
        });
    }

    [Test]
    public void The_provider_facing_overload_rejects_a_mismatch()
    {
        var stored = Space(model: "m", dimension: 3, norm: VectorNormalization.UnitL2);
        var query = new EmbeddingSpace("m", 4, normalized: true);
        Assert.That(() => VectorSpaceGuard.EnsureMatch(stored, query),
            Throws.TypeOf<EmbeddingSpaceMismatchException>());
    }

    [Test]
    public void The_provider_facing_overload_rejects_a_null_query()
    {
        var stored = Space();
        Assert.Multiple(() =>
        {
            Assert.That(() => VectorSpaceGuard.Matches(stored, (EmbeddingSpace)null!),
                Throws.ArgumentNullException);
            Assert.That(() => VectorSpaceGuard.EnsureMatch(stored, (EmbeddingSpace)null!),
                Throws.ArgumentNullException);
        });
    }
}
