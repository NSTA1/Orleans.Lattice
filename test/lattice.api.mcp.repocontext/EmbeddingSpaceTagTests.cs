using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="EmbeddingSpaceTag"/> and <see cref="VectorNormalization"/>:
/// construction validation, projection from the provider-facing
/// <see cref="EmbeddingSpace"/>, the immutable-identity contract, and an Orleans
/// serialization round-trip proving the tag has a resolvable wire identity.
/// </summary>
[TestFixture]
public sealed class EmbeddingSpaceTagTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Constructor_populates_the_immutable_identity()
    {
        var tag = new EmbeddingSpaceTag("nomic-ai/nomic-embed-text-v1", 768, VectorNormalization.UnitL2);
        Assert.Multiple(() =>
        {
            Assert.That(tag.ModelId, Is.EqualTo("nomic-ai/nomic-embed-text-v1"));
            Assert.That(tag.Dimension, Is.EqualTo(768));
            Assert.That(tag.Normalization, Is.EqualTo(VectorNormalization.UnitL2));
            Assert.That(tag.IsSpecified, Is.True);
        });
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase(null)]
    public void Constructor_rejects_a_missing_model_id(string? modelId)
        => Assert.That(() => new EmbeddingSpaceTag(modelId!, 768, VectorNormalization.UnitL2),
            Throws.ArgumentException);

    [TestCase(0)]
    [TestCase(-1)]
    public void Constructor_rejects_a_non_positive_dimension(int dimension)
        => Assert.That(() => new EmbeddingSpaceTag("m", dimension, VectorNormalization.None),
            Throws.ArgumentException);

    [Test]
    public void Default_tag_is_not_specified()
        => Assert.That(default(EmbeddingSpaceTag).IsSpecified, Is.False);

    [Test]
    public void FromSpace_maps_a_normalized_space_to_unit_l2()
    {
        var tag = EmbeddingSpaceTag.FromSpace(new EmbeddingSpace("m", 3, normalized: true));
        Assert.Multiple(() =>
        {
            Assert.That(tag.ModelId, Is.EqualTo("m"));
            Assert.That(tag.Dimension, Is.EqualTo(3));
            Assert.That(tag.Normalization, Is.EqualTo(VectorNormalization.UnitL2));
        });
    }

    [Test]
    public void FromSpace_maps_an_unnormalized_space_to_none()
        => Assert.That(
            EmbeddingSpaceTag.FromSpace(new EmbeddingSpace("m", 3, normalized: false)).Normalization,
            Is.EqualTo(VectorNormalization.None));

    [Test]
    public void FromSpace_rejects_a_null_space()
        => Assert.That(() => EmbeddingSpaceTag.FromSpace(null!), Throws.ArgumentNullException);

    [Test]
    public void The_space_tag_is_immutable_and_compares_by_value()
    {
        var a = new EmbeddingSpaceTag("m", 4, VectorNormalization.UnitL2);
        var b = new EmbeddingSpaceTag("m", 4, VectorNormalization.UnitL2);

        // A 'with' expression produces a new value; the original is untouched.
        var changed = a with { Dimension = 8 };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b), "tags with identical components are value-equal");
            Assert.That(a.Dimension, Is.EqualTo(4), "the original tag is not mutated by 'with'");
            Assert.That(changed.Dimension, Is.EqualTo(8));
            Assert.That(changed, Is.Not.EqualTo(a));
        });
    }

    [Test]
    public void Round_trips_through_the_orleans_serializer()
    {
        var tag = new EmbeddingSpaceTag("nomic-ai/nomic-embed-text-v1", 768, VectorNormalization.UnitL2);
        var copy = _serializer.Deserialize<EmbeddingSpaceTag>(_serializer.SerializeToArray(tag));
        Assert.That(copy, Is.EqualTo(tag));
    }
}
