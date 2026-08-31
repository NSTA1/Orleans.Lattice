namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the snapshot format constants and their version gate.
/// </summary>
[TestFixture]
public sealed class VectorIndexFormatTests
{
    [Test]
    public void The_header_and_chunk_markers_are_distinct()
    {
        Assert.That(VectorIndexFormat.HeaderMagic, Is.Not.EqualTo(VectorIndexFormat.ChunkMagic));
    }

    [Test]
    public void The_declared_sizes_are_the_ones_the_writers_use()
    {
        Assert.That(VectorIndexFormat.HeaderSize, Is.EqualTo(56));
        Assert.That(VectorIndexFormat.ChunkHeaderSize, Is.EqualTo(24));
    }

    [Test]
    public void The_current_version_is_supported_and_neighbouring_versions_are_not()
    {
        Assert.That(VectorIndexFormat.IsSupported(VectorIndexFormat.Version), Is.True);
        Assert.That(VectorIndexFormat.IsSupported(VectorIndexFormat.Version + 1), Is.False);
        Assert.That(VectorIndexFormat.IsSupported(VectorIndexFormat.Version - 1), Is.False);
        Assert.That(VectorIndexFormat.IsSupported(0), Is.False);
    }
}
