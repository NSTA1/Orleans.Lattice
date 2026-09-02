namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the snapshot header's binary encoding and its refusal of a
/// persisted form this build cannot read.
/// </summary>
[TestFixture]
public sealed class VectorIndexHeaderTests
{
    private static VectorIndexHeader Sample() => new(
        VectorIndexFormat.Version,
        Dimensions: 768,
        VectorDistanceMetric.Cosine,
        PartitionCount: 271,
        Probes: 34,
        Seed: 0x0123456789ABCDEFUL,
        Count: 73_537,
        ChunkCount: 300,
        CentroidChunkCount: 3,
        IndexVersion: 987_654_321L);

    [Test]
    public void Size_matches_the_declared_format_constant()
    {
        Assert.That(VectorIndexHeader.Size, Is.EqualTo(VectorIndexFormat.HeaderSize));
    }

    [Test]
    public void A_header_round_trips_through_its_binary_form()
    {
        var header = Sample();
        var buffer = new byte[VectorIndexHeader.Size];

        var written = header.Write(buffer);
        var read = VectorIndexHeader.Read(buffer);

        Assert.That(written, Is.EqualTo(VectorIndexHeader.Size));
        Assert.That(read, Is.EqualTo(header));
    }

    [Test]
    public void A_dot_product_header_round_trips_too()
    {
        var header = Sample() with { Metric = VectorDistanceMetric.DotProduct };
        var buffer = new byte[VectorIndexHeader.Size];
        header.Write(buffer);

        Assert.That(VectorIndexHeader.Read(buffer).Metric, Is.EqualTo(VectorDistanceMetric.DotProduct));
    }

    [Test]
    public void Write_rejects_a_destination_that_is_too_short()
    {
        Assert.Throws<ArgumentException>(() => Sample().Write(new byte[VectorIndexHeader.Size - 1]));
    }

    [Test]
    public void Write_accepts_a_destination_larger_than_the_header()
    {
        Assert.That(Sample().Write(new byte[256]), Is.EqualTo(VectorIndexHeader.Size));
    }

    [Test]
    public void Read_rejects_a_truncated_header()
    {
        var thrown = Assert.Throws<VectorIndexFormatException>(() => VectorIndexHeader.Read(new byte[8]));

        Assert.That(thrown!.Message, Does.Contain("only 8 were supplied"));
    }

    [Test]
    public void Read_rejects_bytes_that_do_not_open_with_the_header_marker()
    {
        var thrown = Assert.Throws<VectorIndexFormatException>(
            () => VectorIndexHeader.Read(new byte[VectorIndexHeader.Size]));

        Assert.That(thrown!.Message, Does.Contain("marker"));
    }

    [Test]
    public void Read_rejects_a_format_version_this_build_does_not_support()
    {
        var buffer = new byte[VectorIndexHeader.Size];
        (Sample() with { FormatVersion = VectorIndexFormat.Version + 1 }).Write(buffer);

        var thrown = Assert.Throws<VectorIndexFormatException>(() => VectorIndexHeader.Read(buffer));

        Assert.That(thrown!.Message, Does.Contain("is not supported by this build"));
    }

    [Test]
    public void Read_rejects_a_non_positive_dimensionality()
    {
        var buffer = new byte[VectorIndexHeader.Size];
        (Sample() with { Dimensions = 0 }).Write(buffer);

        Assert.Throws<VectorIndexFormatException>(() => VectorIndexHeader.Read(buffer));
    }

    [Test]
    public void Read_rejects_a_metric_outside_the_defined_members()
    {
        var buffer = new byte[VectorIndexHeader.Size];
        (Sample() with { Metric = (VectorDistanceMetric)7 }).Write(buffer);

        Assert.Throws<VectorIndexFormatException>(() => VectorIndexHeader.Read(buffer));
    }

    [Test]
    public void Read_rejects_a_negative_count()
    {
        var buffer = new byte[VectorIndexHeader.Size];
        (Sample() with { Count = -1 }).Write(buffer);

        Assert.Throws<VectorIndexFormatException>(() => VectorIndexHeader.Read(buffer));
    }

    [Test]
    public void TryRead_returns_the_header_for_a_readable_form()
    {
        var buffer = new byte[VectorIndexHeader.Size];
        var header = Sample();
        header.Write(buffer);

        Assert.That(VectorIndexHeader.TryRead(buffer, out var read), Is.True);
        Assert.That(read, Is.EqualTo(header));
    }

    [Test]
    public void TryRead_reports_failure_instead_of_throwing_on_an_unreadable_form()
    {
        Assert.That(VectorIndexHeader.TryRead(new byte[VectorIndexHeader.Size], out var read), Is.False);
        Assert.That(read, Is.EqualTo(default(VectorIndexHeader)));
    }

    [Test]
    public void TryRead_reports_failure_for_a_future_format_version()
    {
        var buffer = new byte[VectorIndexHeader.Size];
        (Sample() with { FormatVersion = 999 }).Write(buffer);

        Assert.That(VectorIndexHeader.TryRead(buffer, out _), Is.False);
    }
}
