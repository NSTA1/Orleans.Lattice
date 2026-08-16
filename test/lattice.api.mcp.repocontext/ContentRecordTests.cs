namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for <see cref="ContentRecord"/>: the per-file searchable-content
/// projection. <see cref="ContentRecord.Create"/> carries identity and bounded body
/// text; oversized text is truncated to <see cref="ContentRecord.MaxContentChars"/>;
/// and <see cref="ContentRecord.Merge"/> converges the last-writer-wins body across
/// replicas.
/// </summary>
[TestFixture]
public sealed class ContentRecordTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void Create_carries_identity_and_text()
    {
        var record = ContentRecord.Create("acme", "src/A.cs", "namespace Acme;", Clock(1));

        Assert.Multiple(() =>
        {
            Assert.That(record.RepoId, Is.EqualTo("acme"));
            Assert.That(record.Path, Is.EqualTo("src/A.cs"));
            Assert.That(RepoContextValues.ReadString(record.Text), Is.EqualTo("namespace Acme;"));
        });
    }

    [Test]
    public void Create_truncates_text_beyond_the_bound()
    {
        var oversized = new string('x', ContentRecord.MaxContentChars + 500);

        var record = ContentRecord.Create("acme", "big.txt", oversized, Clock(1));

        Assert.That(RepoContextValues.ReadString(record.Text)!.Length,
            Is.EqualTo(ContentRecord.MaxContentChars),
            "A file longer than the bound is truncated so the projection stays bounded.");
    }

    [Test]
    public void Merge_keeps_the_later_body_regardless_of_order()
    {
        var older = ContentRecord.Create("acme", "src/A.cs", "old body", Clock(100));
        var newer = ContentRecord.Create("acme", "src/A.cs", "new body", Clock(200));

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(ContentRecord.Merge(older, newer).Text),
                Is.EqualTo("new body"));
            Assert.That(RepoContextValues.ReadString(ContentRecord.Merge(newer, older).Text),
                Is.EqualTo("new body"), "Merge is commutative on the last-writer-wins body.");
        });
    }

    [Test]
    public void Merge_recovers_identity_from_the_specified_side()
    {
        var identity = ContentRecord.Create("acme", "src/A.cs", "body", Clock(1));
        var blank = new ContentRecord();

        var merged = ContentRecord.Merge(blank, identity);

        Assert.Multiple(() =>
        {
            Assert.That(merged.RepoId, Is.EqualTo("acme"));
            Assert.That(merged.Path, Is.EqualTo("src/A.cs"));
        });
    }

    [Test]
    public void Create_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.That(() => ContentRecord.Create(null!, "p", "t", Clock(1)), Throws.ArgumentNullException);
            Assert.That(() => ContentRecord.Create("acme", null!, "t", Clock(1)), Throws.ArgumentNullException);
            Assert.That(() => ContentRecord.Create("acme", "p", null!, Clock(1)), Throws.ArgumentNullException);
        });

    [Test]
    public void Merge_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.That(() => ContentRecord.Merge(null!, new ContentRecord()), Throws.ArgumentNullException);
            Assert.That(() => ContentRecord.Merge(new ContentRecord(), null!), Throws.ArgumentNullException);
        });
}
