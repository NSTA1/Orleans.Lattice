using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class DataPagingTests
{
    [Test]
    public void PageSizes_AreIncrementsOf25UpTo150()
    {
        Assert.That(DataPaging.PageSizes, Is.EqualTo(new[] { 25, 50, 75, 100, 125, 150 }));
    }

    [Test]
    public void DefaultPageSize_Is25()
    {
        Assert.That(DataPaging.DefaultPageSize, Is.EqualTo(25));
    }

    [Test]
    public void Normalize_NonPositive_FallsBackToDefault()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataPaging.Normalize(0), Is.EqualTo(25));
            Assert.That(DataPaging.Normalize(-10), Is.EqualTo(25));
        });
    }

    [Test]
    public void Normalize_AboveMax_ClampsTo150()
    {
        Assert.That(DataPaging.Normalize(500), Is.EqualTo(150));
    }

    [Test]
    public void Normalize_RoundsToNearestIncrement()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataPaging.Normalize(10), Is.EqualTo(25));
            Assert.That(DataPaging.Normalize(40), Is.EqualTo(50));
            Assert.That(DataPaging.Normalize(75), Is.EqualTo(75));
        });
    }
}
