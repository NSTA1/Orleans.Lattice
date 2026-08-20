using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

/// <summary>
/// Direct unit tests for the paged Data scan result records
/// (<see cref="DataPage"/> and <see cref="TagMemberPage"/>): their empty
/// singletons and the <c>HasMore</c> continuation projection.
/// </summary>
[TestFixture]
public class DataPageRecordsTests
{
    [Test]
    public void DataPage_Empty_has_no_entries_and_no_continuation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataPage.Empty.Entries, Is.Empty);
            Assert.That(DataPage.Empty.ContinuationToken, Is.Null);
            Assert.That(DataPage.Empty.HasMore, Is.False);
        });
    }

    [Test]
    public void DataPage_with_continuation_reports_has_more()
    {
        var page = new DataPage { ContinuationToken = "cursor" };

        Assert.That(page.HasMore, Is.True);
    }

    [Test]
    public void TagMemberPage_Empty_has_no_members_and_no_continuation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TagMemberPage.Empty.Members, Is.Empty);
            Assert.That(TagMemberPage.Empty.ContinuationToken, Is.Null);
            Assert.That(TagMemberPage.Empty.HasMore, Is.False);
        });
    }

    [Test]
    public void TagMemberPage_with_continuation_reports_has_more()
    {
        var page = new TagMemberPage { ContinuationToken = "cursor" };

        Assert.That(page.HasMore, Is.True);
    }
}
