using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryRevisionRowLiveTests
{
    [Test]
    public void FromLive_Set_IsLiveTailMetadataRow()
    {
        var row = HistoryRevisionRow.FromLive(NotificationFactory.Set("k", 42, position: "p1"));

        Assert.Multiple(() =>
        {
            Assert.That(row.IsLiveTail, Is.True);
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.LiveTail));
            Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(row.Hlc.WallClockTicks, Is.EqualTo(42));
            Assert.That(row.Position, Is.EqualTo("p1"));
            Assert.That(row.Category, Is.EqualTo(MutationCategory.User));
            Assert.That(row.Value, Is.Null, "the live feed carries no value preview");
            Assert.That(row.Diff, Is.Empty);
            Assert.That(row.MemberChanges, Is.Empty);
            Assert.That(row.OriginClusterId, Is.Null);
        });
    }

    [Test]
    public void FromLive_Delete_MapsToDeleteKind()
    {
        var row = HistoryRevisionRow.FromLive(NotificationFactory.Delete("k", 5));

        Assert.Multiple(() =>
        {
            Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.Delete));
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.LiveTail));
            Assert.That(row.IsLiveTail, Is.True);
        });
    }

    [Test]
    public void FromLive_DeleteRange_MapsToRangeTombstoneWithEndKey()
    {
        var row = HistoryRevisionRow.FromLive(NotificationFactory.DeleteRange("a", "m", 7));

        Assert.Multiple(() =>
        {
            Assert.That(row.Kind, Is.EqualTo(HistoryRowKind.RangeTombstone));
            Assert.That(row.EndKey, Is.EqualTo("m"));
            Assert.That(row.IsLiveTail, Is.True);
        });
    }

    [Test]
    public void FromLive_Null_Throws()
    {
        Assert.That(() => HistoryRevisionRow.FromLive(null!), Throws.ArgumentNullException);
    }
}
