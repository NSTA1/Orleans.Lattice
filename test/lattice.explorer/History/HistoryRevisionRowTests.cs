using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryRevisionRowTests
{
    [Test]
    public void From_SetWithValueRetained_RendersValueDiffMode()
    {
        var row = HistoryRevisionRow.From(RevisionFactory.Set(10, value: "{\"a\":1}", valueRetained: true));

        Assert.Multiple(() =>
        {
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.ValueDiff));
            Assert.That(row.Value, Is.Not.Null);
            Assert.That(row.Value!.Format, Is.EqualTo(ValueFormat.Json));
            Assert.That(row.ValueRetained, Is.True);
        });
    }

    [Test]
    public void From_SetWithoutValueRetained_IsMetadataOnly()
    {
        var row = HistoryRevisionRow.From(RevisionFactory.Set(
            10, value: "x", valueRetained: false, mode: HistoryRetentionMode.MetadataOnly,
            valueLength: 42, valueHash: 99));

        Assert.Multiple(() =>
        {
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.MetadataOnly));
            Assert.That(row.Value, Is.Null);
            Assert.That(row.ValueLength, Is.EqualTo(42));
            Assert.That(row.ValueHash, Is.EqualTo(99));
            Assert.That(row.RetentionMode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        });
    }

    [Test]
    public void From_CrdtWithMembers_MapsMemberChanges()
    {
        var record = RevisionFactory.Crdt(10, new[]
        {
            RevisionFactory.Member("apple", CrdtMemberChangeKind.Added, "replica-a", 3),
            RevisionFactory.Member("pear", CrdtMemberChangeKind.Removed, "replica-b", 4),
        });

        var row = HistoryRevisionRow.From(record);

        Assert.Multiple(() =>
        {
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.CrdtMembers));
            Assert.That(row.MemberChanges, Has.Count.EqualTo(2));
            Assert.That(row.MemberChanges[0].ElementText, Is.EqualTo("apple"));
            Assert.That(row.MemberChanges[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(row.MemberChanges[0].ReplicaId, Is.EqualTo("replica-a"));
            Assert.That(row.MemberChanges[1].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
            Assert.That(row.MemberChanges[1].Ordinal, Is.EqualTo(4));
        });
    }

    [Test]
    public void From_CrdtMetadataOnly_HasNoMemberChanges()
    {
        var record = RevisionFactory.Crdt(10, members: Array.Empty<CrdtMemberChange>(),
            valueRetained: false, mode: HistoryRetentionMode.MetadataOnly);

        var row = HistoryRevisionRow.From(record);

        Assert.Multiple(() =>
        {
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.CrdtMembers));
            Assert.That(row.MemberChanges, Is.Empty);
        });
    }

    [Test]
    public void From_Delete_IsDeleteMode()
    {
        var row = HistoryRevisionRow.From(RevisionFactory.Delete(10));

        Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.Delete));
    }

    [Test]
    public void From_RangeTombstone_CarriesEndKey()
    {
        var row = HistoryRevisionRow.From(RevisionFactory.RangeTombstone(10, endKey: "m"));

        Assert.Multiple(() =>
        {
            Assert.That(row.RenderMode, Is.EqualTo(HistoryRowRenderMode.RangeTombstone));
            Assert.That(row.EndKey, Is.EqualTo("m"));
        });
    }

    [Test]
    public void From_CarriesOriginAndClock()
    {
        var row = HistoryRevisionRow.From(RevisionFactory.Set(123, value: "v", originClusterId: "east"));

        Assert.Multiple(() =>
        {
            Assert.That(row.OriginClusterId, Is.EqualTo("east"));
            Assert.That(row.Hlc.WallClockTicks, Is.EqualTo(123));
        });
    }

    [Test]
    public void From_Null_Throws()
    {
        Assert.That(() => HistoryRevisionRow.From(null!), Throws.ArgumentNullException);
    }
}
