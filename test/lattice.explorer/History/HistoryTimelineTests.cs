using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryTimelineTests
{
    private static IReadOnlyList<HistoryRevisionRow> Rows(params EntryRevisionRecord[] records) =>
        records.Select(HistoryRevisionRow.From).ToArray();

    private static HistoryTimeline Build(
        IReadOnlyList<HistoryRevisionRow> chronological,
        EntryHistoryBound bound = EntryHistoryBound.BoundedByAge,
        bool newestFirst = true,
        string? continuation = null,
        HybridLogicalClock earliest = default) =>
        HistoryTimeline.Build("tree-1", "k", StateQueryStatus.Found, chronological, bound, earliest, continuation, newestFirst);

    [Test]
    public void Build_FullValueLww_SecondRevisionHasDiff()
    {
        var rows = Rows(
            RevisionFactory.Set(10, value: "line1\nline2"),
            RevisionFactory.Set(20, value: "line1\nline2-changed"));

        // Oldest-first to inspect the diff on the newer revision directly.
        var timeline = Build(rows, newestFirst: false);

        Assert.Multiple(() =>
        {
            Assert.That(timeline.Rows[0].Diff, Is.Empty, "oldest revision has nothing to diff against");
            Assert.That(timeline.Rows[1].Diff, Is.Not.Empty);
            Assert.That(timeline.Rows[1].Diff.Any(d => d.Kind == HistoryDiffLineKind.Added), Is.True);
        });
    }

    [Test]
    public void Build_LiveTailRow_HasNoDiffNoDividerAndDoesNotChangeActiveMode()
    {
        // A retained Set (full-value) followed by a live-tail row whose retention
        // descriptor defaults differently: the live row must not emit a retention
        // divider, must not be diffed, and must not become the active-mode probe.
        var durable = HistoryRevisionRow.From(RevisionFactory.Set(
            10, value: "v", mode: HistoryRetentionMode.FullValue));
        var live = HistoryRevisionRow.FromLive(NotificationFactory.Set("k", 20));

        var timeline = Build(new[] { durable, live }, newestFirst: false);

        Assert.Multiple(() =>
        {
            Assert.That(timeline.Rows[1].RenderMode, Is.EqualTo(HistoryRowRenderMode.LiveTail));
            Assert.That(timeline.Rows[1].Diff, Is.Empty);
            Assert.That(timeline.Rows[1].RetentionChange, Is.Null, "a live-tail row never emits a retention divider");
            Assert.That(timeline.ActiveRetentionMode, Is.EqualTo(HistoryRetentionMode.FullValue),
                "the active badge tracks the newest durable revision, not the live tail");
        });
    }

    [Test]
    public void Build_MetadataOnlyLww_HasNoDiffAndNoValue()
    {
        var rows = Rows(
            RevisionFactory.Set(10, valueRetained: false, mode: HistoryRetentionMode.MetadataOnly, valueLength: 5, valueHash: 7),
            RevisionFactory.Set(20, valueRetained: false, mode: HistoryRetentionMode.MetadataOnly, valueLength: 6, valueHash: 8));

        var timeline = Build(rows);

        Assert.Multiple(() =>
        {
            Assert.That(timeline.Rows.Select(r => r.RenderMode), Is.All.EqualTo(HistoryRowRenderMode.MetadataOnly));
            Assert.That(timeline.Rows.SelectMany(r => r.Diff), Is.Empty);
            Assert.That(timeline.Rows.Select(r => r.Value), Is.All.Null);
        });
    }

    [Test]
    public void Build_Crdt_ShowsMemberEvents()
    {
        var rows = Rows(RevisionFactory.Crdt(10, new[]
        {
            RevisionFactory.Member("apple", CrdtMemberChangeKind.Added, "replica-a"),
        }));

        var timeline = Build(rows);

        Assert.Multiple(() =>
        {
            Assert.That(timeline.Rows[0].RenderMode, Is.EqualTo(HistoryRowRenderMode.CrdtMembers));
            Assert.That(timeline.Rows[0].MemberChanges[0].ElementText, Is.EqualTo("apple"));
            Assert.That(timeline.Rows[0].Diff, Is.Empty, "a CRDT revision is not value-diffed");
        });
    }

    [Test]
    public void Build_RetentionTransition_MarksDividerAtNewerRevision()
    {
        var rows = Rows(
            RevisionFactory.Set(10, value: "v1", valueRetained: true, mode: HistoryRetentionMode.FullValue),
            RevisionFactory.Set(20, valueRetained: false, mode: HistoryRetentionMode.MetadataOnly, valueHash: 3));

        // Oldest-first: index 1 is the newer (metadata-only) revision the boundary sits at.
        var timeline = Build(rows, newestFirst: false);

        Assert.Multiple(() =>
        {
            Assert.That(timeline.Rows[0].RetentionChange, Is.Null);
            Assert.That(timeline.Rows[1].RetentionChange, Is.Not.Null);
            var transition = timeline.Rows[1].RetentionChange!.Value;
            Assert.That(transition.From, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(transition.To, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
            Assert.That(transition.Label(), Is.EqualTo("retention changed: full-value -> metadata-only"));
        });
    }

    [Test]
    public void Build_HybridValueToMetadata_MarksDividerOnValueRetainedChange()
    {
        // Same Hybrid mode on both rows, but the older retained its value and the
        // newer (aged-out) did not: the descriptor differs, so a divider appears.
        var rows = Rows(
            RevisionFactory.Set(10, value: "v", valueRetained: true, mode: HistoryRetentionMode.Hybrid),
            RevisionFactory.Set(20, valueRetained: false, mode: HistoryRetentionMode.Hybrid, valueHash: 1));

        var timeline = Build(rows, newestFirst: false);

        Assert.That(timeline.Rows[1].RetentionChange, Is.Not.Null);
    }

    [Test]
    public void Build_NewestFirst_OrdersHighestClockFirst()
    {
        var rows = Rows(
            RevisionFactory.Set(10, value: "a"),
            RevisionFactory.Set(20, value: "b"),
            RevisionFactory.Set(30, value: "c"));

        var timeline = Build(rows, newestFirst: true);

        Assert.That(timeline.Rows.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 30, 20, 10 }));
    }

    [Test]
    public void Build_OldestFirst_OrdersLowestClockFirst()
    {
        var rows = Rows(
            RevisionFactory.Set(10, value: "a"),
            RevisionFactory.Set(20, value: "b"));

        var timeline = Build(rows, newestFirst: false);

        Assert.That(timeline.Rows.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 10, 20 }));
    }

    [Test]
    public void Build_ActiveRetentionMode_IsNewestRevisionMode()
    {
        var rows = Rows(
            RevisionFactory.Set(10, value: "v", mode: HistoryRetentionMode.FullValue),
            RevisionFactory.Set(20, valueRetained: false, mode: HistoryRetentionMode.MetadataOnly, valueHash: 1));

        var timeline = Build(rows);

        Assert.Multiple(() =>
        {
            Assert.That(timeline.ActiveRetentionMode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
            Assert.That(timeline.ActiveValueRetained, Is.False);
        });
    }

    [Test]
    public void Build_TruncatedBound_CarriesBoundAndEarliest()
    {
        var rows = Rows(RevisionFactory.Set(20, value: "v"));

        var timeline = Build(rows, bound: EntryHistoryBound.Truncated, earliest: RevisionFactory.Hlc(15));

        Assert.Multiple(() =>
        {
            Assert.That(timeline.Bound, Is.EqualTo(EntryHistoryBound.Truncated));
            Assert.That(timeline.EarliestAvailable.WallClockTicks, Is.EqualTo(15));
        });
    }

    [Test]
    public void Build_WalWindowFallbackBound_CarriesBound()
    {
        var rows = Rows(RevisionFactory.Set(20, value: "v"));

        var timeline = Build(rows, bound: EntryHistoryBound.WalWindowFallback);

        Assert.That(timeline.Bound, Is.EqualTo(EntryHistoryBound.WalWindowFallback));
    }

    [Test]
    public void Build_BoundedByAge_IsDefaultDurableBound()
    {
        var rows = Rows(RevisionFactory.Set(20, value: "v"));

        var timeline = Build(rows, bound: EntryHistoryBound.BoundedByAge);

        Assert.That(timeline.Bound, Is.EqualTo(EntryHistoryBound.BoundedByAge));
    }

    [Test]
    public void Build_ContinuationToken_SetsHasMore()
    {
        var rows = Rows(RevisionFactory.Set(20, value: "v"));

        var timeline = Build(rows, continuation: "next");

        Assert.Multiple(() =>
        {
            Assert.That(timeline.HasMore, Is.True);
            Assert.That(timeline.ContinuationToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public void Build_NoRows_HasNoActiveModeAndNoRows()
    {
        var timeline = Build(Array.Empty<HistoryRevisionRow>());

        Assert.Multiple(() =>
        {
            Assert.That(timeline.HasRows, Is.False);
            Assert.That(timeline.ActiveRetentionMode, Is.Null);
        });
    }

    [Test]
    public void Build_DeleteBetweenValues_DiffsAgainstLastRetainedValue()
    {
        var rows = Rows(
            RevisionFactory.Set(10, value: "first"),
            RevisionFactory.Delete(20),
            RevisionFactory.Set(30, value: "second"));

        var timeline = Build(rows, newestFirst: false);

        // The third revision (index 2) diffs against the first retained value,
        // skipping the delete that carried no value.
        Assert.That(timeline.Rows[2].Diff.Any(d => d.Kind == HistoryDiffLineKind.Removed && d.Text == "first"), Is.True);
    }
}
