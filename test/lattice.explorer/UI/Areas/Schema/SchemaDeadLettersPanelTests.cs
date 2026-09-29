using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The Dead letters tab: counted on arrival, listed on demand a page at a time,
/// previews as text only, empty, refused and failed reads, and the compact rows
/// with their detail sheet.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaDeadLettersPanelTests : SchemaTestContext
{
    private IRenderedComponent<SchemaTreePage> Open(LtBreakpoint? band = null)
    {
        var cut = RenderAt<SchemaTreePage>("schema/orders?tab=dead-letters", band);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel] .lt-skeleton"), Is.Empty));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private void UseDeadLetters(int count)
    {
        UseEstate();
        Schema.DeadLetters["orders"] = [.. Enumerable.Range(0, count).Select(index => SchemaTestData.DeadLetter($"order/{index:0000}"))];
    }

    [Test]
    public void The_queue_is_counted_on_arrival_and_listed_on_demand()
    {
        UseDeadLetters(3);

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-status").TextContent, Is.EqualTo("3 dead letters."));
            Assert.That(Schema.CountOf("ListDeadLetters"), Is.Zero, "the queue can be large, so it is listed only on demand");
        });

        Button(cut, "Load dead letters").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel] tbody tr"), Has.Count.EqualTo(3));
            Assert.That(cut.FindAll("[role=tabpanel] tbody tr")[0].Children.Select(cell => cell.TextContent.Trim()),
                Is.EqualTo(new[] { "order/0000", "currency does not match", "Replication", "2026-01-01 12:00:00 UTC", "19 bytes" }));
            Assert.That(Button(cut, "Refresh"), Is.Not.Null);
        });
    }

    [Test]
    public void A_long_queue_is_read_a_page_at_a_time()
    {
        UseDeadLetters(SchemaDeadLettersPanel.PageSize + 5);
        var cut = Open();
        Button(cut, "Load dead letters").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel] tbody tr"), Has.Count.EqualTo(SchemaDeadLettersPanel.PageSize));
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-status").TextContent, Is.EqualTo("105 dead letters; showing the first 100."));
        });

        Button(cut, "Load more dead letters").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel] tbody tr"), Has.Count.EqualTo(SchemaDeadLettersPanel.PageSize + 5));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Load more dead letters"));
        });
    }

    [Test]
    public void An_empty_queue_says_so_and_offers_nothing_to_load()
    {
        UseEstate();

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("No dead letters"));
            Assert.That(Button(cut, "Load dead letters").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void A_count_that_fails_can_be_tried_again()
    {
        UseDeadLetters(2);
        Schema.Faults["CountDeadLetters"] = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__body").TextContent, Is.EqualTo("You are not permitted to count the dead letters.")));

        Schema.Faults.Remove("CountDeadLetters");
        cut.Find("[role=tabpanel] .lt-empty__actions button").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-schema-status").TextContent, Is.EqualTo("2 dead letters.")));
    }

    [Test]
    public void A_listing_that_fails_is_explained()
    {
        UseDeadLetters(2);
        Schema.Faults["ListDeadLetters"] = new NotSupportedException("x");
        var cut = Open();

        Button(cut, "Load dead letters").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__body").TextContent, Is.EqualTo(SchemaFailure.NotServed)));
    }

    [Test]
    public void A_caller_who_may_not_read_the_queue_is_told_so()
    {
        UseDeadLetters(2);
        Schema.Capabilities["orders"] = tree => FakeSchemaControl.ReadOnly(tree) with { CanViewDeadLetters = false };

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("You may not read this tree's dead letters"));
            Assert.That(Schema.CountOf("CountDeadLetters"), Is.Zero);
        });
    }

    [Test]
    public void A_long_key_is_clipped_in_its_cell()
    {
        var key = new string('k', SchemaDeadLettersPanel.KeyCharacters + 4);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaDeadLettersPanel.Clip(key), Is.EqualTo(new string('k', SchemaDeadLettersPanel.KeyCharacters) + "..."));
            Assert.That(SchemaDeadLettersPanel.Clip("short"), Is.EqualTo("short"));
        });
    }

    [Test]
    public void Below_the_small_breakpoint_each_dead_letter_is_a_two_line_row_whose_sheet_shows_the_value_as_text()
    {
        UseEstate();
        Schema.DeadLetters["orders"] = [SchemaTestData.DeadLetter("order/1", "<img src=x onerror=alert(1)>")];
        var cut = Open(LtBreakpoint.Compact);
        Button(cut, "Load dead letters").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel] table"), Is.Empty);
            var row = cut.Find("[role=tabpanel] li.lt-table-list__row");
            Assert.That(row.QuerySelector(".lt-compact-row__primary")!.TextContent.Trim(), Is.EqualTo("order/1"));
            Assert.That(row.QuerySelector(".lt-compact-row__secondary")!.TextContent.Trim(), Is.EqualTo("currency does not match"));
        });

        cut.Find("[role=tabpanel] li.lt-table-list__row button").Click();

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find("[role=dialog]");
            Assert.That(sheet.ClassList, Does.Contain("lt-dialog--end"));
            Assert.That(sheet.QuerySelector("pre")!.TextContent, Is.EqualTo("<img src=x onerror=alert(1)>"));
            Assert.That(sheet.QuerySelectorAll("img"), Is.Empty, "a value is text, never markup");
        });
    }
}
