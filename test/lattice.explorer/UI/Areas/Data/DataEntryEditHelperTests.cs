using System.Text;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The value editor's helpers (#5367): the draft codec, the schema preview, the
/// entry version and the entry commands' publication rules.
/// </summary>
[TestFixture]
public sealed class DataEntryEditHelperTests
{
    [TestCase("{\"a\":1}", "Json")]
    [TestCase("[1,2]", "Json")]
    [TestCase("plain text", "Text")]
    [TestCase("", "Text")]
    public void Detect_picks_json_for_a_document_and_text_for_other_utf8(string value, string expected) =>
        Assert.That(DataDraft.Detect(Encoding.UTF8.GetBytes(value)).ToString(), Is.EqualTo(expected));

    [Test]
    public void Detect_picks_base64_for_bytes_that_are_not_utf8() =>
        Assert.That(DataDraft.Detect([0xFF, 0xFE, 0x00]), Is.EqualTo(DataDraftFormat.Base64));

    [Test]
    public void ToText_pretty_prints_json_and_round_trips_base64()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataDraft.ToText("{\"a\":1}"u8, DataDraftFormat.Json)!.ReplaceLineEndings("\n"), Is.EqualTo("{\n  \"a\": 1\n}"));
            Assert.That(DataDraft.ToText("{\"a\":1}"u8, DataDraftFormat.Text), Is.EqualTo("{\"a\":1}"));
            Assert.That(DataDraft.ToText([1, 2], DataDraftFormat.Base64), Is.EqualTo("AQI="));
            Assert.That(DataDraft.ToText([0xFF], DataDraftFormat.Text), Is.Null);
        });
    }

    [Test]
    public void TryEncode_validates_json_and_base64_and_stores_text_as_typed()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataDraft.TryEncode("{\"a\":", DataDraftFormat.Json, out _, out var jsonError), Is.False);
            Assert.That(jsonError, Does.StartWith("This is not valid JSON"));
            Assert.That(DataDraft.TryEncode("not base64!", DataDraftFormat.Base64, out _, out var base64Error), Is.False);
            Assert.That(base64Error, Does.Contain("not valid base64"));
            Assert.That(DataDraft.TryEncode(" AQI= ", DataDraftFormat.Base64, out var bytes, out _), Is.True);
            Assert.That(bytes, Is.EqualTo(new byte[] { 1, 2 }));
            Assert.That(DataDraft.TryEncode(null, DataDraftFormat.Text, out var empty, out var none), Is.True);
            Assert.That(empty, Is.Empty);
            Assert.That(none, Is.Null);
            Assert.That(DataDraft.TryEncode(" {\"a\": 1} ", DataDraftFormat.Json, out var json, out _), Is.True);
            Assert.That(Encoding.UTF8.GetString(json), Is.EqualTo(" {\"a\": 1} "));
        });
    }

    [Test]
    public void Pretty_leaves_text_that_is_not_json_alone_and_labels_name_each_format()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataDraft.Pretty("nope"), Is.EqualTo("nope"));
            Assert.That(DataDraft.Label(DataDraftFormat.Json), Is.EqualTo("JSON"));
            Assert.That(DataDraft.Label(DataDraftFormat.Text), Is.EqualTo("Text (UTF-8)"));
            Assert.That(DataDraft.Label(DataDraftFormat.Base64), Is.EqualTo("Binary (base64)"));
            Assert.That(() => DataDraft.IsJson(null!, out _), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_schema_check_reports_every_failing_rule_in_order()
    {
        var check = DataSchemaCheck.From(new LatticeSchemaPolicy([LatticeSchemaRule.Json(), LatticeSchemaRule.MaxLength(3)]));

        Assert.Multiple(() =>
        {
            Assert.That(check.HasPolicy, Is.True);
            Assert.That(check.RuleCount, Is.EqualTo(2));
            Assert.That(check.Check(Encoding.UTF8.GetBytes("nope!")), Has.Count.EqualTo(2));
            Assert.That(check.Check(Encoding.UTF8.GetBytes("[1]")), Is.Empty);
            Assert.That(check.Note, Is.Null);
        });
    }

    [Test]
    public void No_policy_or_an_empty_one_previews_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataSchemaCheck.From(null).HasPolicy, Is.False);
            Assert.That(DataSchemaCheck.From(new LatticeSchemaPolicy([])).HasPolicy, Is.False);
            Assert.That(DataSchemaCheck.NotRead.Check([1]), Is.Empty);
        });
    }

    [Test]
    public void The_version_tells_a_live_key_from_a_deleted_or_absent_one()
    {
        var live = new Orleans.Lattice.Explorer.Core.Data.DataEntry { Key = "k", Hlc = new HybridLogicalClock { WallClockTicks = 5 } };

        Assert.Multiple(() =>
        {
            Assert.That(DataEntryVersion.Of(null), Is.EqualTo(default(DataEntryVersion)));
            Assert.That(DataEntryVersion.Of(live).Exists, Is.True);
            Assert.That(DataEntryVersion.Of(live with { IsTombstone = true }).Exists, Is.False);
            Assert.That(DataEntryVersion.Of(live), Is.Not.EqualTo(DataEntryVersion.Of(live with { Hlc = new HybridLogicalClock { WallClockTicks = 6 } })));
            Assert.That(DataEntryVersion.Body([1, 2]), Is.EqualTo(new byte[] { 1, 2 }));
        });
    }

    [Test]
    public void Entry_commands_follow_what_is_published_and_withdraw_with_it()
    {
        var commands = new DataEntryCommands();
        var tree = new DataTreeEntry { LogicalId = "orders", StateId = "orders", Kind = DataTreeKind.Tree };

        Assert.That(commands.Commands, Is.Empty);

        commands.ShowTree(tree, new DataWriteRights(true, false));
        commands.ShowEntry("k", new DataWriteRights(true, true));
        Assert.That(commands.Commands.Select(command => command.Id), Is.EqualTo(new[] { DataEntryCommands.NewEntryId, DataEntryCommands.EditEntryId, DataEntryCommands.DeleteEntryId }));
        Assert.That(commands.Commands[1].Target, Is.EqualTo(tree.Address.WithQuery(ExplorerAddress.KeyQuery, "k")));

        commands.HideEntry("other");
        Assert.That(commands.Commands, Has.Count.EqualTo(3));
        commands.HideEntry("k");
        Assert.That(commands.Commands.Select(command => command.Id), Is.EqualTo(new[] { DataEntryCommands.NewEntryId }));
        commands.HideTree(tree);
        Assert.That(commands.Commands, Is.Empty);
    }

    [Test]
    public async Task An_unheard_request_is_held_until_a_control_takes_it()
    {
        var commands = new DataEntryCommands();
        await commands.RequestAsync(DataEntryCommands.EditEntryId);

        Assert.Multiple(() =>
        {
            Assert.That(commands.TryTake(DataEntryCommands.DeleteEntryId), Is.False);
            Assert.That(commands.TryTake(DataEntryCommands.EditEntryId), Is.True);
            Assert.That(commands.TryTake(DataEntryCommands.EditEntryId), Is.False);
        });

        string? heard = null;
        commands.Requested += id => heard = id;
        await commands.RequestAsync(DataEntryCommands.NewEntryId);
        Assert.Multiple(() =>
        {
            Assert.That(heard, Is.EqualTo(DataEntryCommands.NewEntryId));
            Assert.That(commands.TryTake(DataEntryCommands.NewEntryId), Is.False);
        });
    }

    [Test]
    public void Rights_report_whether_anything_is_drawn()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataWriteRights.None.Any, Is.False);
            Assert.That(new DataWriteRights(false, true).Any, Is.True);
        });
    }
}
