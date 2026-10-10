using System.Text;
using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Data area's pure helpers: logical names from state ids, prefix bounds,
/// fault classification with fixed sentences, value renderers, formats and tabs.
/// </summary>
[TestFixture]
public sealed class DataHelperTests
{
    [TestCase(true, "acme", true)]
    [TestCase(true, "default", false)]
    [TestCase(true, null, false)]
    [TestCase(true, "", false)]
    [TestCase(false, "acme", false)]
    public void Sharing_applies_only_to_a_tenant_other_than_the_default_one(bool tenancy, string? tenant, bool applies)
    {
        // #3987: the default tenant takes no part in grants, so its listing offers no sharing.
        Assert.That(DataDirectory.SharingAppliesTo(tenancy, tenant), Is.EqualTo(applies));
    }

    [TestCase("orders", false, "orders", null)]
    [TestCase("a/crm/orders", false, "a/crm/orders", null)]
    [TestCase("t/acme/a/crm/orders", true, "a/crm/orders", "acme")]
    [TestCase("orders", true, "orders", "default")]
    [TestCase("t/acme/orders", false, "orders", null)]
    public void A_state_id_describes_as_its_logical_id_and_tenant(string stateId, bool tenancy, string logical, string? tenant)
    {
        Assert.That(DataTreeNames.TryDescribe(stateId, tenancy, out var actualLogical, out var actualTenant), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(actualLogical, Is.EqualTo(logical));
            Assert.That(actualTenant, Is.EqualTo(tenant));
        });
    }

    [TestCase("")]
    [TestCase(null)]
    [TestCase("_lattice_trees")]
    [TestCase("sys-app-registry")]
    [TestCase("t/acme/_lattice_x")]
    [TestCase("t//orders")]
    [TestCase("t/acme/")]
    public void Platform_internal_and_malformed_ids_are_never_described(string? stateId) =>
        Assert.That(DataTreeNames.TryDescribe(stateId, tenancyActive: true, out _, out _), Is.False);

    [TestCase("a/crm/orders", "crm")]
    [TestCase("a/crm/", null)]
    [TestCase("a//orders", null)]
    [TestCase("orders", null)]
    public void The_owning_app_is_the_slug_of_an_a_tree(string logical, string? slug) =>
        Assert.That(DataTreeNames.AppSlugOf(logical), Is.EqualTo(slug));

    [Test]
    public void A_prefix_bound_is_the_next_string_after_every_key_with_the_prefix()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataTreeNames.PrefixUpperBound("order/"), Is.EqualTo("order0"));
            Assert.That(DataTreeNames.PrefixUpperBound("a" + char.MaxValue), Is.EqualTo("b"));
            Assert.That(DataTreeNames.PrefixUpperBound(new string(char.MaxValue, 2)), Is.Null);
            Assert.Throws<ArgumentNullException>(() => DataTreeNames.PrefixUpperBound(null!));
        });
    }

    [TestCase("x\uD7FF", "x\U00010000")]
    [TestCase("x\U0001F3FF", "x\U0001F400")]
    [TestCase("x\U0001F3FF\uFFFF", "x\U0001F400")]
    [TestCase("x\U0001F600", "x\U0001F601")]
    [TestCase("x\U0010FFFF", "x\uE000")]
    public void A_prefix_bound_is_well_formed_so_it_survives_the_wire(string prefix, string expected)
    {
        // The state API carries strings as UTF-8, which replaces a lone surrogate: a bound
        // such as "x\uD83C\uE000" arrived as "x\uFFFD\uE000" and let keys outside the prefix in.
        var bound = DataTreeNames.PrefixUpperBound(prefix)!;
        var sent = Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(bound));

        Assert.Multiple(() =>
        {
            Assert.That(bound, Is.EqualTo(expected));
            Assert.That(sent, Is.EqualTo(bound), "the bound is changed on the wire");
            Assert.That(string.CompareOrdinal(prefix + "\uFFFF\uFFFF", sent), Is.LessThan(0), "a key with the prefix falls outside the range");
            Assert.That(string.CompareOrdinal(expected, sent), Is.GreaterThanOrEqualTo(0), "the first key past the prefix falls inside the range");
        });
    }

    [Test]
    public void Faults_are_classified_and_described_without_the_servers_words()
    {
        var denied = new LatticeStateApiException("Access denied t/acme/x") { IsPermissionDenied = true };
        var unimplemented = new LatticeStateApiException("no", new RpcException(new Status(StatusCode.Unimplemented, "t/acme")));
        var expired = new LatticeStateApiException("x", new RpcException(new Status(StatusCode.FailedPrecondition, "t/acme")));
        var badToken = new LatticeStateApiException("x", new RpcException(new Status(StatusCode.InvalidArgument, "t/acme")));
        var transient = new ShellTransportException("t/acme down", isTransient: true, new RpcException(Status.DefaultCancelled));

        Assert.Multiple(() =>
        {
            Assert.That(DataErrors.IsDenied(denied), Is.True);
            Assert.That(DataErrors.IsDenied(new UnauthorizedAccessException()), Is.True);
            Assert.That(DataErrors.IsNotOffered(unimplemented), Is.True);
            Assert.That(DataErrors.IsNotOffered(new NotSupportedException()), Is.True);
            Assert.That(DataErrors.IsCursorExpired(expired, resuming: false), Is.True);
            Assert.That(DataErrors.IsCursorExpired(new LatticeStateCursorExpiredException(), resuming: false), Is.True);
            Assert.That(DataErrors.IsCursorExpired(badToken, resuming: false), Is.False);
            Assert.That(DataErrors.IsCursorExpired(badToken, resuming: true), Is.True);
            Assert.That(DataErrors.IsCursorExpired(new ArgumentException("x"), resuming: true), Is.True);
            Assert.That(DataErrors.Describe(denied, "read this tree"), Is.EqualTo("You do not have permission to read this tree."));
            Assert.That(DataErrors.Describe(unimplemented, "follow changes"), Is.EqualTo("This cluster does not let you follow changes."));
            Assert.That(DataErrors.Describe(new KeyNotFoundException("t/acme/x"), "read it"), Is.EqualTo("It no longer exists, or you cannot see it."));
            Assert.That(DataErrors.Describe(transient, "read it"), Is.EqualTo("The cluster did not answer in time, so the Explorer could not read it. Try again."));
            Assert.That(
                DataErrors.Describe(
                    new ShellTransportException(
                        "retry",
                        isTransient: true,
                        new LatticeStateApiException(
                            "retry",
                            new RpcException(new Status(StatusCode.Unavailable, "The requested tree is being bootstrapped from a snapshot.")))),
                    "read this tree's keys"),
                Is.EqualTo("This tree is finishing a legacy in-place bootstrap; reads resume when it completes."));
            Assert.That(DataErrors.Describe(new InvalidOperationException("t/acme/x"), "read it"), Does.Not.Contain("t/acme"));
        });
    }

    [Test]
    public void A_text_preview_cut_inside_a_character_drops_it_rather_than_showing_a_replacement()
    {
        // "caf\u00e9" is 63 61 66 C3 A9; a preview of four bytes cuts the e-acute in half.
        var cut = "caf\u00e9"u8[..4].ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(DataValueRendering.Render(cut, true, DataValueRenderer.Text).Content, Is.EqualTo("caf"));
            Assert.That(DataValueRendering.Render(cut, true, DataValueRenderer.Json).Content, Is.EqualTo("caf"));
            Assert.That(DataValueRendering.Render(cut, false, DataValueRenderer.Text).Content, Is.EqualTo("caf\uFFFD"), "A whole value ending mid-character really is invalid.");
            Assert.That(DataValueRendering.Render([0xFF, 0x61, 0xC3], true, DataValueRenderer.Text).Content, Is.EqualTo("\uFFFDa"), "Invalid bytes before the cut are still replaced.");
        });
    }

    [Test]
    public void Each_renderer_reads_the_bytes_its_own_way_and_notes_a_preview()
    {
        var json = Encoding.UTF8.GetBytes("{\"a\":1}");
        var members = new[] { new DataCrdtMember { ElementText = "x", ElementFormat = ValueFormat.Text, ReplicaId = "r", Ordinal = 2 } };

        Assert.Multiple(() =>
        {
            Assert.That(DataValueRendering.Render(json, false, DataValueRenderer.Auto).FormatText, Is.EqualTo("JSON"));
            Assert.That(DataValueRendering.Render(json, false, DataValueRenderer.Text).Content, Is.EqualTo("{\"a\":1}"));
            Assert.That(DataValueRendering.Render(json, false, DataValueRenderer.Hex).Content, Does.StartWith("00000000  7b"));
            Assert.That(DataValueRendering.Render([0xff], false, DataValueRenderer.Json).Note, Does.Contain("not valid JSON"));
            Assert.That(DataValueRendering.Render(json, false, DataValueRenderer.Members, members).Content, Is.EqualTo("x  (replica r, #2)\n"));
            Assert.That(DataValueRendering.Render(json, true, DataValueRenderer.Hex).Note, Does.StartWith("Preview only"));
            Assert.That(DataValueRendering.Offered(hasMembers: true)[0], Is.EqualTo(DataValueRenderer.Members));
            Assert.That(DataValueRendering.Offered(hasMembers: false), Has.None.EqualTo(DataValueRenderer.Members));
            Assert.That(DataValueRendering.Label(DataValueRenderer.Text), Is.EqualTo("UTF-8 text"));
            Assert.That(DataValueRendering.Inline(Encoding.UTF8.GetBytes("{ \"a\" : 1 }"), false), Is.EqualTo("{\"a\":1}"));
            Assert.That(DataValueRendering.Inline(Encoding.UTF8.GetBytes(new string('x', 300)), false), Has.Length.EqualTo(160).And.EndsWith("..."));
            Assert.That(DataValueRendering.Size(512), Is.EqualTo("512 B"));
            Assert.That(DataValueRendering.Size(12_700), Is.EqualTo("12.4 KiB"));
            Assert.That(DataValueRendering.Size(3 * 1024 * 1024), Is.EqualTo("3 MiB"));
        });
    }

    [TestCase(100, 160, true)]
    [TestCase(81, 160, true)]
    [TestCase(80, 160, false)]
    [TestCase(10, 160, false)]
    [TestCase(30, 40, true)]
    public void An_inline_hex_preview_marks_the_bytes_it_leaves_out(int length, int maximum, bool leavesSomeOut)
    {
        // #4354: two hex digits per byte filled the cell exactly, so a longer binary
        // value read as if the visible bytes were all of it.
        var bytes = Enumerable.Range(0, length).Select(i => (byte)(i % 2 == 0 ? 0x00 : 0xff)).ToArray();

        var inline = DataValueRendering.Inline(bytes, truncated: false, maximum);

        Assert.Multiple(() =>
        {
            Assert.That(inline, Has.Length.AtMost(maximum));
            Assert.That(inline.EndsWith("...", StringComparison.Ordinal), Is.EqualTo(leavesSomeOut));
            Assert.That(inline, Does.StartWith(leavesSomeOut ? "00ff" : Convert.ToHexString(bytes).ToLowerInvariant()));
        });
    }

    [Test]
    public void An_inline_preview_of_a_truncated_value_says_the_value_continues()
    {
        var binary = Enumerable.Range(0, 20).Select(i => (byte)(i % 2 == 0 ? 0x00 : 0xff)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(DataValueRendering.Inline(Encoding.UTF8.GetBytes("short text"), truncated: true), Is.EqualTo("short text..."));
            Assert.That(DataValueRendering.Inline(binary, truncated: true), Is.EqualTo(Convert.ToHexString(binary).ToLowerInvariant() + "..."));
            Assert.That(DataValueRendering.Inline(Encoding.UTF8.GetBytes(new string('x', 300)), truncated: true), Has.Length.EqualTo(160).And.EndsWith("..."));
            Assert.That(DataValueRendering.Inline(Encoding.UTF8.GetBytes("short text"), truncated: false), Is.EqualTo("short text"));
        });
    }

    [Test]
    public void A_clipped_key_or_preview_never_splits_a_character_in_two()
    {
        // An emoji is a surrogate pair; a cut between its halves left a lone surrogate,
        // which the page can only draw as the replacement character.
        const string Emoji = "\U0001F600";
        var value = new string('x', 156) + Emoji + new string('y', 50);
        var key = new string('k', DataKeysPanel.KeyClip - 4) + Emoji + "tail";

        Assert.Multiple(() =>
        {
            Assert.That(DataValueRendering.Inline(Encoding.UTF8.GetBytes(value), truncated: false), Is.EqualTo(new string('x', 156) + "..."));
            Assert.That(DataKeysPanel.Clip(key), Is.EqualTo(new string('k', DataKeysPanel.KeyClip - 4) + "..."));
            Assert.That(DataKeysPanel.Clip(new string('k', DataKeysPanel.KeyClip - 5) + Emoji + "tail"), Is.EqualTo(new string('k', DataKeysPanel.KeyClip - 5) + Emoji + "..."));
        });
    }

    [TestCase(1023L, "1,023 B")]
    [TestCase(1024L, "1 KiB")]
    [TestCase(1_048_524L, "1023.9 KiB")]
    [TestCase(1_048_525L, "1 MiB")]
    [TestCase(1_048_575L, "1 MiB")]
    [TestCase(1_048_576L, "1 MiB")]
    public void A_size_just_under_a_unit_boundary_reads_in_the_next_unit(long bytes, string expected) =>
        // #4355: 1,048,575 bytes read "1024 KiB".
        Assert.That(DataValueRendering.Size(bytes), Is.EqualTo(expected));

    [Test]
    public void Json_renderers_show_text_as_written_rather_than_escaped()
    {
        // #4325: the forced JSON view and the inline preview escaped every non-ASCII and < > & ' + character.
        const string Text = "caf\u00e9 <b> & it's +44 \u4e2d\u6587";
        var bytes = Encoding.UTF8.GetBytes("{ \"name\" : \"" + Text + "\" }");

        Assert.Multiple(() =>
        {
            Assert.That(DataValueRendering.Render(bytes, false, DataValueRenderer.Json).Content, Does.Contain("\"name\": \"" + Text + "\""));
            Assert.That(DataValueRendering.Render(bytes, false, DataValueRenderer.Auto).Content, Does.Contain("\"name\": \"" + Text + "\""));
            Assert.That(DataValueRendering.Inline(bytes, false), Is.EqualTo("{\"name\":\"" + Text + "\"}"));
        });
    }

    [Test]
    public void Formats_and_tabs_are_fixed_and_culture_invariant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataFormat.Time(new HybridLogicalClock { WallClockTicks = new DateTimeOffset(2026, 9, 28, 14, 2, 11, TimeSpan.Zero).UtcTicks }), Is.EqualTo("2026-09-28 14:02:11 UTC"));
            Assert.That(DataFormat.Time(default(HybridLogicalClock)), Is.EqualTo("-"));
            Assert.That(DataFormat.Count(1204), Is.EqualTo("1,204"));
            Assert.That(DataFormat.TryParseInstant("2026-09-28 14:00", out var instant), Is.True);
            Assert.That(DataFormat.Instant(instant), Is.EqualTo("2026-09-28T14:00:00Z"));
            Assert.That(DataFormat.TryParseInstant("soon", out _), Is.False);
            Assert.That(DataTabs.Parse("history"), Is.EqualTo(DataTabs.History));
            Assert.That(DataTabs.Parse("nonsense"), Is.EqualTo(DataTabs.Keys));
            Assert.That(DataTabs.All.Select(tab => tab.Id), Is.EqualTo(new[] { "keys", "history", "metrics", "dead-letters", "tag-indexes", "views" }));
        });
    }
}
