using System.Text;
using Bunit;
using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Keys tab: paging, the prefix and tag filters, page size and scan mode,
/// the entry below the table with every value renderer, long keys and values,
/// live updates, cursor expiry, a restricted identity and the compact form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataKeysPanelTests : DataTestContext
{
    [Test]
    public void Keys_page_forward_and_back_with_the_range_in_the_toolbar()
    {
        Client.WithTree("orders", keys: 60);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(25));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("1-25"));
            Assert.That(Button(cut, "Previous page").HasAttribute("disabled"), Is.True);
        });

        Button(cut, "Next page").Click();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("26-50"));
            Assert.That(Rows(cut)[0].QuerySelector("th")!.TextContent, Is.EqualTo("key/0025"));
            Assert.That(cut.Find(".lt-data-pager__position").TextContent, Is.EqualTo("Page 2"));
        });

        Button(cut, "Previous page").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("1-25")));
    }

    [Test]
    public void The_prefix_comes_from_the_address_and_submitting_one_puts_it_there()
    {
        Client.WithTree("orders", keys: 5, prefix: "order/2026-09/");
        Client.Entries["orders"]["customer/1"] = FakeStateClient.Entry("customer/1", "x");
        var cut = RenderAt("data/orders?prefix=order%2F");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(5));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("1-5 under prefix"));
        });

        var input = cut.Find("input[type=search]");
        input.Input("customer/");
        input.KeyDown("Enter");

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentRelative, Is.EqualTo("/data/orders?prefix=customer%2F"));
            Assert.That(Rows(cut), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Page_size_and_scan_mode_restart_the_scan_and_a_snapshot_turns_live_updates_off()
    {
        Client.WithTree("orders", keys: 60);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(25)));

        Control(cut, "Page size").Change("50");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(50)));

        Control(cut, "Scan").Change("Snapshot");
        cut.WaitUntil(() =>
        {
            Assert.That(Switch(cut, "Live updates").HasAttribute("disabled"), Is.True);
            Assert.That(Client.OpenFeeds, Is.EqualTo(0));
        });
    }

    [Test]
    public void The_selected_key_is_the_current_row_and_its_entry_opens_below()
    {
        Client.WithTree("orders", keys: 3);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(3)));
        Navigation.NavigateTo(cut.Find("a.lt-data-key").GetAttribute("href")!);

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut)[0].GetAttribute("aria-current"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-data-entry__key").TextContent, Is.EqualTo("key/0000"));
            Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("{\n  \"n\": 0\n}").Or.EqualTo("{\r\n  \"n\": 0\r\n}"));
            Assert.That(cut.Find("a.lt-btn[href*='tab=history']").TextContent, Is.EqualTo("History of this key"));
        });
    }

    [Test]
    public void At_the_expanded_width_an_open_entry_stands_beside_the_key_table()
    {
        // #3987: the entry rendered below the whole table, so opening a key meant
        // scrolling past the list to read it.
        Client.WithTree("orders");
        Client.Entries["orders"]["doc"] = FakeStateClient.Entry("doc", "{\"a\":1}");

        var cut = RenderAt("data/orders?key=doc");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-split__detail .lt-data-entry"), Has.Count.EqualTo(1)));
        var split = cut.Find(".lt-data-split");
        Assert.Multiple(() =>
        {
            Assert.That(split.Children.Select(child => child.ClassName), Is.EqualTo(new[] { "lt-data-split__list", "lt-data-split__detail" }));
            Assert.That(split.QuerySelector(".lt-data-split__list table"), Is.Not.Null, "the key table is the list side");
            Assert.That(split.QuerySelector(".lt-data-split__list .lt-data-pager"), Is.Not.Null);
        });
    }

    [Test]
    public void Without_an_open_entry_there_is_no_split()
    {
        Client.WithTree("orders");
        Client.Entries["orders"]["doc"] = FakeStateClient.Entry("doc", "{\"a\":1}");

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll("table"), Has.Count.EqualTo(1)));
        Assert.That(cut.FindAll(".lt-data-split, .lt-data-entry"), Is.Empty);
    }

    [Test]
    public void At_the_compact_width_the_entry_stays_below_the_list()
    {
        Client.WithTree("orders");
        Client.Entries["orders"]["doc"] = FakeStateClient.Entry("doc", "{\"a\":1}");

        var cut = RenderAt("data/orders?key=doc", compact: true);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-entry"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-data-split, .lt-data-split__detail"), Is.Empty);
            var list = cut.Find(".lt-table-list");
            var entry = cut.Find(".lt-data-entry");
            Assert.That(list.CompareDocumentPosition(entry).HasFlag(AngleSharp.Dom.DocumentPositions.Following), Is.True, "stacked, the entry follows the list");
        });
    }

    [Test]
    public void Each_value_renderer_draws_the_same_bytes_its_own_way()
    {
        Client.WithTree("orders");
        Client.Entries["orders"]["doc"] = FakeStateClient.Entry("doc", "{\"a\":1}");
        var cut = RenderAt("data/orders?key=doc");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").GetAttribute("aria-label"), Is.EqualTo("Value, as JSON")));

        Control(cut, "Show value as").Change("Text");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("{\"a\":1}")));

        Control(cut, "Show value as").Change("Hex");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Does.StartWith("00000000  7b 22 61 22")));

        Control(cut, "Show value as").Change("Json");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").TextContent, Does.Contain("\"a\": 1")));
    }

    [Test]
    public void A_value_that_is_not_json_says_so_when_forced_and_binary_renders_as_hex_automatically()
    {
        Client.WithTree("orders");
        Client.Entries["orders"]["blob"] = FakeStateClient.Entry("blob", new byte[] { 0x00, 0xff, 0x10 });
        var cut = RenderAt("data/orders?key=blob");
        cut.WaitUntil(() => Assert.That(cut.Find("pre.lt-data-value").GetAttribute("aria-label"), Is.EqualTo("Value, as Hex")));

        Control(cut, "Show value as").Change("Json");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry .lt-data-note").TextContent, Does.Contain("not valid JSON")));
    }

    [Test]
    public void Crdt_members_are_offered_and_chosen_first_when_the_state_api_decoded_them()
    {
        Client.WithTree("sets");
        Client.Entries["sets"]["tags"] = FakeStateClient.Entry("tags", "ignored") with
        {
            CrdtShape = "OrSet",
            CurrentMembers =
            [
                new CrdtMemberValue { Element = Encoding.UTF8.GetBytes("red"), ReplicaId = "r1", Ordinal = 3 },
                new CrdtMemberValue { Element = Encoding.UTF8.GetBytes("blue"), ReplicaId = "r2", Ordinal = 1 },
            ],
        };

        var cut = RenderAt("data/sets?key=tags");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("pre.lt-data-value").TextContent, Does.Contain("red  (replica r1, #3)").And.Contain("blue  (replica r2, #1)"));
            Assert.That(Rows(cut)[0].TextContent, Does.Contain("OrSet"));
        });
    }

    [Test]
    public void Long_keys_and_values_are_clipped_with_an_explicit_expansion()
    {
        var key = "k/" + new string('x', 200);
        var value = new string('v', 10_000);
        Client.WithTree("big");
        Client.Entries["big"][key] = FakeStateClient.Entry(key, value);
        var cut = RenderAt("data/big?key=" + Uri.EscapeDataString(key));
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry__key").TextContent, Has.Length.EqualTo(96).And.EndsWith("..."));
            Assert.That(cut.Find("pre.lt-data-value").TextContent, Has.Length.LessThan(4100));
        });

        Button(cut, "Show the whole key (202 characters)").Click();
        Button(cut, "Show all 10,000 characters").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-entry__key").TextContent, Is.EqualTo(key));
            Assert.That(cut.Find("pre.lt-data-value").TextContent, Has.Length.EqualTo(10_000));
        });
    }

    [Test]
    public void A_key_with_no_entry_says_so()
    {
        Client.WithTree("orders", keys: 1);

        var cut = RenderAt("data/orders?key=missing");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry .lt-empty h3").TextContent, Is.EqualTo("No entry at this key")));
    }

    [Test]
    public void A_live_change_on_the_first_page_reloads_it_and_reloads_the_open_entry()
    {
        Client.WithTree("orders", keys: 2);
        var cut = RenderAt("data/orders?key=key%2F0000");
        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
        });
        Assert.That(Switch(cut, "Live updates").GetAttribute("aria-checked"), Is.EqualTo("true"));

        Client.Entries["orders"]["key/0000"] = FakeStateClient.Entry("key/0000", "\"changed\"");
        Client.Entries["orders"]["key/0005"] = FakeStateClient.Entry("key/0005", "\"new\"");
        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("orders", "key/0000"));

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(3));
            Assert.That(cut.Find("pre.lt-data-value").TextContent, Is.EqualTo("\"changed\""));
        });
    }

    [Test]
    public async Task Live_changes_do_not_restart_a_first_page_scan_during_failure_backoff()
    {
        Client.WithTree("orders", keys: 2);
        Client.Fault = call => call == nameof(ILatticeStateClient.ScanEntriesAsync)
            ? new LatticeStateApiException("unavailable", new RpcException(new Status(StatusCode.Unavailable, "down"))) { IsTransient = true }
            : null;

        var cut = RenderAt("data/orders");
        cut.WaitUntil(() =>
        {
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(cut.Markup, Does.Contain("The cluster did not answer in time"));
        });
        var initialScans = Client.Calls.Count(call => call == "ScanEntriesAsync:first");
        Assert.That(initialScans, Is.EqualTo(1));

        for (var i = 0; i < 20; i++)
        {
            Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("orders", $"key/{i:D4}"));
        }

        await Task.Delay(TimeSpan.FromMilliseconds(100));

        Assert.That(Client.Calls.Count(call => call == "ScanEntriesAsync:first"), Is.EqualTo(initialScans));
    }

    [Test]
    public void A_live_change_seen_from_a_later_page_offers_a_reload_from_the_first()
    {
        Client.WithTree("orders", keys: 30);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        Button(cut, "Next page").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-pager__position").TextContent, Is.EqualTo("Page 2")));

        Client.Feeds[^1].Writer.TryWrite(FakeStateClient.Change("orders", "key/0001"));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-note").TextContent, Does.Contain("have changed since this page loaded")));

        Button(cut, "Reload from the first page").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-pager__position").TextContent, Is.EqualTo("Page 1"));
            Assert.That(cut.FindAll(".lt-data-note"), Is.Empty);
        });
    }

    [Test]
    public void Live_updates_follow_only_the_prefix_and_turning_them_off_closes_the_feed()
    {
        Client.WithTree("orders", keys: 2);
        var cut = RenderAt("data/orders?prefix=key%2F");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(Client.ObserveRequests[^1].StartInclusive, Is.EqualTo("key/"));
            Assert.That(Client.ObserveRequests[^1].EndExclusive, Is.EqualTo("key0"));
            Assert.That(Client.ObserveRequests[^1].TreeId, Is.EqualTo("orders"));
        });

        Switch(cut, "Live updates").Click();

        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(0)));
    }

    [Test]
    public void A_cluster_without_a_change_feed_hides_live_updates_and_says_why()
    {
        Client.WithTree("orders", keys: 2);
        Client.Fault = call => call == nameof(ILatticeStateClient.ObserveChangesAsync)
            ? new LatticeStateApiException("The endpoint does not expose the Lattice state API.", new RpcException(new Status(StatusCode.Unimplemented, "no")))
            : null;

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-note").TextContent, Is.EqualTo("Live updates are not available on this cluster."));
            Assert.That(cut.FindAll("button[role=switch]"), Is.Empty);
        });
    }

    [Test]
    public void An_expired_change_feed_position_offers_to_restart_live_updates()
    {
        Client.WithTree("orders", keys: 2);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.OpenFeeds, Is.EqualTo(1)));

        Client.Feeds[^1].Writer.TryComplete(new LatticeStateApiException("expired", new RpcException(new Status(StatusCode.FailedPrecondition, "trimmed"))));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-note").TextContent, Does.Contain("change feed moved past this position")));

        Button(cut, "Restart live updates").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Client.OpenFeeds, Is.EqualTo(1));
            Assert.That(cut.FindAll(".lt-data-note"), Is.Empty);
        });
    }

    [Test]
    public void An_expired_scan_cursor_offers_a_restart_from_the_first_page()
    {
        Client.WithTree("orders", keys: 60);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(25)));
        Client.ContinuationFault = new LatticeStateApiException("The state-API call failed (InvalidArgument).", new RpcException(new Status(StatusCode.InvalidArgument, "token t/acme/orders expired")));

        Button(cut, "Next page").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("This scan's cursor has expired")));
        Assert.That(cut.Markup, Does.Not.Contain("t/acme"));

        Client.ContinuationFault = null;
        Button(cut, "Restart from the first page").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(25));
            Assert.That(cut.Find(".lt-data-pager__position").TextContent, Is.EqualTo("Page 1"));
        });
    }

    [Test]
    public void A_restricted_identity_is_told_it_may_not_read_the_keys()
    {
        Client.WithTree("orders", keys: 2);
        Client.Fault = call => call == nameof(ILatticeStateClient.ScanEntriesAsync)
            ? new LatticeStateApiException("Access to the state API was denied.") { IsPermissionDenied = true }
            : null;

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("You do not have permission to read this tree's keys.")));
    }

    [Test]
    public void The_tag_filter_narrows_to_the_tagged_keys_and_disables_the_prefix()
    {
        Client.WithTree("orders", keys: 5);
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["orders"];
        Client.Members[("by-region", "eu")] = [new TagMember { TreeId = "orders", Key = "key/0001" }, new TagMember { TreeId = "orders", Key = "key/0003" }];
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(5)));

        Control(cut, "Tag index").Change("by-region");
        cut.WaitUntil(() => Assert.That(CurrentRelative, Is.EqualTo("/data/orders?index=by-region")));
        cut.WaitUntil(() => Assert.That(cut.FindAll("label").Any(label => label.TextContent.Trim() == "Tag"), Is.True));
        Control(cut, "Tag").Change("eu");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut).Select(row => row.QuerySelector("th")!.TextContent), Is.EqualTo(new[] { "key/0001", "key/0003" }));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("1-2 tagged eu"));
            Assert.That(cut.Find("input[type=search]").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void Compact_rows_show_the_key_and_value_and_the_sheet_opens_the_entry()
    {
        Client.WithTree("orders", keys: 2);

        var cut = RenderAt("data/orders", compact: true);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.FindAll(".lt-compact-row__primary").Select(line => line.TextContent), Is.EqualTo(new[] { "key/0000", "key/0001" }));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent.Trim(), Is.EqualTo("{\"n\":0}"));
        });

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find(".lt-dialog");
            Assert.That(sheet.QuerySelector("h2")!.TextContent, Is.EqualTo("key/0000"));
            Assert.That(sheet.QuerySelector("a.lt-btn")!.GetAttribute("href"), Is.EqualTo("data/orders?key=key%2F0000"));
        });
    }
}
