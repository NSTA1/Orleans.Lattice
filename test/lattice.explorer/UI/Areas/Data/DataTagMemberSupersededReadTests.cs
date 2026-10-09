using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>Tag-member pages and faults belong only to the selected index and tag.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTagMemberSupersededReadTests : DataTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public void A_superseded_member_reply_or_fault_cannot_change_the_current_tag(bool fails)
    {
        Client.WithTree("orders");
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["orders"];
        Client.Members[("by-region", "eu")] = [];
        Client.Members[("by-region", "us")] = [];
        var previous = new TaskCompletionSource<TagMemberScanPage>();
        CancellationToken previousToken = default;
        Client.TagMemberRead = (request, token) =>
        {
            if (request.Tag == "eu")
            {
                previousToken = token;
                return previous.Task;
            }

            return Task.FromResult(Members("current-key"));
        };

        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region&tag=eu");
        var panel = cut.FindComponent<DataTagIndexesPanel>();
        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=by-region&tag=us");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry tbody").TextContent, Does.Contain("current-key")));
        Assert.That(cut.FindComponent<DataTagIndexesPanel>().Instance, Is.SameAs(panel.Instance));
        var renders = panel.RenderCount;

        if (fails)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(Members("stale-key", "next"));
        }

        panel.WaitForState(() => panel.RenderCount > renders);
        Assert.Multiple(() =>
        {
            Assert.That(previousToken.IsCancellationRequested, Is.True);
            Assert.That(cut.Find(".lt-data-entry tbody").TextContent, Does.Contain("current-key").And.Not.Contain("stale-key"));
            Assert.That(cut.FindAll(".lt-data-entry [role=alert]"), Is.Empty);
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Load more keys"));
        });
    }

    [Test]
    public void A_superseded_member_reply_cannot_enable_paging_while_the_current_page_is_pending()
    {
        Client.WithTree("orders");
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["orders"];
        var previous = new TaskCompletionSource<TagMemberScanPage>();
        var currentPage = new TaskCompletionSource<TagMemberScanPage>();
        Client.TagMemberRead = (request, _) => request.Tag == "eu"
            ? previous.Task
            : request.PageToken is null ? Task.FromResult(Members("current-key", "next")) : currentPage.Task;
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region&tag=eu");
        var panel = cut.FindComponent<DataTagIndexesPanel>();
        Navigation.NavigateTo("data/orders?tab=tag-indexes&index=by-region&tag=us");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry tbody").TextContent, Does.Contain("current-key")));
        Button(cut, "Load more keys").Click();
        Assert.That(Button(cut, "Load more keys").HasAttribute("disabled"), Is.True);
        var renders = panel.RenderCount;

        previous.SetResult(Members("stale-key"));

        panel.WaitForState(() => panel.RenderCount > renders);
        Assert.That(Button(cut, "Load more keys").HasAttribute("disabled"), Is.True);
        currentPage.SetResult(Members("next-key"));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry tbody").TextContent,
            Does.Contain("current-key").And.Contain("next-key").And.Not.Contain("stale-key")));
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public void Superseded_index_metadata_cannot_restart_the_current_member_read(bool switchIndex, bool fails)
    {
        Client.WithTree("orders");
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-country", TreeId = "tag-by-country" });
        Client.Covered["by-region"] = ["orders"];
        Client.Covered["by-country"] = ["orders"];
        var previous = new TaskCompletionSource<CoveredTreeCatalogPage>();
        var reads = 0;
        Client.CoveredTreeRead = (_, _) => ++reads == 1
            ? previous.Task
            : Task.FromResult(new CoveredTreeCatalogPage { Entries = ["orders"] });
        Client.TagMemberRead = (_, _) => Task.FromResult(Members("current-key"));
        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region&tag=eu");
        var panel = cut.FindComponent<DataTagIndexesPanel>();
        var index = switchIndex ? "by-country" : "by-region";
        Navigation.NavigateTo($"data/orders?tab=tag-indexes&index={index}&tag=us");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry tbody").TextContent, Does.Contain("current-key")));
        var memberReads = Client.Calls.Count(call => call == nameof(FakeStateClient.ScanTagMembersAsync));
        var renders = panel.RenderCount;

        if (fails)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(new CoveredTreeCatalogPage { Entries = ["obsolete-tree"] });
        }

        panel.WaitForState(() => panel.RenderCount > renders);
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-data-entry tbody").TextContent, Does.Contain("current-key"));
            Assert.That(Client.Calls.Count(call => call == nameof(FakeStateClient.ScanTagMembersAsync)), Is.EqualTo(memberReads));
        });
    }

    private static TagMemberScanPage Members(string key, string? next = null) => new()
    {
        Entries = [new TagMember { TreeId = "orders", Key = key }],
        NextPageToken = next,
    };
}
