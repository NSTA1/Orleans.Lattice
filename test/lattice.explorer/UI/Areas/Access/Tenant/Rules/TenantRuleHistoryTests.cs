using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Design;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>Rule history's loading, empty, paging and component-lifetime boundaries.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantRuleHistoryTests : ShellDesignTestContext
{
    private readonly ILatticeStateClient _client = Substitute.For<ILatticeStateClient>();
    private static readonly TenantRuleView Rule = new()
    {
        RuleId = "read-orders",
        ScopeKind = TenantRuleScopeKind.Tree,
        TreeName = "orders",
    };
    private static readonly EntryHistoryResponse EmptyPage = new()
    {
        TreeId = LatticeAuthReservedTrees.PolicyTreeId,
        Key = "t/acme/orders\u001ftenant:acme:read-orders",
    };

    [Test]
    public void History_with_no_retained_revisions_says_so()
    {
        Serve(Task.FromResult(EmptyPage));

        var cut = RenderHistory();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-rule-history]").GetAttribute("data-lt-rule-history"), Is.EqualTo("loaded"));
            Assert.That(cut.Find(".lt-access-caveat").TextContent, Is.EqualTo("No revision of this rule is retained."));
            Assert.That(cut.FindAll("ol, button"), Is.Empty);
        });
    }

    [Test]
    public async Task History_pages_oldest_first_but_renders_saved_and_removed_revisions_newest_first()
    {
        var second = new TaskCompletionSource<EntryHistoryResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Serve(Task.FromResult(EmptyPage with
        {
            Revisions = [Revision(14, 0, HistoryRowKind.Set, null)],
            ContinuationToken = "next-page",
        }), second.Task);
        var cut = RenderHistory();
        var more = cut.Find("button");
        Assert.That(more.TextContent, Is.EqualTo("Load more revisions"));

        var loading = more.ClickAsync(new());
        cut.WaitUntil(() => Assert.That(cut.Find("button").HasAttribute("disabled"), Is.True));
        second.SetResult(EmptyPage with
        {
            Revisions =
            [
                Revision(14, 5, HistoryRowKind.Set, "east"),
                Revision(14, 10, HistoryRowKind.Delete, "west"),
            ],
        });
        await loading;

        var requests = _client.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == nameof(ILatticeStateClient.GetEntryHistoryAsync))
            .Select(call => (EntryHistoryRequest)call.GetArguments()[0]!).ToArray();
        var rows = cut.FindAll(".lt-access-history__rev");
        Assert.Multiple(() =>
        {
            Assert.That(requests, Has.Length.EqualTo(2));
            Assert.That(requests.Select(request => request.ContinuationToken), Is.EqualTo(new string?[] { null, "next-page" }));
            Assert.That(requests.Select(request => request.TreeId), Is.All.EqualTo(LatticeAuthReservedTrees.PolicyTreeId));
            Assert.That(requests.Select(request => request.Key), Is.All.EqualTo("t/acme/orders\u001ftenant:acme:read-orders"));
            Assert.That(requests.Select(request => request.Limit), Is.All.EqualTo(50));
            Assert.That(requests.Select(request => request.Reverse), Is.All.False);
            Assert.That(rows, Has.Count.EqualTo(3));
            Assert.That(rows.Select(row => row.QuerySelector(".lt-access-history__time")!.TextContent),
                Is.EqualTo(new[] { "2026-09-28 14:10:00 UTC", "2026-09-28 14:05:00 UTC", "2026-09-28 14:00:00 UTC" }));
            Assert.That(rows[0].TextContent, Does.Contain("Removed").And.Contain("from west"));
            Assert.That(rows[1].TextContent, Does.Contain("Saved").And.Contain("from east"));
            Assert.That(rows[2].TextContent, Does.Not.Contain("from"));
            Assert.That(rows[0].QuerySelector(".lt-node")!.ClassList, Does.Contain("lt-node--hollow"));
            Assert.That(rows[1].QuerySelector(".lt-node")!.GetAttribute("class"), Is.EqualTo("lt-node"));
            Assert.That(cut.Find("ol").GetAttribute("aria-label"), Is.EqualTo("Revisions of the rule, newest first"));
            Assert.That(cut.FindAll("button"), Is.Empty);
        });
    }

    [Test]
    public async Task History_shows_a_named_loading_state_until_the_read_answers()
    {
        var response = new TaskCompletionSource<EntryHistoryResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        Serve(response.Task);

        var cut = RenderHistory();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-rule-history]").GetAttribute("data-lt-rule-history"), Is.EqualTo("loading"));
            Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo("Loading the rule's history"));
            Assert.That(cut.FindAll(".lt-access-caveat, ol, button"), Is.Empty);
        });
        response.SetResult(EmptyPage);
        await cut.InvokeAsync(() => Task.CompletedTask);
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-rule-history]").GetAttribute("data-lt-rule-history"), Is.EqualTo("loaded")));
    }

    [TestCase("")]
    [TestCase(null)]
    public void History_without_a_rule_id_is_unavailable_without_reading_the_policy_store(string? id)
    {
        Serve(Task.FromResult(EmptyPage));

        var cut = RenderHistory(Rule with { RuleId = id! });

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-rule-history]").GetAttribute("data-lt-rule-history"), Is.EqualTo("unavailable"));
            Assert.That(cut.Find(".lt-access-caveat").TextContent, Is.EqualTo(TenantRuleHistory.UnreadableText));
            Assert.That(_client.ReceivedCalls(), Is.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task History_left_during_a_read_cancels_it_and_ignores_a_late_result(bool cancelled)
    {
        var response = new TaskCompletionSource<EntryHistoryResponse>();
        Serve(response.Task);
        var cut = RenderHistory();
        var token = (CancellationToken)_client.ReceivedCalls().Single().GetArguments()[1]!;

        await DisposeComponentsAsync();
        Assert.That(token.IsCancellationRequested, Is.True);
        // Complete on the captured renderer context so the read resumes before the assertion.
        await cut.InvokeAsync(() =>
        {
            if (cancelled)
            {
                response.SetCanceled(token);
            }
            else
            {
                response.SetResult(EmptyPage with { Revisions = [Revision(14, 0, HistoryRowKind.Set, "east")] });
            }
        });
        Assert.That(Renderer.UnhandledException.IsCompleted, Is.False, "leaving during a read must not fault the circuit");
    }

    [Test]
    public void History_parameters_changed_after_a_read_reset_the_rows_and_paging_cursor()
    {
        Serve(
            Task.FromResult(EmptyPage with { Revisions = [Revision(14, 0, HistoryRowKind.Set, "old")], ContinuationToken = "old-cursor" }),
            Task.FromResult(EmptyPage with { Key = "t/acme/orders\u001ftenant:acme:new-rule", Revisions = [Revision(15, 0, HistoryRowKind.Delete, "new")] }));
        var cut = RenderHistory();

        cut.Render(p => p.Add(x => x.Rule, Rule with { RuleId = "new-rule" }));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-access-history__rev"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("ol").TextContent, Does.Contain("from new").And.Not.Contain("from old"));
            Assert.That(cut.FindAll("button"), Is.Empty);
            var request = (EntryHistoryRequest)_client.ReceivedCalls().Last().GetArguments()[0]!;
            Assert.That(request.Key, Is.EqualTo("t/acme/orders\u001ftenant:acme:new-rule"));
            Assert.That(request.ContinuationToken, Is.Null);
        });
    }

    private void Serve(params Task<EntryHistoryResponse>[] pages)
    {
        Services.AddSingleton(_client);
        var next = 0;
        _client.GetEntryHistoryAsync(Arg.Any<EntryHistoryRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => pages[next++]);
    }

    private IRenderedComponent<TenantRuleHistory> RenderHistory(TenantRuleView? rule = null) =>
        Render<TenantRuleHistory>(p => p.Add(x => x.Tenant, "acme").Add(x => x.Rule, rule ?? Rule));

    private static EntryRevisionRecord Revision(int hour, int minute, HistoryRowKind kind, string? origin) => new()
    {
        SourceKey = "t/acme/orders\u001ftenant:acme:read-orders",
        Hlc = new HybridLogicalClock { WallClockTicks = new DateTimeOffset(2026, 9, 28, hour, minute, 0, TimeSpan.Zero).UtcTicks },
        Kind = kind,
        OriginClusterId = origin,
    };
}
