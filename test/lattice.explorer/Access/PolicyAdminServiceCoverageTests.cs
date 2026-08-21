using Grpc.Core;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Coverage-focused tests for <see cref="PolicyAdminService"/> that fill the
/// branches the existing <c>PolicyAdminServiceTests</c> does not reach: the
/// per-tree listing, the single-rule lookup, and the remaining server-denial and
/// transport-failure paths on each mutation and query. They build on the existing
/// <see cref="FakeAuthAdminClient"/> and, for the lookup the fake cannot fault, a
/// scripted NSubstitute client.
/// </summary>
[TestFixture]
public class PolicyAdminServiceCoverageTests
{
    private static readonly LatticeAuthorizationDeniedException Denied = new("nope");
    private static RpcException Failed() => new(new Status(StatusCode.Unavailable, "gone"));

    private static PolicyAdminService Create(IAuthAdminClient client) => new(client);

    private static LatticeAuthorizationRule Rule(string id, LatticeScope scope) =>
        new(id, LatticeSubjectSelector.User("alice"), scope, LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public async Task ListRulesAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Failed() };

        var view = await Create(client).ListRulesAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task ListRulesForTreeAsync_success_returns_entries()
    {
        var client = new FakeAuthAdminClient
        {
            RulesResult = new AuthRulePage
            {
                Entries = new[] { Rule("r1", LatticeScope.Tree("orders")) },
                NextPageToken = "next",
            },
        };

        var view = await Create(client).ListRulesForTreeAsync("orders", pageSize: 10, pageToken: "cursor");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Has.Count.EqualTo(1));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task ListRulesForTreeAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Denied };

        var view = await Create(client).ListRulesForTreeAsync("orders");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task ListRulesForTreeAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = Failed() };

        var view = await Create(client).ListRulesForTreeAsync("orders");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task GetRuleAsync_success_returns_rule()
    {
        var client = new FakeAuthAdminClient { RuleResult = Rule("r1", LatticeScope.Tree("orders")) };

        var rule = await Create(client).GetRuleAsync("orders", "r1");

        Assert.That(rule!.RuleId, Is.EqualTo("r1"));
    }

    [Test]
    public void GetRuleAsync_empty_tree_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).GetRuleAsync(string.Empty, "r1"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetRuleAsync_empty_rule_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).GetRuleAsync("orders", string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task GetRuleAsync_denied_returns_null()
    {
        var client = Substitute.For<IAuthAdminClient>();
        client.GetRuleAsync("orders", "r1", Arg.Any<CancellationToken>())
            .Returns<Task<LatticeAuthorizationRule?>>(_ => throw Denied);

        var rule = await Create(client).GetRuleAsync("orders", "r1");

        Assert.That(rule, Is.Null);
    }

    [Test]
    public async Task GetRuleAsync_transport_failure_returns_null()
    {
        var client = Substitute.For<IAuthAdminClient>();
        client.GetRuleAsync("orders", "r1", Arg.Any<CancellationToken>())
            .Returns<Task<LatticeAuthorizationRule?>>(_ => throw Failed());

        var rule = await Create(client).GetRuleAsync("orders", "r1");

        Assert.That(rule, Is.Null);
    }

    [Test]
    public async Task PutRuleAsync_denied_returns_denied_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Denied };

        var result = await Create(client).PutRuleAsync(Rule("r1", LatticeScope.Tree("orders")));

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task DeleteRuleAsync_denied_returns_denied_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Denied };

        var result = await Create(client).DeleteRuleAsync("orders", "r1");

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task DeleteRuleAsync_transport_failure_returns_failed_result()
    {
        var client = new FakeAuthAdminClient { MutationThrows = Failed() };

        var result = await Create(client).DeleteRuleAsync("orders", "r1");

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public void DeleteRuleAsync_empty_tree_throws()
    {
        Assert.That(() => Create(new FakeAuthAdminClient()).DeleteRuleAsync(string.Empty, "r1"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ExplainAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { ExplainThrows = Failed() };

        var view = await Create(client).ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree("orders"));

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task EffectivePermissionsAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ExplainThrows = Denied };

        var view = await Create(client).EffectivePermissionsAsync("alice");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public async Task EffectivePermissionsAsync_transport_failure_returns_failed_view()
    {
        var client = new FakeAuthAdminClient { ExplainThrows = Failed() };

        var view = await Create(client).EffectivePermissionsAsync("alice");

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }
}
