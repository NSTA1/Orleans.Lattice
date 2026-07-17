using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

[TestFixture]
public class PolicyAdminServiceTests
{
    private static PolicyAdminService Create(FakeAuthAdminClient client) => new(client);

    private static LatticeAuthorizationRule Rule(string id, LatticeScope scope, LatticeEffect effect = LatticeEffect.Allow) =>
        new(id, LatticeSubjectSelector.User("alice"), scope, LatticeOperation.Read, effect);

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new PolicyAdminService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ListRulesAsync_success_returns_entries()
    {
        var client = new FakeAuthAdminClient
        {
            RulesResult = new AuthRulePage
            {
                Entries = new[] { Rule("r1", LatticeScope.Tree("t")) },
                NextPageToken = "next",
            },
        };
        var service = Create(client);

        var view = await service.ListRulesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Entries, Has.Count.EqualTo(1));
            Assert.That(view.NextPageToken, Is.EqualTo("next"));
        });
    }

    [Test]
    public async Task ListRulesAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ListThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.ListRulesAsync();

        Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
    }

    [Test]
    public void ListRulesForTreeAsync_empty_tree_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.ListRulesForTreeAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task PutRuleAsync_forwards_and_succeeds()
    {
        var client = new FakeAuthAdminClient();
        var service = Create(client);
        var rule = Rule("r1", LatticeScope.Key("t", "k"));

        var result = await service.PutRuleAsync(rule);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(client.LastPutRule, Is.SameAs(rule));
        });
    }

    [Test]
    public void PutRuleAsync_null_rule_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.PutRuleAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task PutRuleAsync_transport_failure_folds_into_failed_result()
    {
        var client = new FakeAuthAdminClient
        {
            MutationThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")),
        };
        var service = Create(client);

        var result = await service.PutRuleAsync(Rule("r1", LatticeScope.Tree("t")));

        Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Failed));
    }

    [Test]
    public async Task DeleteRuleAsync_removed_reports_success()
    {
        var client = new FakeAuthAdminClient { RemoveRuleResult = true };
        var service = Create(client);

        var result = await service.DeleteRuleAsync("t", "r1");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public async Task DeleteRuleAsync_absent_still_reports_success()
    {
        var client = new FakeAuthAdminClient { RemoveRuleResult = false };
        var service = Create(client);

        var result = await service.DeleteRuleAsync("t", "r1");

        Assert.That(result.IsSuccess, Is.True);
    }

    [Test]
    public void DeleteRuleAsync_empty_rule_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.DeleteRuleAsync("t", string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ExplainAsync_passes_through_verdict_verbatim()
    {
        var scope = LatticeScope.Key("t", "k");
        var client = new FakeAuthAdminClient
        {
            ExplanationResult = new AuthExplanation
            {
                SubjectId = "alice",
                Operation = LatticeOperation.Write,
                Scope = scope,
                Allowed = false,
                Reason = "deny rule",
            },
        };
        var service = Create(client);

        var view = await service.ExplainAsync("alice", LatticeOperation.Write, scope);

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Explanation!.Allowed, Is.False);
            Assert.That(view.Explanation!.Reason, Is.EqualTo("deny rule"));
            Assert.That(client.LastExplainSubjectId, Is.EqualTo("alice"));
            Assert.That(client.LastExplainOperation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(client.LastExplainScope, Is.SameAs(scope));
        });
    }

    [Test]
    public void ExplainAsync_null_scope_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.ExplainAsync("alice", LatticeOperation.Read, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ExplainAsync_empty_subject_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.ExplainAsync(string.Empty, LatticeOperation.Read, LatticeScope.Tree("t")),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ExplainAsync_denied_returns_denied_view()
    {
        var client = new FakeAuthAdminClient { ExplainThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var view = await service.ExplainAsync("alice", LatticeOperation.Read, LatticeScope.Tree("t"));

        Assert.Multiple(() =>
        {
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Denied));
            Assert.That(view.Explanation, Is.Null);
        });
    }

    [Test]
    public async Task EffectivePermissionsAsync_success_returns_permissions()
    {
        var client = new FakeAuthAdminClient
        {
            EffectiveResult = new AuthEffectivePermissions
            {
                SubjectId = "alice",
                Rules = new[] { Rule("r1", LatticeScope.Tree("t")) },
            },
        };
        var service = Create(client);

        var view = await service.EffectivePermissionsAsync("alice");

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Permissions!.Rules, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void EffectivePermissionsAsync_empty_subject_throws()
    {
        var service = Create(new FakeAuthAdminClient());

        Assert.That(() => service.EffectivePermissionsAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }
}
