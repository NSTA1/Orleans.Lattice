using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// End-to-end coverage for the policy-administration and introspection halves of
/// the control facade. Proves rule CRUD round-trips through the policy store,
/// that <see cref="ILatticeAuthAdmin.ExplainAsync"/> returns the same verdict the
/// enforcing access gate produces for the same inputs (parity by construction),
/// that <see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/> reflects a live
/// policy change, and that every facade operation is refused for a non-admin or
/// anonymous caller (fail-closed), so the control plane can never be driven by an
/// unauthorized caller.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthAdminPolicyTests
{
    private const string Subject = "policy-subject";
    private const string Intruder = "intruder";

    private AuthAdminClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthAdminClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static IDisposable AsAdmin() => AuthAdminClusterFixture.AsSubject(AuthAdminClusterFixture.BootstrapAdmin);

    private static IDisposable As(string subject) => AuthAdminClusterFixture.AsSubject(subject);

    private static LatticeAuthorizationRule AllowUserKey(
        string ruleId, string subject, string treeId, string key, LatticeOperation ops) =>
        new(ruleId, LatticeSubjectSelector.User(subject), LatticeScope.Key(treeId, key), ops, LatticeEffect.Allow);

    /// <summary>
    /// Resolves a named subject exactly as the facade does (system-origin group
    /// closure), so a parity assertion feeds the gate the same subject the facade
    /// fed it.
    /// </summary>
    private async Task<LatticeSubject> ResolveAsync(string subjectId)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var groups = await _fixture.Directory.GroupsOfAsync(subjectId);
            return new LatticeSubject(subjectId, groups);
        }
    }

    [Test]
    public async Task rule_crud_round_trips_through_the_store()
    {
        const string tree = "policy-crud";
        var rule = AllowUserKey("r-crud", Subject, tree, "k1", LatticeOperation.Read);

        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(rule);

            var read = await _fixture.Admin.GetRuleAsync(tree, "r-crud");
            Assert.Multiple(() =>
            {
                Assert.That(read, Is.Not.Null);
                Assert.That(read!.RuleId, Is.EqualTo("r-crud"));
                Assert.That(read.Effect, Is.EqualTo(LatticeEffect.Allow));
            });

            var forTree = new List<string>();
            var page = await _fixture.Admin.ListRulesForTreeAsync(tree, new AuthPageRequest { PageSize = 100 });
            forTree.AddRange(page.Entries.Select(r => r.RuleId));
            Assert.That(forTree, Does.Contain("r-crud"));

            var removed = await _fixture.Admin.RemoveRuleAsync(tree, "r-crud");
            Assert.Multiple(async () =>
            {
                Assert.That(removed, Is.True);
                Assert.That(await _fixture.Admin.GetRuleAsync(tree, "r-crud"), Is.Null);
            });

            // A second remove reports nothing to remove.
            Assert.That(await _fixture.Admin.RemoveRuleAsync(tree, "r-crud"), Is.False);
        }
    }

    [Test]
    public async Task list_rules_across_trees_pages_and_covers_authored_rules()
    {
        const string treeA = "policy-list-a";
        const string treeB = "policy-list-b";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r1", Subject, treeA, "k", LatticeOperation.Read));
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r2", Subject, treeB, "k", LatticeOperation.Read));

            var seen = new List<string>();
            string? token = null;
            do
            {
                var page = await _fixture.Admin.ListRulesAsync(new AuthPageRequest { PageSize = 1, PageToken = token });
                seen.AddRange(page.Entries.Select(r => $"{r.Scope.TreeId}/{r.RuleId}"));
                token = page.NextPageToken;
            }
            while (token is not null);

            Assert.Multiple(() =>
            {
                Assert.That(seen, Is.Unique);
                Assert.That(seen, Does.Contain($"{treeA}/r1"));
                Assert.That(seen, Does.Contain($"{treeB}/r2"));
            });
        }
    }

    [Test]
    public async Task explain_matches_the_gate_verdict_for_the_same_inputs()
    {
        const string tree = "policy-explain-parity";
        var scope = LatticeScope.Key(tree, "k1");

        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("r-explain", Subject, tree, "k1", LatticeOperation.Read));
        }

        await _fixture.RebuildAsync();

        // Allowed operation: facade explanation must equal the gate's decision.
        AuthExplanation allowedExplanation;
        using (AsAdmin())
        {
            allowedExplanation = await _fixture.Admin.ExplainAsync(Subject, LatticeOperation.Read, scope);
        }

        var subject = await ResolveAsync(Subject);
        var allowedRequest = new LatticeAccessRequest(tree, LatticeOperation.Read, subject, "k1");
        var allowedDecision = await _fixture.Gate.AuthorizeAsync(in allowedRequest);

        // Denied operation (Write was never granted): parity must hold there too.
        AuthExplanation deniedExplanation;
        using (AsAdmin())
        {
            deniedExplanation = await _fixture.Admin.ExplainAsync(Subject, LatticeOperation.Write, scope);
        }

        var deniedRequest = new LatticeAccessRequest(tree, LatticeOperation.Write, subject, "k1");
        var deniedDecision = await _fixture.Gate.AuthorizeAsync(in deniedRequest);

        Assert.Multiple(() =>
        {
            Assert.That(allowedExplanation.Allowed, Is.EqualTo(allowedDecision.Allowed));
            Assert.That(allowedExplanation.Allowed, Is.True, "the granted Read must be allowed");
            Assert.That(allowedExplanation.MatchedRules.Select(r => r.RuleId), Does.Contain("r-explain"));

            Assert.That(deniedExplanation.Allowed, Is.EqualTo(deniedDecision.Allowed));
            Assert.That(deniedExplanation.Allowed, Is.False, "the ungranted Write must be denied");
        });
    }

    [Test]
    public async Task effective_permissions_reflects_a_policy_change()
    {
        const string tree = "policy-effective";
        using (AsAdmin())
        {
            await _fixture.Admin.PutRuleAsync(AllowUserKey("eff-1", Subject, tree, "k1", LatticeOperation.Read));

            var afterFirst = await _fixture.Admin.EffectivePermissionsAsync(Subject);
            Assert.That(afterFirst.Rules.Select(r => r.RuleId), Does.Contain("eff-1"));

            // Author a second rule: the resolved set updates from the live store.
            await _fixture.Admin.PutRuleAsync(AllowUserKey("eff-2", Subject, tree, "k2", LatticeOperation.Write));
            var afterSecond = await _fixture.Admin.EffectivePermissionsAsync(Subject);
            Assert.That(afterSecond.Rules.Select(r => r.RuleId), Is.SupersetOf(new[] { "eff-1", "eff-2" }));

            // Remove the first: it disappears from the resolved set.
            await _fixture.Admin.RemoveRuleAsync(tree, "eff-1");
            var afterRemoval = await _fixture.Admin.EffectivePermissionsAsync(Subject);
            Assert.Multiple(() =>
            {
                Assert.That(afterRemoval.Rules.Select(r => r.RuleId), Does.Not.Contain("eff-1"));
                Assert.That(afterRemoval.Rules.Select(r => r.RuleId), Does.Contain("eff-2"));
            });
        }
    }

    [Test]
    public async Task non_admin_caller_cannot_author_policy()
    {
        const string tree = "policy-intruder";
        var rule = AllowUserKey("intrusion", Intruder, tree, "k1", LatticeOperation.Read);

        using (As(Intruder))
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Admin.PutRuleAsync(rule));
        }

        // The denied attempt persisted nothing.
        using (AsAdmin())
        {
            Assert.That(await _fixture.Admin.GetRuleAsync(tree, "intrusion"), Is.Null);
        }
    }

    [Test]
    public void anonymous_caller_cannot_author_policy_fail_closed()
    {
        const string tree = "policy-anon";
        var rule = AllowUserKey("anon-rule", Subject, tree, "k1", LatticeOperation.Read);

        // No ambient subject: the caller is anonymous and default-denied.
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await _fixture.Admin.PutRuleAsync(rule));
    }

    [Test]
    public async Task non_admin_caller_cannot_mutate_membership()
    {
        using (As(Intruder))
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                async () => await _fixture.Admin.UpsertUserAsync(new AuthUser { UserId = "ghost" }));
        }

        using (AsAdmin())
        {
            Assert.That(await _fixture.Admin.GetUserAsync("ghost"), Is.Null);
        }
    }

    [Test]
    public void non_admin_caller_cannot_read_policy_introspection()
    {
        // Even the read-only introspection endpoints require an administrator: a
        // non-admin cannot enumerate policy through Explain / EffectivePermissions.
        using (As(Intruder))
        {
            Assert.Multiple(() =>
            {
                Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                    async () => await _fixture.Admin.ExplainAsync(
                        Subject, LatticeOperation.Read, LatticeScope.Key("any-tree", "k")));
                Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
                    async () => await _fixture.Admin.EffectivePermissionsAsync(Subject));
            });
        }
    }

    [Test]
    public void explain_and_rule_ops_validate_their_arguments()
    {
        using (AsAdmin())
        {
            Assert.Multiple(() =>
            {
                Assert.ThrowsAsync<ArgumentException>(
                    async () => await _fixture.Admin.ExplainAsync("", LatticeOperation.Read, LatticeScope.Tree("t")));
                Assert.ThrowsAsync<ArgumentNullException>(
                    async () => await _fixture.Admin.ExplainAsync(Subject, LatticeOperation.Read, null!));
                Assert.ThrowsAsync<ArgumentNullException>(
                    async () => await _fixture.Admin.PutRuleAsync(null!));
                Assert.ThrowsAsync<ArgumentException>(
                    async () => await _fixture.Admin.GetRuleAsync("", "r"));
            });
        }
    }
}
