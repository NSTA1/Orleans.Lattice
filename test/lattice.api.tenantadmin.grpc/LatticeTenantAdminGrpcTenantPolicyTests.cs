using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Round-trip tests for the seven <see cref="ILatticeTenantPolicyAdmin"/> RPCs: each
/// client member is driven through the real Orleans marshallers into the service and
/// onto a substitute facade, proving every argument reaches the facade unaltered and
/// every result comes back intact, including the layer-aware explanation and the
/// tenant's posture with its D13 caps. Also proves the client's local argument
/// guards and that the RPCs answer <see cref="StatusCode.Unimplemented"/> when the
/// facade is not registered.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcTenantPolicyTests
{
    private TenantAccessGrpcHarness _h = null!;

    private ILatticeTenantPolicyAdmin Client => _h.Client;

    [SetUp]
    public void SetUp() => _h = new TenantAccessGrpcHarness();

    [TearDown]
    public void TearDown() => _h.Dispose();

    private static TenantRuleDraft Draft() => new()
    {
        RuleId = "readers-read",
        SubjectId = "readers",
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.Prefix,
        TreeName = "orders",
        KeyOrPrefix = "eu/",
        Operations = LatticeOperation.Read | LatticeOperation.RangeRead,
        Effect = LatticeEffect.Allow,
    };

    private static TenantRuleView View(string ruleId = "readers-read") => new()
    {
        RuleId = ruleId,
        Layer = TenantRuleLayer.Tenant,
        Origin = TenantRuleOrigin.Tenant,
        Editable = true,
        SubjectId = "readers",
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.Prefix,
        TreeName = "orders",
        KeyOrPrefix = "eu/",
        Operations = LatticeOperation.Read | LatticeOperation.RangeRead,
        Effect = LatticeEffect.Allow,
    };

    // ---- round trips -----------------------------------------------------

    [Test]
    public async Task PutRule_round_trips_the_draft_and_the_stored_view()
    {
        _h.Policy.PutRuleAsync("acme", Arg.Any<TenantRuleDraft>(), Arg.Any<CancellationToken>()).Returns(View());

        var view = await Client.PutRuleAsync("acme", Draft());

        Assert.That(view, Is.EqualTo(View()));
        await _h.Policy.Received(1).PutRuleAsync("acme", Draft(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetRule_round_trips_a_found_rule()
    {
        _h.Policy.GetRuleAsync("acme", "readers-read", Arg.Any<CancellationToken>()).Returns(View());

        var view = await Client.GetRuleAsync("acme", "readers-read");

        Assert.That(view, Is.EqualTo(View()));
    }

    [Test]
    public async Task GetRule_round_trips_an_absent_rule_as_null()
    {
        _h.Policy.GetRuleAsync("acme", "ghost", Arg.Any<CancellationToken>()).Returns((TenantRuleView?)null);

        var view = await Client.GetRuleAsync("acme", "ghost");

        Assert.That(view, Is.Null);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task RemoveRule_round_trips_whether_a_rule_was_removed(bool removed)
    {
        _h.Policy.RemoveRuleAsync("acme", "readers-read", Arg.Any<CancellationToken>()).Returns(removed);

        var result = await Client.RemoveRuleAsync("acme", "readers-read");

        Assert.That(result, Is.EqualTo(removed));
    }

    [Test]
    public async Task ListRules_round_trips_editable_and_withheld_rules()
    {
        var withheld = new TenantRuleView
        {
            RuleId = "platform-wide",
            Layer = TenantRuleLayer.Platform,
            Origin = TenantRuleOrigin.PlatformWide,
            Effect = LatticeEffect.Deny,
        };
        _h.Policy.ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TenantRulePage { Entries = [View(), withheld], NextPageToken = "platform-wide" });

        var page = await Client.ListRulesAsync("acme", new TenantAccessPageRequest { PageSize = 2, PageToken = "a" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { View(), withheld }));
            Assert.That(page.Entries[1].SubjectWithheld, Is.True);
            Assert.That(page.NextPageToken, Is.EqualTo("platform-wide"));
        });
        await _h.Policy.Received(1).ListRulesAsync(
            "acme", Arg.Is<TenantAccessPageRequest>(p => p.PageSize == 2 && p.PageToken == "a"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Explain_round_trips_every_argument_and_the_layer_aware_explanation()
    {
        _h.Policy.ExplainAsync("acme", "bob", "orders", "eu/1", LatticeOperation.Read, TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .Returns(new TenantExplanation
            {
                TenantId = "acme",
                SubjectId = "bob",
                TreeName = "orders",
                Key = "eu/1",
                Operation = LatticeOperation.Read,
                Allowed = true,
                Reason = "tenant rule",
                DecidingLayer = TenantRuleLayer.Tenant,
                DecidingRule = View(),
                DefaultEffect = LatticeEffect.Deny,
                MatchedRules = [View()],
            });

        var explanation = await Client.ExplainAsync("acme", "bob", "orders", "eu/1", LatticeOperation.Read);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Allowed, Is.True);
            Assert.That(explanation.Key, Is.EqualTo("eu/1"));
            Assert.That(explanation.DecidingLayer, Is.EqualTo(TenantRuleLayer.Tenant));
            Assert.That(explanation.DecidingRuleId, Is.EqualTo("readers-read"));
            Assert.That(explanation.DefaultEffect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(explanation.MatchedRules, Is.EqualTo(new[] { View() }));
        });
    }

    [Test]
    public async Task Explain_carries_a_whole_tree_request_and_a_group_subject()
    {
        _h.Policy.ExplainAsync("acme", "readers", "orders", null, LatticeOperation.RangeRead, TenantSubjectKind.TenantGroup, Arg.Any<CancellationToken>())
            .Returns(new TenantExplanation { TenantId = "acme", SubjectId = "readers", TreeName = "orders" });

        var explanation = await Client.ExplainAsync("acme", "readers", "orders", null, LatticeOperation.RangeRead, TenantSubjectKind.TenantGroup);

        Assert.Multiple(() =>
        {
            Assert.That(explanation.Key, Is.Null);
            Assert.That(explanation.DecidingLayer, Is.Null, "a default-effect decision has no deciding layer");
        });
        await _h.Policy.Received(1).ExplainAsync(
            "acme", "readers", "orders", null, LatticeOperation.RangeRead, TenantSubjectKind.TenantGroup, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EffectivePermissions_round_trips_with_and_without_a_tree()
    {
        _h.Policy.EffectivePermissionsAsync("acme", "bob", Arg.Any<string?>(), TenantSubjectKind.User, Arg.Any<CancellationToken>())
            .Returns(call => new TenantEffectivePermissions
            {
                TenantId = "acme",
                SubjectId = "bob",
                TreeName = call.ArgAt<string?>(2),
                Rules = [View()],
            });

        var narrowed = await Client.EffectivePermissionsAsync("acme", "bob", "orders");
        var all = await Client.EffectivePermissionsAsync("acme", "bob");

        Assert.Multiple(() =>
        {
            Assert.That(narrowed.TreeName, Is.EqualTo("orders"));
            Assert.That(narrowed.Rules, Is.EqualTo(new[] { View() }));
            Assert.That(all.TreeName, Is.Null);
        });
    }

    [Test]
    public async Task GetPosture_round_trips_the_flag_the_caller_and_every_cap()
    {
        _h.Policy.GetPostureAsync("acme", Arg.Any<CancellationToken>()).Returns(new TenantAccessPosture
        {
            TenantId = "acme",
            Enabled = true,
            CallerIsTenantAdmin = true,
            CallerIsPlatformOperator = false,
            Groups = new TenantQuotaDimensionUsage { Usage = 3, Limit = 500 },
            MembershipEdges = new TenantQuotaDimensionUsage { Usage = 12, Limit = 10_000 },
            MemberSubjects = new TenantQuotaDimensionUsage { Usage = 7, Limit = 5_000 },
            TenantRules = new TenantQuotaDimensionUsage { Usage = 999, Limit = 1_000 },
        });

        var posture = await Client.GetPostureAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(posture.Enabled, Is.True);
            Assert.That(posture.CallerIsTenantAdmin, Is.True);
            Assert.That(posture.CallerIsPlatformOperator, Is.False);
            Assert.That(posture.Groups, Is.EqualTo(new TenantQuotaDimensionUsage { Usage = 3, Limit = 500 }));
            Assert.That(posture.MembershipEdges, Is.EqualTo(new TenantQuotaDimensionUsage { Usage = 12, Limit = 10_000 }));
            Assert.That(posture.MemberSubjects, Is.EqualTo(new TenantQuotaDimensionUsage { Usage = 7, Limit = 5_000 }));
            Assert.That(posture.TenantRules, Is.EqualTo(new TenantQuotaDimensionUsage { Usage = 999, Limit = 1_000 }));
        });
    }

    [Test]
    public async Task GetPosture_answers_with_the_feature_off()
    {
        // The one RPC the facade answers while the feature is disabled: the binding
        // must pass the report through rather than refusing it itself.
        _h.Policy.GetPostureAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantAccessPosture { TenantId = "acme", Enabled = false });

        var posture = await Client.GetPostureAsync("acme");

        Assert.That(posture.Enabled, Is.False);
    }

    // ---- client argument guards ------------------------------------------

    private static IEnumerable<TestCaseData> InvalidArgumentCalls()
    {
        var page = new TenantAccessPageRequest();
        yield return Case("PutRule tenant", c => c.PutRuleAsync("", Draft()));
        yield return Case("PutRule rule", c => c.PutRuleAsync("acme", null!));
        yield return Case("GetRule id", c => c.GetRuleAsync("acme", ""));
        yield return Case("RemoveRule tenant", c => c.RemoveRuleAsync(null!, "r"));
        yield return Case("RemoveRule id", c => c.RemoveRuleAsync("acme", null!));
        yield return Case("ListRules page", c => c.ListRulesAsync("acme", null!));
        yield return Case("Explain subject", c => c.ExplainAsync("acme", "", "orders", null, LatticeOperation.Read));
        yield return Case("Explain tree", c => c.ExplainAsync("acme", "bob", "", null, LatticeOperation.Read));
        yield return Case("EffectivePermissions subject", c => c.EffectivePermissionsAsync("acme", null!));
        yield return Case("GetPosture tenant", c => c.GetPostureAsync(""));

        static TestCaseData Case(string name, Func<ILatticeTenantPolicyAdmin, Task> call) =>
            new TestCaseData(call).SetArgDisplayNames(name);
    }

    [TestCaseSource(nameof(InvalidArgumentCalls))]
    public void A_missing_argument_is_refused_locally_before_any_call(Func<ILatticeTenantPolicyAdmin, Task> call)
    {
        Assert.That(async () => await call(Client), Throws.InstanceOf<ArgumentException>());
        Assert.That(_h.Policy.ReceivedCalls(), Is.Empty, "a locally refused call must never reach the server");
    }

    // ---- optional facade --------------------------------------------------

    private static IEnumerable<TestCaseData> EveryPolicyCall()
    {
        var page = new TenantAccessPageRequest();
        yield return Case("PutTenantRule", c => c.PutRuleAsync("acme", Draft()));
        yield return Case("GetTenantRule", c => c.GetRuleAsync("acme", "r"));
        yield return Case("RemoveTenantRule", c => c.RemoveRuleAsync("acme", "r"));
        yield return Case("ListTenantRules", c => c.ListRulesAsync("acme", page));
        yield return Case("ExplainTenantAccess", c => c.ExplainAsync("acme", "bob", "orders", null, LatticeOperation.Read));
        yield return Case("GetTenantEffectivePermissions", c => c.EffectivePermissionsAsync("acme", "bob"));
        yield return Case("GetTenantAccessPosture", c => c.GetPostureAsync("acme"));

        static TestCaseData Case(string name, Func<ILatticeTenantPolicyAdmin, Task> call) =>
            new TestCaseData(call).SetArgDisplayNames(name);
    }

    [TestCaseSource(nameof(EveryPolicyCall))]
    public void Every_policy_rpc_reports_unimplemented_when_the_facade_is_absent(Func<ILatticeTenantPolicyAdmin, Task> call)
    {
        using var harness = new TenantAccessGrpcHarness(withPolicy: false);

        var fault = Assert.ThrowsAsync<RpcException>(async () => await call(harness.Client));

        Assert.That(fault!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public async Task The_directory_rpcs_still_serve_when_only_the_policy_facade_is_absent()
    {
        using var harness = new TenantAccessGrpcHarness(withPolicy: false);
        harness.Directory.GetGroupAsync("acme", "ops", Arg.Any<CancellationToken>())
            .Returns(new TenantGroupDescriptor { Name = "ops" });

        var group = await harness.Client.GetGroupAsync("acme", "ops");

        Assert.That(group!.Name, Is.EqualTo("ops"));
    }

    [Test]
    public async Task The_pre_epic_rpcs_still_serve_when_both_tenant_access_facades_are_absent()
    {
        using var harness = new TenantAccessGrpcHarness(withDirectory: false, withPolicy: false);

        var result = await harness.Client.SuspendTenantAsync("acme");

        Assert.That(result.TenantId, Is.EqualTo("acme"));
    }

    // ---- server-side argument guards --------------------------------------

    [Test]
    public void The_server_refuses_a_null_request_or_context()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _h.Service.PutTenantRule(null!, TenantAccessGrpcHarness.Context("PutTenantRule")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _h.Service.GetTenantRule(new TenantAdminRuleRequest { TenantId = "acme", RuleId = "r" }, null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _h.Service.RemoveTenantRule(null!, TenantAccessGrpcHarness.Context("RemoveTenantRule")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await _h.Service.GetTenantAccessPosture(null!, TenantAccessGrpcHarness.Context("GetTenantAccessPosture")),
                Throws.ArgumentNullException);
        });
    }
}
