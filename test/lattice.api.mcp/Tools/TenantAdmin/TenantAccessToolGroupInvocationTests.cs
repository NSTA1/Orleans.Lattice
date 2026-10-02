using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Fakes;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Drives every <see cref="TenantAccessToolGroup"/> tool's own invocation delegate
/// through <see cref="McpToolInvocation"/> against the in-memory delegated tenant
/// access fakes. Each tool is proved for the four outcomes the facades decide - an
/// authorized call, a denial, the feature switched off and a confinement refusal -
/// and the tools' happy paths are proved to reach the facade with the bound
/// arguments and to project its result. The tools add no authorization of their
/// own, so a denial must surface unchanged. Deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class TenantAccessToolGroupInvocationTests
{
    private const string Tenant = "acme";

    private FakeTenantAccessGate _gate = null!;
    private FakeTenantPolicyAdmin _policy = null!;
    private FakeTenantDirectoryAdmin _directory = null!;
    private ServiceProvider _services = null!;
    private TenantAccessToolGroup _group = null!;

    [SetUp]
    public void SetUp()
    {
        _gate = new FakeTenantAccessGate();
        _policy = new FakeTenantPolicyAdmin(_gate);
        _directory = new FakeTenantDirectoryAdmin(_gate, _policy);
        _services = new ServiceCollection()
            .AddSingleton<ILatticeTenantDirectoryAdmin>(_directory)
            .AddSingleton<ILatticeTenantPolicyAdmin>(_policy)
            .BuildServiceProvider();
        _group = new TenantAccessToolGroup(
            _services, Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = true }));
    }

    [TearDown]
    public async Task TearDown() => await _services.DisposeAsync();

    /// <summary>One representative, well-formed argument set per tool.</summary>
    private static IEnumerable<TestCaseData> EveryTool()
    {
        yield return Case("lattice_tenant_group_list", ("tenantId", Tenant));
        yield return Case("lattice_tenant_group_get", ("tenantId", Tenant), ("name", "ops"));
        yield return Case("lattice_tenant_group_members", ("tenantId", Tenant), ("groupName", "ops"));
        yield return Case("lattice_tenant_member_list", ("tenantId", Tenant));
        yield return Case("lattice_tenant_group_upsert", ("tenantId", Tenant), ("name", "ops"));
        yield return Case("lattice_tenant_group_remove", ("tenantId", Tenant), ("name", "ops"));
        yield return Case("lattice_tenant_group_member_add",
            ("tenantId", Tenant), ("groupName", "ops"), ("memberId", "alice"));
        yield return Case("lattice_tenant_group_member_remove",
            ("tenantId", Tenant), ("groupName", "ops"), ("memberId", "alice"));
        yield return Case("lattice_tenant_member_add", ("tenantId", Tenant), ("subjectId", "alice"));
        yield return Case("lattice_tenant_member_remove", ("tenantId", Tenant), ("subjectId", "alice"));
        yield return Case("lattice_tenant_rule_list", ("tenantId", Tenant));
        yield return Case("lattice_tenant_rule_get", ("tenantId", Tenant), ("ruleId", "r1"));
        yield return Case("lattice_tenant_rule_put",
            ("tenantId", Tenant), ("ruleId", "r1"), ("subjectId", "alice"), ("scopeKind", TenantRuleScopeKind.Tree),
            ("treeName", "orders"), ("operations", LatticeOperation.Read), ("effect", LatticeEffect.Allow));
        yield return Case("lattice_tenant_rule_remove", ("tenantId", Tenant), ("ruleId", "r1"));
        yield return Case("lattice_tenant_explain",
            ("tenantId", Tenant), ("subjectId", "alice"), ("treeName", "orders"), ("operation", LatticeOperation.Read));
        yield return Case("lattice_tenant_effective_permissions", ("tenantId", Tenant), ("subjectId", "alice"));
        yield return Case("lattice_tenant_access_posture", ("tenantId", Tenant));
    }

    private static TestCaseData Case(string tool, params (string Name, object? Value)[] args)
        => new TestCaseData(tool, args).SetArgDisplayNames(tool);

    private McpServerTool Tool(string name) => _group.Tools.Single(t => t.ProtocolTool.Name == name);

    private Task<ModelContextProtocol.Protocol.CallToolResult> CallAsync(string tool, (string Name, object? Value)[] args)
        => McpToolInvocation.CallAsync(Tool(tool), _services, McpToolInvocation.Args(args));

    private async Task<T> CallAsync<T>(string tool, params (string Name, object? Value)[] args)
        => (await CallAsync(tool, args)).Structured<T>();

    // ---- the four facade-decided outcomes, for every tool ------------------

    [TestCaseSource(nameof(EveryTool))]
    public async Task Authorized_call_reaches_the_facade_and_returns_structured_content(
        string tool, (string Name, object? Value)[] args)
    {
        await _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "ops" });
        _gate.Calls.Clear();

        var result = await CallAsync(tool, args);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(result.StructuredContent, Is.Not.Null);
            Assert.That(_gate.Calls, Has.Count.EqualTo(1), "Each tool is exactly one facade call.");
        });
    }

    [TestCaseSource(nameof(EveryTool))]
    public void Denial_surfaces_unchanged(string tool, (string Name, object? Value)[] args)
    {
        _gate.Denied = true;

        Assert.That(
            async () => await CallAsync(tool, args),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "The MCP layer adds no authorization path and never downgrades a denial.");
    }

    [TestCaseSource(nameof(EveryTool))]
    public async Task Feature_off_reads_clearly(string tool, (string Name, object? Value)[] args)
    {
        _gate.Enabled = false;

        if (tool == "lattice_tenant_access_posture")
        {
            var posture = (await CallAsync(tool, args)).Structured<McpTenantAccessPostureResult>();
            Assert.That(posture.Enabled, Is.False, "The posture probe answers while the feature is off.");
            return;
        }

        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(tool, args))!;
        Assert.Multiple(() =>
        {
            Assert.That(fault.Message, Does.Contain("delegated tenant access administration is not enabled on this cluster")
                .IgnoreCase);
            Assert.That(McpToolClientErrors.TryGetReason(fault, out _), Is.False,
                "A switched-off feature is not the caller's mistake.");
        });
    }

    [TestCaseSource(nameof(EveryTool))]
    public void Confinement_refusal_is_a_rejected_content_client_error(
        string tool, (string Name, object? Value)[] args)
    {
        _gate.NextFailure = new TenantAccessConfinementException(
            Tenant, TenantAccessConfinementRule.ForeignTenantGroup, "t/other/ops\r\nforged", "memberId");

        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(tool, args))!;

        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(fault, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.RejectedContent));
            Assert.That(fault.Message, Does.Contain("ForeignTenantGroup"));
            Assert.That(fault.Message, Does.Not.Contain("t/other/ops"),
                "The refusal is described from fixed text and never echoes the facade's caller-derived message.");
        });
    }

    [TestCaseSource(nameof(EveryTool))]
    public void Default_tenant_is_refused_as_an_invalid_argument(string tool, (string Name, object? Value)[] args)
    {
        var rewritten = args.Select(a => a.Name == "tenantId" ? (a.Name, (object?)TenantId.DefaultId) : a).ToArray();

        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(tool, rewritten))!;

        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(fault, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.InvalidArgument));
            Assert.That(fault.Message, Is.EqualTo(TenantAccessToolFaults.ReservedTenantMessage));
        });
    }

    // ---- directory happy paths ---------------------------------------------

    [Test]
    public async Task Group_upsert_get_and_list_round_trip_by_tenant_local_name()
    {
        var written = await CallAsync<McpTenantGroupResult>(
            "lattice_tenant_group_upsert", ("tenantId", Tenant), ("name", "ops"), ("displayName", "Operations"));
        var read = await CallAsync<McpTenantGroupGetResult>(
            "lattice_tenant_group_get", ("tenantId", Tenant), ("name", "ops"));
        var listed = await CallAsync<McpTenantGroupListResult>(
            "lattice_tenant_group_list", ("tenantId", Tenant), ("pageSize", 10));

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(new McpTenantGroupResult { TenantId = Tenant, Name = "ops", DisplayName = "Operations" }));
            Assert.That(read.Found, Is.True);
            Assert.That(read.DisplayName, Is.EqualTo("Operations"));
            Assert.That(listed.Groups.Select(g => g.Name), Is.EqualTo(new[] { "ops" }));
            Assert.That(listed.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task Group_get_of_an_absent_group_reports_not_found()
    {
        var read = await CallAsync<McpTenantGroupGetResult>(
            "lattice_tenant_group_get", ("tenantId", Tenant), ("name", "missing"));

        Assert.Multiple(() =>
        {
            Assert.That(read.Found, Is.False);
            Assert.That(read.Name, Is.EqualTo("missing"));
            Assert.That(read.DisplayName, Is.Null);
        });
    }

    [Test]
    public async Task Group_list_forwards_the_page_size_and_cursor()
    {
        foreach (var name in new[] { "a", "b", "c" })
        {
            await _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = name });
        }

        var first = await CallAsync<McpTenantGroupListResult>(
            "lattice_tenant_group_list", ("tenantId", Tenant), ("pageSize", 2));
        var second = await CallAsync<McpTenantGroupListResult>(
            "lattice_tenant_group_list", ("tenantId", Tenant), ("pageSize", 2), ("pageToken", first.NextPageToken));

        Assert.Multiple(() =>
        {
            Assert.That(first.Groups.Select(g => g.Name), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(first.NextPageToken, Is.EqualTo("b"));
            Assert.That(second.Groups.Select(g => g.Name), Is.EqualTo(new[] { "c" }));
        });
    }

    [Test]
    public async Task Group_member_add_binds_the_member_kind_and_lists_the_member()
    {
        await _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "ops" });
        await _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "oncall" });

        var added = await CallAsync<McpTenantMembershipChangeResult>(
            "lattice_tenant_group_member_add",
            ("tenantId", Tenant), ("groupName", "ops"), ("memberId", "oncall"), ("memberKind", TenantSubjectKind.TenantGroup));
        var again = await CallAsync<McpTenantMembershipChangeResult>(
            "lattice_tenant_group_member_add",
            ("tenantId", Tenant), ("groupName", "ops"), ("memberId", "oncall"), ("memberKind", TenantSubjectKind.TenantGroup));
        var members = await CallAsync<McpTenantGroupMembersResult>(
            "lattice_tenant_group_members", ("tenantId", Tenant), ("groupName", "ops"));

        Assert.Multiple(() =>
        {
            Assert.That(added.Changed, Is.True);
            Assert.That(added.GroupName, Is.EqualTo("ops"));
            Assert.That(added.SubjectKind, Is.EqualTo("TenantGroup"));
            Assert.That(again.Changed, Is.False, "Adding a member is idempotent.");
            Assert.That(members.Members, Is.EqualTo(new[] { new McpTenantSubject { SubjectId = "oncall", Kind = "TenantGroup" } }));
        });
    }

    [Test]
    public async Task Group_member_remove_of_an_absent_member_reports_no_change()
    {
        await _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "ops" });

        var removed = await CallAsync<McpTenantMembershipChangeResult>(
            "lattice_tenant_group_member_remove", ("tenantId", Tenant), ("groupName", "ops"), ("memberId", "nobody"));

        Assert.That(removed.Changed, Is.False);
    }

    [Test]
    public async Task Member_add_list_and_remove_round_trip()
    {
        var added = await CallAsync<McpTenantMembershipChangeResult>(
            "lattice_tenant_member_add", ("tenantId", Tenant), ("subjectId", "ops-team"), ("subjectKind", TenantSubjectKind.ClusterGroup));
        var listed = await CallAsync<McpTenantMemberListResult>("lattice_tenant_member_list", ("tenantId", Tenant));
        var removed = await CallAsync<McpTenantMembershipChangeResult>(
            "lattice_tenant_member_remove", ("tenantId", Tenant), ("subjectId", "ops-team"), ("subjectKind", TenantSubjectKind.ClusterGroup));

        Assert.Multiple(() =>
        {
            Assert.That(added.Changed, Is.True);
            Assert.That(added.GroupName, Is.Null, "A member-set change names no group.");
            Assert.That(listed.Members, Is.EqualTo(new[] { new McpTenantSubject { SubjectId = "ops-team", Kind = "ClusterGroup" } }));
            Assert.That(removed.Changed, Is.True);
        });
    }

    [Test]
    public async Task Group_remove_reports_the_cascade()
    {
        await _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "ops" });
        await _directory.AddMemberAsync(Tenant, "ops", TenantSubjectKind.TenantGroup);
        await _policy.PutRuleAsync(Tenant, new TenantRuleDraft
        {
            RuleId = "ops-read",
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.TenantWide,
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });

        var result = await CallAsync<McpTenantGroupRemovalResult>(
            "lattice_tenant_group_remove", ("tenantId", Tenant), ("name", "ops"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Is.True);
            Assert.That(result.RemovedFromMemberSet, Is.True);
            Assert.That(result.RemovedRuleIds, Is.EqualTo(new[] { "ops-read" }));
        });
    }

    [Test]
    public async Task Group_remove_of_an_absent_group_reports_not_removed()
    {
        var result = await CallAsync<McpTenantGroupRemovalResult>(
            "lattice_tenant_group_remove", ("tenantId", Tenant), ("name", "missing"));

        Assert.That(result.Removed, Is.False);
    }

    [Test]
    public void Removing_the_last_admin_group_is_refused_as_an_invalid_argument()
    {
        _ = _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "admins" });
        _directory.SeedAdmin(Tenant, new TenantMemberEntry { SubjectId = "admins", Kind = TenantSubjectKind.TenantGroup });

        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(
            "lattice_tenant_group_remove", [("tenantId", Tenant), ("name", "admins")]))!;

        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(fault, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.InvalidArgument));
            Assert.That(fault.Message, Is.EqualTo(TenantAccessToolFaults.LastAdminMessage));
        });
    }

    [Test]
    public void Adding_a_member_to_a_missing_group_is_an_invalid_argument()
    {
        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(
            "lattice_tenant_group_member_add", [("tenantId", Tenant), ("groupName", "missing"), ("memberId", "alice")]))!;

        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(fault, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.InvalidArgument));
        });
    }

    [Test]
    public void A_foreign_tenant_group_named_as_a_cluster_group_is_refused_by_the_facade()
    {
        _ = _directory.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "ops" });

        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(
            "lattice_tenant_group_member_add",
            [("tenantId", Tenant), ("groupName", "ops"), ("memberId", "t/other/admins"), ("memberKind", TenantSubjectKind.ClusterGroup)]))!;

        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(fault, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.RejectedContent));
            Assert.That(fault.Message, Does.Not.Contain("t/other/admins"));
        });
    }

    // ---- policy happy paths -------------------------------------------------

    [Test]
    public async Task Rule_put_binds_every_draft_field_and_get_and_list_read_it_back()
    {
        var put = await CallAsync<McpTenantRulePutResult>(
            "lattice_tenant_rule_put",
            ("tenantId", Tenant), ("ruleId", "r1"), ("subjectId", "ops"), ("subjectKind", TenantSubjectKind.TenantGroup),
            ("scopeKind", TenantRuleScopeKind.Prefix), ("treeName", "orders"), ("keyOrPrefix", "eu/"),
            ("operations", LatticeOperation.Read | LatticeOperation.Write), ("effect", LatticeEffect.Deny));
        var read = await CallAsync<McpTenantRuleGetResult>("lattice_tenant_rule_get", ("tenantId", Tenant), ("ruleId", "r1"));
        var listed = await CallAsync<McpTenantRuleListResult>("lattice_tenant_rule_list", ("tenantId", Tenant));

        var expected = new McpTenantRule
        {
            RuleId = "r1",
            Layer = "Tenant",
            Origin = "Tenant",
            Editable = true,
            SubjectWithheld = false,
            SubjectId = "ops",
            SubjectKind = "TenantGroup",
            ScopeKind = "Prefix",
            TreeName = "orders",
            KeyOrPrefix = "eu/",
            Operations = (LatticeOperation.Read | LatticeOperation.Write).ToString(),
            Effect = "Deny",
        };
        Assert.Multiple(() =>
        {
            Assert.That(put.Rule, Is.EqualTo(expected));
            Assert.That(read.Found, Is.True);
            Assert.That(read.Rule, Is.EqualTo(expected));
            Assert.That(listed.Rules, Is.EqualTo(new[] { expected }));
        });
    }

    [Test]
    public async Task Rule_get_of_an_absent_rule_reports_not_found_and_remove_reports_not_removed()
    {
        var read = await CallAsync<McpTenantRuleGetResult>("lattice_tenant_rule_get", ("tenantId", Tenant), ("ruleId", "nope"));
        var removed = await CallAsync<McpTenantRuleRemoveResult>("lattice_tenant_rule_remove", ("tenantId", Tenant), ("ruleId", "nope"));

        Assert.Multiple(() =>
        {
            Assert.That(read.Found, Is.False);
            Assert.That(read.Rule, Is.Null);
            Assert.That(removed.Removed, Is.False);
        });
    }

    [Test]
    public void Rule_put_at_the_cap_reads_as_the_quota_message()
    {
        _policy.MaxTenantRules = 0;

        var fault = Assert.ThrowsAsync<McpException>(async () => await CallAsync(
            "lattice_tenant_rule_put",
            [("tenantId", Tenant), ("ruleId", "r1"), ("subjectId", "alice"), ("scopeKind", TenantRuleScopeKind.TenantWide),
             ("operations", LatticeOperation.Read), ("effect", LatticeEffect.Allow)]))!;

        Assert.Multiple(() =>
        {
            Assert.That(fault.Message, Does.Contain("MaxTenantRules cap of 0"));
            Assert.That(fault.Message, Does.Contain("lattice_tenant_set_quotas"));
        });
    }

    [Test]
    public async Task Rule_list_withholds_the_subject_of_a_platform_wide_rule()
    {
        _policy.SeedPlatformRule(Tenant, new TenantRuleView
        {
            RuleId = "platform-tree",
            Layer = TenantRuleLayer.Platform,
            Origin = TenantRuleOrigin.PlatformTree,
            SubjectId = "operators",
            SubjectKind = TenantSubjectKind.ClusterGroup,
            ScopeKind = TenantRuleScopeKind.Tree,
            TreeName = "orders",
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });

        var listed = await CallAsync<McpTenantRuleListResult>("lattice_tenant_rule_list", ("tenantId", Tenant));

        Assert.That(listed.Rules.Single(), Is.EqualTo(new McpTenantRule
        {
            RuleId = "platform-tree",
            Layer = "Platform",
            Origin = "PlatformTree",
            Editable = false,
            SubjectWithheld = false,
            SubjectId = "operators",
            SubjectKind = "ClusterGroup",
            ScopeKind = "Tree",
            TreeName = "orders",
            Operations = "Read",
            Effect = "Allow",
        }));
    }

    [Test]
    public async Task Explain_binds_the_key_operation_and_subject_kind()
    {
        await _policy.PutRuleAsync(Tenant, new TenantRuleDraft
        {
            RuleId = "ops-read",
            SubjectId = "ops",
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.Tree,
            TreeName = "orders",
            Operations = LatticeOperation.Read,
            Effect = LatticeEffect.Allow,
        });

        var explained = await CallAsync<McpTenantExplanationResult>(
            "lattice_tenant_explain",
            ("tenantId", Tenant), ("subjectId", "ops"), ("subjectKind", TenantSubjectKind.TenantGroup),
            ("treeName", "orders"), ("key", "k1"), ("operation", LatticeOperation.Read));

        Assert.Multiple(() =>
        {
            Assert.That(explained.Allowed, Is.True);
            Assert.That(explained.Key, Is.EqualTo("k1"));
            Assert.That(explained.SubjectKind, Is.EqualTo("TenantGroup"));
            Assert.That(explained.Operation, Is.EqualTo("Read"));
            Assert.That(explained.DecidingLayer, Is.EqualTo("Tenant"));
            Assert.That(explained.DecidingRuleId, Is.EqualTo("ops-read"));
        });
    }

    [Test]
    public async Task Effective_permissions_binds_the_optional_tree()
    {
        var report = await CallAsync<McpTenantEffectivePermissionsResult>(
            "lattice_tenant_effective_permissions", ("tenantId", Tenant), ("subjectId", "alice"), ("treeName", "orders"));

        Assert.Multiple(() =>
        {
            Assert.That(report.SubjectId, Is.EqualTo("alice"));
            Assert.That(report.TreeName, Is.EqualTo("orders"));
            Assert.That(report.SubjectKind, Is.EqualTo("User"));
        });
    }

    [Test]
    public async Task Posture_reports_the_caller_standing_and_caps()
    {
        _policy.CallerIsPlatformOperator = true;

        var posture = await CallAsync<McpTenantAccessPostureResult>("lattice_tenant_access_posture", ("tenantId", Tenant));

        Assert.Multiple(() =>
        {
            Assert.That(posture.Enabled, Is.True);
            Assert.That(posture.CallerIsPlatformOperator, Is.True);
            Assert.That(posture.Groups.Limit, Is.EqualTo(500));
            Assert.That(posture.TenantRules.Limit, Is.EqualTo(1000));
        });
    }
}
