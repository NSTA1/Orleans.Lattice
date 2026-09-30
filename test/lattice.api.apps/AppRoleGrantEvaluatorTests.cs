using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppRoleGrantEvaluator"/>, the shared app-role evaluation the workspace and the app
/// MCP tools gate on: which installs it considers, its fail-closed cases, and its per-revision cache.
/// </summary>
[TestFixture]
public sealed class AppRoleGrantEvaluatorTests
{
    private static readonly LatticeSubject Alice = new(WorkspaceHarness.Alice);

    private static AppRoleGrantEvaluator Evaluator(WorkspaceHarness harness, IAppSource? source = null) =>
        new(harness.Projection, source ?? harness.Sources, harness.Gate);

    private static LatticeSubject Member(params string[] groups) =>
        new(WorkspaceHarness.Alice, new HashSet<string>(groups, StringComparer.Ordinal));

    private static AppRegistryRecord BothBound() =>
        WorkspaceHarness.Record() with
        {
            RoleBindings = [AppRoleBinding.Create("reader", "g-readers"), AppRoleBinding.Create("writer", "g-writers")],
        };

    [Test]
    public async Task EvaluateAsync_reports_the_held_roles_in_manifest_order()
    {
        var harness = new WorkspaceHarness().Publish(BothBound());

        var evaluation = await Evaluator(harness).EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Member("g-writers", "g-readers"), CancellationToken.None);

        Assert.That(evaluation!.HeldRoles, Is.EqualTo(new[] { "reader", "writer" }));
        Assert.That(evaluation.HasGrant, Is.True);
        Assert.That(evaluation.Install.Record.Revision, Is.EqualTo(7));
    }

    /// <summary>
    /// #3902: a role is held by binding, not by capability. The harness gate allows everything (the app rules are
    /// live and the caller has broad rights), yet a caller bound only to reader holds reader only.
    /// </summary>
    [Test]
    public async Task EvaluateAsync_reports_only_the_roles_the_caller_is_bound_to()
    {
        var harness = new WorkspaceHarness().Publish(BothBound());

        var evaluation = await Evaluator(harness).EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Member("g-readers", "cluster-admins"), CancellationToken.None);

        Assert.That(evaluation!.HeldRoles, Is.EqualTo(new[] { "reader" }));
    }

    [Test]
    public async Task EvaluateAsync_follows_a_re_binding_through_the_record_revision()
    {
        var harness = new WorkspaceHarness().Publish(BothBound());
        var evaluator = Evaluator(harness);
        var alice = Member("g-readers");
        Assert.That((await evaluator.EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, alice, CancellationToken.None))!.HeldRoles, Is.EqualTo(new[] { "reader" }));

        harness.Publish(BothBound() with { Revision = 8, RoleBindings = [AppRoleBinding.Create("writer", "g-readers")] });

        Assert.That((await evaluator.EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, alice, CancellationToken.None))!.HeldRoles, Is.EqualTo(new[] { "writer" }));
    }

    [Test]
    public async Task EvaluateAsync_reports_an_install_without_a_grant_for_a_caller_holding_no_role()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record());

        var evaluation = await Evaluator(harness).EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Alice, CancellationToken.None);

        Assert.That(evaluation!.HeldRoles, Is.Empty);
        Assert.That(evaluation.HasGrant, Is.False);
    }

    [Test]
    public async Task EvaluateAsync_is_null_for_an_unknown_tenant_slug_or_uninitialised_value()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record());
        var evaluator = Evaluator(harness);
        var alice = Member("g-readers");

        Assert.That(await evaluator.EvaluateAsync(AppsControlHarness.Acme, WorkspaceHarness.Crm, alice, CancellationToken.None), Is.Null);
        Assert.That(await evaluator.EvaluateAsync(TenantId.Default, AppSlug.Parse("other"), alice, CancellationToken.None), Is.Null);
        Assert.That(await evaluator.EvaluateAsync(default, WorkspaceHarness.Crm, alice, CancellationToken.None), Is.Null);
        Assert.That(await evaluator.EvaluateAsync(TenantId.Default, default, alice, CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetInstallAsync_caches_the_compiled_install_per_record_revision()
    {
        var harness = new WorkspaceHarness();
        var evaluator = Evaluator(harness);
        var record = WorkspaceHarness.Record();

        var first = await evaluator.GetInstallAsync(record, CancellationToken.None);
        var again = await evaluator.GetInstallAsync(record, CancellationToken.None);
        var next = await evaluator.GetInstallAsync(record with { Revision = 8 }, CancellationToken.None);

        Assert.That(again, Is.SameAs(first));
        Assert.That(next, Is.Not.SameAs(first));
        Assert.That(harness.Source.Resolutions, Is.EqualTo(2));
    }

    [Test]
    public async Task GetInstallAsync_is_null_when_the_manifest_is_absent_mismatched_or_the_source_faults()
    {
        var harness = new WorkspaceHarness();
        var record = WorkspaceHarness.Record();
        var faulting = Substitute.For<IAppSource>();
        faulting.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<AppSourceResult>>(_ => throw new InvalidOperationException("source down"));
        var mismatched = Substitute.For<IAppSource>();
        var handle = Substitute.For<IAppActivationHandle>();
        mismatched.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<AppSourceResult>(AppSourceResult.Resolved(AppsControlHarness.Manifest(slug: "other"), new AppProvenance(), handle)));

        Assert.That(await Evaluator(harness, new TestCatalogSource("in-image")).GetInstallAsync(record, CancellationToken.None), Is.Null);
        Assert.That(await Evaluator(harness, faulting).GetInstallAsync(record, CancellationToken.None), Is.Null);
        Assert.That(await Evaluator(harness, mismatched).GetInstallAsync(record, CancellationToken.None), Is.Null);
    }

    [Test]
    public void GetInstallAsync_propagates_cancellation()
    {
        var harness = new WorkspaceHarness();
        var cancelled = Substitute.For<IAppSource>();
        cancelled.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<AppSourceResult>>(_ => throw new OperationCanceledException());

        Assert.ThrowsAsync<OperationCanceledException>(async () => await Evaluator(harness, cancelled).GetInstallAsync(WorkspaceHarness.Record(), CancellationToken.None));
    }

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    [TestCase(AppRegistryLifecycleState.Uninstalled)]
    public async Task Only_an_enabled_pinned_install_is_evaluated(AppRegistryLifecycleState state)
    {
        var record = WorkspaceHarness.Record(state);

        Assert.That(AppRoleGrantEvaluator.IsEvaluated(record), Is.False);
        Assert.That(AppRoleGrantEvaluator.IsEvaluated(WorkspaceHarness.Record() with { CeilingVersion = AppsControlHarness.V("0.1.0") }), Is.False);
        Assert.That(AppRoleGrantEvaluator.IsEvaluated(WorkspaceHarness.Record()), Is.True);
        Assert.That(await Evaluator(new WorkspaceHarness()).GetInstallAsync(record, CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task An_evaluator_missing_a_collaborator_cannot_serve()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record());
        var alice = Member("g-readers");

        foreach (var evaluator in new[]
        {
            new AppRoleGrantEvaluator(null, harness.Sources, harness.Gate),
            new AppRoleGrantEvaluator(harness.Projection, null, harness.Gate),
            new AppRoleGrantEvaluator(harness.Projection, harness.Sources, null),
        })
        {
            Assert.That(evaluator.CanServe, Is.False);
            Assert.That(await evaluator.GetInstallAsync(WorkspaceHarness.Record(), CancellationToken.None), Is.Null);
            Assert.That(await evaluator.EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, alice, CancellationToken.None), Is.Null);
        }

        Assert.That(await new AppRoleGrantEvaluator(null, null, null).GetSnapshotAsync(CancellationToken.None), Is.SameAs(CompiledAppRegistrySnapshot.Empty));
        Assert.That(new AppRoleGrantEvaluator(null, null, harness.Gate).Gate, Is.SameAs(harness.Gate));
    }

    /// <summary>
    /// Coordinator review of #3902: the binding grants a role and the gate can only take it away. An explicit
    /// deny on one bound role removes that role only; the gate is never asked for a role the caller is not bound
    /// to, so it can never add one.
    /// </summary>
    [Test]
    public async Task EvaluateAsync_lets_an_explicit_deny_take_a_bound_role_away_and_never_add_one()
    {
        var harness = new WorkspaceHarness().Publish(BothBound());
        harness.Gate.Deny(WorkspaceHarness.Alice, "a/crm/contacts", LatticeOperation.Write);
        var evaluator = Evaluator(harness);

        var bothBound = await evaluator.EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Member("g-readers", "g-writers"), CancellationToken.None);
        var requestsBefore = harness.Gate.Requests;
        var unbound = await evaluator.EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Member("cluster-admins"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(bothBound!.HeldRoles, Is.EqualTo(new[] { "reader" }), "the denied writer role is taken away");
            Assert.That(unbound!.HeldRoles, Is.Empty);
            Assert.That(harness.Gate.Requests, Is.EqualTo(requestsBefore), "no gate call for an unbound caller");
        });
    }

    [Test]
    public void CompileRoles_resolves_each_role_for_the_install_tenant()
    {
        var record = WorkspaceHarness.Record(tenant: AppsControlHarness.Acme);

        var roles = AppRoleGrantEvaluator.CompileRoles(record, AppsControlHarness.Manifest());

        Assert.That(roles, Has.Length.EqualTo(2));
        Assert.That(roles[0].Scopes.Select(s => s.TreeId), Is.EqualTo(new[] { "t/acme/a/crm/contacts", "t/acme/a/billing/ledger", "t/acme/a/crm/contacts" }));
        Assert.That(roles[1].Operations, Is.EqualTo(LatticeOperation.Write));
    }

    [Test]
    public void CompileRoles_carries_each_roles_distinct_bound_groups_and_ignores_unusable_bindings()
    {
        var record = WorkspaceHarness.Record() with
        {
            RoleBindings =
            [
                AppRoleBinding.Create("reader", "g-a"),
                AppRoleBinding.Create("reader", "g-b"),
                AppRoleBinding.Create("reader", "g-a"),
                AppRoleBinding.Create("auditor", "g-auditors"),
                null!,
                new AppRoleBinding { RoleName = "writer", GroupId = string.Empty },
            ],
        };

        var roles = AppRoleGrantEvaluator.CompileRoles(record, AppsControlHarness.Manifest());

        Assert.Multiple(() =>
        {
            Assert.That(roles[0].GroupIds, Is.EqualTo(new[] { "g-a", "g-b" }));
            Assert.That(roles[1].GroupIds, Is.Empty, "an empty group id binds nobody");
            Assert.That(roles[1].ConfersGrant, Is.False);
            Assert.That(
                AppRoleGrantEvaluator.CompileRoles(record, AppsControlHarness.Manifest() with { Roles = [null!] })[0].ConfersGrant,
                Is.False,
                "an unreadable role declaration confers nothing");
        });
    }

    [Test]
    public void CompileRoles_intersects_each_roles_operations_with_the_consented_ceiling()
    {
        var readOnly = WorkspaceHarness.Record() with
        {
            Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read),
            RoleBindings = [AppRoleBinding.Create("reader", "g-readers"), AppRoleBinding.Create("writer", "g-writers")],
        };
        var none = readOnly with { Ceiling = null! };

        var roles = AppRoleGrantEvaluator.CompileRoles(readOnly, AppsControlHarness.Manifest());
        var writerOnly = new LatticeSubject(WorkspaceHarness.Alice, ["g-writers"]);

        Assert.Multiple(() =>
        {
            Assert.That(roles[0].Operations, Is.EqualTo(LatticeOperation.Read));
            Assert.That(roles[1].Operations, Is.EqualTo(LatticeOperation.None), "the ceiling does not allow write");
            Assert.That(roles[1].IsHeld(writerOnly), Is.False, "a role whose rules confer nothing is never held");
            Assert.That(AppRoleGrantEvaluator.CompileRoles(none, AppsControlHarness.Manifest()).Select(r => r.Operations), Is.All.EqualTo(LatticeOperation.None));
        });
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var harness = new WorkspaceHarness();

        Assert.Throws<ArgumentNullException>(() => AppRoleGrantEvaluator.CompileRoles(null!, AppsControlHarness.Manifest()));
        Assert.Throws<ArgumentNullException>(() => AppRoleGrantEvaluator.CompileRoles(WorkspaceHarness.Record(), null!));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await Evaluator(harness).GetInstallAsync(null!, CancellationToken.None));
        var install = new AppRoleGrantInstall(WorkspaceHarness.Record(), AppsControlHarness.Manifest(), []);
        Assert.ThrowsAsync<ArgumentNullException>(async () => await AppRoleGrantEvaluator.EvaluateAsync(null!, install, Alice, CancellationToken.None));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await AppRoleGrantEvaluator.EvaluateAsync(harness.Gate, null!, Alice, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGrantInstall(null!, AppsControlHarness.Manifest(), []));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGrantInstall(WorkspaceHarness.Record(), null!, []));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGrantInstall(WorkspaceHarness.Record(), AppsControlHarness.Manifest(), null!));
    }
}
