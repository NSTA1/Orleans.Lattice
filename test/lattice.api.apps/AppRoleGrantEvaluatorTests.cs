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

    [Test]
    public async Task EvaluateAsync_reports_the_held_roles_in_manifest_order()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record());
        harness.Gate.Grant(WorkspaceHarness.Alice, "a/crm/contacts", LatticeOperation.Read);
        harness.Gate.Grant(WorkspaceHarness.Alice, "a/crm/contacts", LatticeOperation.Write);

        var evaluation = await Evaluator(harness).EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Alice, CancellationToken.None);

        Assert.That(evaluation!.HeldRoles, Is.EqualTo(new[] { "reader", "writer" }));
        Assert.That(evaluation.HasGrant, Is.True);
        Assert.That(evaluation.Install.Record.Revision, Is.EqualTo(7));
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
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record()).GrantReader();
        var evaluator = Evaluator(harness);

        Assert.That(await evaluator.EvaluateAsync(AppsControlHarness.Acme, WorkspaceHarness.Crm, Alice, CancellationToken.None), Is.Null);
        Assert.That(await evaluator.EvaluateAsync(TenantId.Default, AppSlug.Parse("other"), Alice, CancellationToken.None), Is.Null);
        Assert.That(await evaluator.EvaluateAsync(default, WorkspaceHarness.Crm, Alice, CancellationToken.None), Is.Null);
        Assert.That(await evaluator.EvaluateAsync(TenantId.Default, default, Alice, CancellationToken.None), Is.Null);
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
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record()).GrantReader();

        foreach (var evaluator in new[]
        {
            new AppRoleGrantEvaluator(null, harness.Sources, harness.Gate),
            new AppRoleGrantEvaluator(harness.Projection, null, harness.Gate),
            new AppRoleGrantEvaluator(harness.Projection, harness.Sources, null),
        })
        {
            Assert.That(evaluator.CanServe, Is.False);
            Assert.That(await evaluator.GetInstallAsync(WorkspaceHarness.Record(), CancellationToken.None), Is.Null);
            Assert.That(await evaluator.EvaluateAsync(TenantId.Default, WorkspaceHarness.Crm, Alice, CancellationToken.None), Is.Null);
        }

        Assert.That(await new AppRoleGrantEvaluator(null, null, null).GetSnapshotAsync(CancellationToken.None), Is.SameAs(CompiledAppRegistrySnapshot.Empty));
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
    public void Null_arguments_are_rejected()
    {
        var harness = new WorkspaceHarness();
        var install = new AppRoleGrantInstall(WorkspaceHarness.Record(), AppsControlHarness.Manifest(), []);

        Assert.Throws<ArgumentNullException>(() => AppRoleGrantEvaluator.CompileRoles(null!, AppsControlHarness.Manifest()));
        Assert.Throws<ArgumentNullException>(() => AppRoleGrantEvaluator.CompileRoles(WorkspaceHarness.Record(), null!));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await Evaluator(harness).GetInstallAsync(null!, CancellationToken.None));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await AppRoleGrantEvaluator.EvaluateAsync(null!, install, Alice, CancellationToken.None));
        Assert.ThrowsAsync<ArgumentNullException>(async () => await AppRoleGrantEvaluator.EvaluateAsync(harness.Gate, null!, Alice, CancellationToken.None));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGrantInstall(null!, AppsControlHarness.Manifest(), []));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGrantInstall(WorkspaceHarness.Record(), null!, []));
        Assert.Throws<ArgumentNullException>(() => new AppRoleGrantInstall(WorkspaceHarness.Record(), AppsControlHarness.Manifest(), null!));
    }
}
