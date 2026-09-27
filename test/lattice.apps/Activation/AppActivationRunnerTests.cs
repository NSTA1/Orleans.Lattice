using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppActivationRunner"/>, the body of every activation grain call: it
/// binds the run to the grain key and authorizes the caller for AppInstall before any side effect.
/// </summary>
[TestFixture]
public sealed class AppActivationRunnerTests
{
    private static readonly string Key = AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug);

    [Test]
    public async Task An_authorized_caller_runs_the_engine()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        var gate = RecordingAccessGate.AllowAll();
        var runner = new AppActivationRunner(new AppInstallAuthorizer(gate), harness.Engine);

        var outcome = await runner.RunAsync(Key, AppActivationOperation.Enable, TenantId.Default, ActivationHarness.Slug, CancellationToken.None);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(gate.Requests.Single().Operation, Is.EqualTo(LatticeOperation.AppInstall));
        Assert.That(gate.Requests.Single().TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
    }

    [Test]
    public async Task A_caller_without_AppInstall_is_denied_before_any_side_effect()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        var runner = new AppActivationRunner(new AppInstallAuthorizer(RecordingAccessGate.DenyAll()), harness.Engine);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => runner.RunAsync(Key, AppActivationOperation.Enable, TenantId.Default, ActivationHarness.Slug, CancellationToken.None));

        Assert.That(harness.Rules.Rules, Is.Empty);
        Assert.That(harness.Trees.Created, Is.Empty);
        Assert.That(await harness.Status.GetAsync(TenantId.Default, ActivationHarness.Slug, CancellationToken.None), Is.Null);
        var record = await harness.Registry.GetAsync(TenantId.Default, ActivationHarness.Slug);
        Assert.That(record!.State, Is.EqualTo(AppRegistryLifecycleState.Installed));
    }

    [Test]
    public async Task A_system_origin_caller_skips_the_gate()
    {
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());
        var gate = RecordingAccessGate.DenyAll();
        var runner = new AppActivationRunner(new AppInstallAuthorizer(gate), harness.Engine);

        AppActivationOutcome outcome;
        using (LatticeSystemOrigin.Enter())
        {
            outcome = await runner.RunAsync(Key, AppActivationOperation.Reconcile, TenantId.Default, ActivationHarness.Slug, CancellationToken.None);
        }

        Assert.That(gate.Requests, Is.Empty);
        Assert.That(outcome.Operation, Is.EqualTo(AppActivationOperation.Reconcile));
    }

    [Test]
    public void A_run_for_another_app_than_the_grain_key_is_rejected_before_authorization()
    {
        var harness = new ActivationHarness();
        var gate = RecordingAccessGate.AllowAll();
        var runner = new AppActivationRunner(new AppInstallAuthorizer(gate), harness.Engine);

        Assert.ThrowsAsync<ArgumentException>(
            () => runner.RunAsync("default/other", AppActivationOperation.Enable, TenantId.Default, ActivationHarness.Slug, CancellationToken.None));
        Assert.ThrowsAsync<ArgumentException>(
            () => runner.RunAsync(Key, AppActivationOperation.Enable, default, ActivationHarness.Slug, CancellationToken.None));
        Assert.That(gate.Requests, Is.Empty);
    }
}
