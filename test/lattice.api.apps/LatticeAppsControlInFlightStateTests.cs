using NSubstitute;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Issue #5482: a lifecycle run can record interim failed evidence while it works (a replicated run
/// records a failed outcome before it enrols). While that run is in flight the reads report it as
/// pending, with the state before the run, and never as <see cref="AppLifecycleState.Failed"/>;
/// Failed is reported only once no run is executing, so it is final.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlInFlightStateTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    /// <summary>The interim status a replicated run records before it enrols.</summary>
    private static AppActivationStatus Interim(AppActivationOperation operation) =>
        AppsControlHarness.Status(AppsControlHarness.Outcome(
            operation, state: null, AppActivationFailure.ReplicationEnrolmentFailed, changed: false, diagnostic: "replication-pending"));

    private void Running(params AppActivationOperation?[] answers) =>
        _h.Pipeline.GetRunningOperationAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(answers[0], answers[1..]);

    [TestCase(AppActivationOperation.Uninstall, AppRegistryLifecycleState.Disabled, AppLifecycleState.Disabled, AppLifecycleState.Uninstalled)]
    [TestCase(AppActivationOperation.Enable, AppRegistryLifecycleState.Installed, AppLifecycleState.Installed, AppLifecycleState.Enabled)]
    [TestCase(AppActivationOperation.Disable, AppRegistryLifecycleState.Enabled, AppLifecycleState.Enabled, AppLifecycleState.Disabled)]
    [TestCase(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppLifecycleState.Enabled, AppLifecycleState.Enabled)]
    public async Task ListAsync_reports_a_run_in_flight_as_pending_never_failed(
        AppActivationOperation operation, AppRegistryLifecycleState prior, AppLifecycleState state, AppLifecycleState target)
    {
        _h.RegistryLists(AppsControlHarness.Record(prior));
        _h.StatusIs(Interim(operation));
        Running(operation);

        var app = (await _h.Control.ListAsync()).Apps.Single();

        Assert.Multiple(() =>
        {
            Assert.That(app.State, Is.EqualTo(state), "the state before the run, not the interim failure");
            Assert.That(app.PendingState, Is.EqualTo(target));
        });
    }

    [Test]
    public async Task DescribeAsync_reports_a_run_in_flight_as_pending_never_failed()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled));
        _h.SourceResolves();
        _h.StatusIs(Interim(AppActivationOperation.Uninstall));
        Running(AppActivationOperation.Uninstall);

        var d = (await _h.Control.DescribeAsync(AppsControlHarness.Slug))!;

        Assert.Multiple(() =>
        {
            Assert.That(d.State, Is.EqualTo(AppLifecycleState.Disabled));
            Assert.That(d.PendingState, Is.EqualTo(AppLifecycleState.Uninstalled));
        });
    }

    [Test]
    public async Task A_failed_status_with_no_run_in_flight_is_final()
    {
        // The run's silo died part-way: its interim evidence is the last word, and nothing is running.
        _h.RegistryLists(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled));
        _h.StatusIs(Interim(AppActivationOperation.Uninstall));
        Running([null]);

        var app = (await _h.Control.ListAsync()).Apps.Single();

        Assert.Multiple(() =>
        {
            Assert.That(app.State, Is.EqualTo(AppLifecycleState.Failed));
            Assert.That(app.PendingState, Is.Null);
        });
    }

    [Test]
    public async Task A_run_that_starts_between_the_reads_is_reported_pending()
    {
        _h.RegistryLists(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled));
        _h.StatusIs(Interim(AppActivationOperation.Uninstall));
        Running([null, AppActivationOperation.Uninstall]);

        var app = (await _h.Control.ListAsync()).Apps.Single();

        Assert.Multiple(() =>
        {
            Assert.That(app.State, Is.EqualTo(AppLifecycleState.Disabled));
            Assert.That(app.PendingState, Is.EqualTo(AppLifecycleState.Uninstalled));
        });
    }

    [Test]
    public async Task A_run_that_ends_between_the_reads_is_reported_by_its_final_outcome()
    {
        _h.RegistryLists(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.Pipeline.GetStatusAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).Returns(
            Interim(AppActivationOperation.Enable),
            AppsControlHarness.Status(AppsControlHarness.Outcome(AppActivationOperation.Enable, AppRegistryLifecycleState.Enabled)));
        Running([null, null]);

        var app = (await _h.Control.ListAsync()).Apps.Single();

        Assert.Multiple(() =>
        {
            Assert.That(app.State, Is.EqualTo(AppLifecycleState.Enabled), "the final outcome, read once no run is executing");
            Assert.That(app.PendingState, Is.Null);
        });
    }

    [Test]
    public async Task A_settled_app_reports_no_pending_state_and_reads_no_run_for_an_uninstalled_record()
    {
        _h.RegistryLists(
            AppsControlHarness.Record(AppRegistryLifecycleState.Enabled, slug: "alpha"),
            AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled, slug: "gamma"));
        _h.StatusIs(null);
        Running([null]);

        var apps = (await _h.Control.ListAsync()).Apps;

        Assert.That(apps.Select(a => (a.Slug, a.State, a.PendingState)), Is.EqualTo(new (string, AppLifecycleState, AppLifecycleState?)[]
        {
            ("alpha", AppLifecycleState.Enabled, null),
            ("gamma", AppLifecycleState.Uninstalled, null),
        }));
        await _h.Pipeline.DidNotReceive().GetRunningOperationAsync(Arg.Any<TenantId>(), AppSlug.Parse("gamma"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_pipeline_reports_no_run_in_flight_by_default()
    {
        IAppActivationPipeline pipeline = new StatusOnlyPipeline();

        Assert.That(await pipeline.GetRunningOperationAsync(AppsControlHarness.Acme, AppsControlHarness.AppSlugValue), Is.Null);
    }

    /// <summary>A pipeline written before the in-flight read existed.</summary>
    private sealed class StatusOnlyPipeline : IAppActivationPipeline
    {
        public Task<AppActivationOutcome> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AppActivationOutcome> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AppActivationOutcome> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AppActivationOutcome> ReconcileAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AppActivationStatus?> GetStatusAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
            Task.FromResult<AppActivationStatus?>(null);
    }
}
