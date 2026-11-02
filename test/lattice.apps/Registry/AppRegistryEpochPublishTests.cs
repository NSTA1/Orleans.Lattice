namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Tests that <see cref="AppRegistry"/> acknowledges a transition only once the cluster-wide registry epoch has
/// moved past it (#5595), so an administrator is never told a revocation took effect while another silo can
/// still answer from a snapshot that predates it.
/// </summary>
[TestFixture]
public sealed class AppRegistryEpochPublishTests
{
    private InMemoryAppRegistryStore _store = null!;
    private InMemoryAppRegistryEpochSource _epoch = null!;
    private AppRegistry _registry = null!;

    [SetUp]
    public void SetUp()
    {
        _store = new InMemoryAppRegistryStore();
        _epoch = new InMemoryAppRegistryEpochSource();

        // Real time: a failed advance is retried after a short real delay.
        _registry = AppRegistryTestData.CreateRegistry(_store, time: TimeProvider.System, epoch: _epoch);
    }

    [Test]
    public async Task Every_committed_transition_advances_the_epoch_before_it_returns()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        Assert.That(_epoch.Current.Version, Is.EqualTo(1));

        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await _registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);
        await _registry.UninstallAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(_epoch.Current.Version, Is.EqualTo(4));
    }

    [Test]
    public async Task A_transient_advance_failure_is_retried_before_the_transition_returns()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        _epoch.FailAdvances = AppRegistry.MaxPublishAttempts - 1;

        var result = await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(result.Succeeded, Is.True);
        Assert.That(_epoch.Current.Version, Is.EqualTo(2));
    }

    [Test]
    public async Task A_revocation_that_cannot_be_published_is_not_acknowledged_until_a_retry_publishes_it()
    {
        await _registry.InstallAsync(AppRegistryTestData.Request());
        await _registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        var before = _epoch.Current.Version;
        _epoch.FailAdvances = AppRegistry.MaxPublishAttempts;

        Assert.That(
            async () => await _registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug),
            Throws.InvalidOperationException);
        Assert.That(_epoch.Current.Version, Is.EqualTo(before), "nothing was published");
        Assert.That((await _registry.GetAsync(TenantId.Default, AppRegistryTestData.Slug))!.State, Is.EqualTo(AppRegistryLifecycleState.Disabled));

        // The retry finds the write already applied, and acknowledges it only after publishing.
        var retry = await _registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(retry.Succeeded, Is.True);
        Assert.That(retry.Changed, Is.False);
        Assert.That(_epoch.Current.Version, Is.EqualTo(before + 1));
    }
}
