using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsControlAuthorizationTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    private static IEnumerable<TestCaseData> GatedVerbs()
    {
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.InstallAsync(AppsControlHarness.InstallRequest()))).SetName("InstallAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.EnableAsync(AppsControlHarness.Slug))).SetName("EnableAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.DisableAsync(AppsControlHarness.Slug))).SetName("DisableAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.UninstallAsync(AppsControlHarness.Slug))).SetName("UninstallAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.ListAsync())).SetName("ListAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.DescribeAsync(AppsControlHarness.Slug))).SetName("DescribeAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.GetConsentAsync(AppsControlHarness.Slug))).SetName("GetConsentAsync");
        yield return new TestCaseData(new Func<ILatticeAppsControl, Task>(c => c.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
        }))).SetName("UpdateConsentAsync");
    }

    [TestCaseSource(nameof(GatedVerbs))]
    public void Denied_caller_is_refused_before_any_engine_access(Func<ILatticeAppsControl, Task> verb)
    {
        _h.Gate.Decision = LatticeAccessDecision.Deny("no");

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => verb(_h.Control));

        Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.AppInstall));
        Assert.That(ex.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
        var request = _h.Gate.Requests.Single();
        Assert.That(request.TreeId, Is.EqualTo(LatticeScope.ClusterWideTreeId));
        Assert.That(request.Operation, Is.EqualTo(LatticeOperation.AppInstall));
        _h.AssertEngineUntouched();
    }

    [TestCaseSource(nameof(GatedVerbs))]
    public void Key_filtered_allow_is_refused_before_any_engine_access(Func<ILatticeAppsControl, Task> verb)
    {
        _h.Gate.Decision = LatticeAccessDecision.Filtered(static _ => true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => verb(_h.Control));
        _h.AssertEngineUntouched();
    }

    [Test]
    public async Task GetCapabilitiesAsync_denied_caller_reports_every_permission_denied()
    {
        _h.Gate.Decision = LatticeAccessDecision.Deny("no");

        var capabilities = await _h.Control.GetCapabilitiesAsync();

        Assert.That(capabilities, Is.EqualTo(new LatticeAppsCapabilities()));
        Assert.That(_h.Gate.Requests.Single().Operation, Is.EqualTo(LatticeOperation.AppInstall));
        _h.AssertEngineUntouched();
    }

    [Test]
    public async Task GetCapabilitiesAsync_key_filtered_allow_reports_every_permission_denied()
    {
        _h.Gate.Decision = LatticeAccessDecision.Filtered(static _ => true);

        Assert.That(await _h.Control.GetCapabilitiesAsync(), Is.EqualTo(new LatticeAppsCapabilities()));
    }

    [Test]
    public async Task System_origin_caller_skips_the_gate()
    {
        _h.RegistryLists();
        using (LatticeSystemOrigin.Enter())
        {
            await _h.Control.ListAsync();
        }

        Assert.That(_h.Gate.Requests, Is.Empty);
    }
}
