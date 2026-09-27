using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Forces engine failures whose messages carry composed physical tree ids through
/// every verb and asserts the exception that crosses the facade carries app-local
/// names only.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlSanitizationTests
{
    private const string Leaky = "Writing 't/acme/a/crm/contacts' and 'a/billing/ledger' and 't/acme/legacy-contacts' failed.";

    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp()
    {
        _h = new AppsControlHarness();
        _h.Tenants.Tenant = AppsControlHarness.Acme;
    }

    private static void AssertSanitized(Exception? ex)
    {
        Assert.That(ex, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Not.Contain("t/acme/"));
            Assert.That(ex.Message, Does.Not.Contain("a/crm/"));
            Assert.That(ex.Message, Does.Not.Contain("a/billing/"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    private static IEnumerable<TestCaseData> Verbs()
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

    [TestCaseSource(nameof(Verbs))]
    public void Engine_exception_carrying_composed_ids_is_sanitized(Func<ILatticeAppsControl, Task> verb)
    {
        var fault = new InvalidOperationException(Leaky, new TimeoutException("inner t/acme/a/crm/contacts"));
        _h.Registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);
        _h.Registry.ListForTenantAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>()).Throws(fault);
        _h.Source.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>()).Throws(fault);
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);
        _h.Pipeline.DisableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);
        _h.Pipeline.UninstallAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);

        var ex = Assert.CatchAsync<InvalidOperationException>(() => verb(_h.Control));

        AssertSanitized(ex);
        Assert.That(ex!.Message, Does.Match("'(crm:)?contacts'").And.Contain("'billing:ledger'").And.Contain("'legacy-contacts'"));
    }

    [Test]
    public void GetCapabilitiesAsync_gate_fault_carrying_composed_ids_is_sanitized()
    {
        _h.Gate.Fault = new TimeoutException(Leaky);

        var ex = Assert.ThrowsAsync<TimeoutException>(() => _h.Control.GetCapabilitiesAsync());

        AssertSanitized(ex);
    }

    [Test]
    public void Activation_failure_diagnostics_are_sanitized()
    {
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(
                AppActivationOperation.Enable,
                AppRegistryLifecycleState.Installed,
                AppActivationFailure.TreeProvisioningFailed,
                diagnostic: "Provisioning tree 't/acme/a/crm/contacts' failed."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug));

        AssertSanitized(ex);
        Assert.That(ex!.Message, Does.Contain("Provisioning tree 'contacts' failed"));
    }

    [Test]
    public void Registry_rejection_message_is_sanitized()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed, tenant: AppsControlHarness.Acme));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Rejected(AppRegistryTransitionError.InvalidTransition, Leaky));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
        }));

        AssertSanitized(ex);
    }

    [Test]
    public void Denial_carrying_a_composed_tree_id_is_rebuilt_with_the_local_name()
    {
        _h.Pipeline.DisableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("t/acme/a/crm/contacts", LatticeOperation.Admin, "alice", "Denied on a/crm/contacts."));

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => _h.Control.DisableAsync(AppsControlHarness.Slug));

        AssertSanitized(ex);
        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo("contacts"));
            Assert.That(ex.Reason, Is.EqualTo("Denied on contacts."));
            Assert.That(ex.SubjectId, Is.EqualTo("alice"));
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.Admin));
        });
    }

    [Test]
    public void Clean_engine_exception_propagates_unchanged()
    {
        var fault = new InvalidOperationException("Nothing physical here.");
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).ThrowsAsync(fault);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug));

        Assert.That(ex, Is.SameAs(fault));
    }
}
