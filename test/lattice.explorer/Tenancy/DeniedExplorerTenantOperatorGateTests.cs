using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The fail-closed platform-operator gate a head gets when it registers none of
/// its own: nobody validates as an operator, so the all-tenant view is never
/// admitted and a cross-tenant request degrades to the caller's active tenant.
/// </summary>
/// <remarks>
/// This is the default the tenant-view seam installs with <c>TryAdd</c>, so it is
/// what decides cross-tenant visibility on any head that opts into tenant scoping
/// without registering an administrative surface. Its verdict is invisible from
/// the UI - a denied operator and a caller who simply asked for their own tenant
/// draw the same page - so only a fixture against the gate shows it is wired at
/// all, which is why it sat wholly uncovered.
/// </remarks>
[TestFixture]
public sealed class DeniedExplorerTenantOperatorGateTests
{
    [Test]
    public async Task Nobody_validates_as_a_platform_operator()
    {
        Assert.That(await DeniedExplorerTenantOperatorGate.Instance.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task The_verdict_is_still_a_refusal_when_the_caller_has_stopped_waiting()
    {
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        // A gate that threw here would surface as a failed render rather than as a
        // denial, so the fail-closed answer has to survive a cancelled check.
        Assert.That(
            await DeniedExplorerTenantOperatorGate.Instance.IsPlatformOperatorAsync(cancelled.Token),
            Is.False);
    }

    [Test]
    public void The_gate_is_shared_and_holds_nothing_per_caller()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DeniedExplorerTenantOperatorGate.Instance, Is.Not.Null);
            Assert.That(DeniedExplorerTenantOperatorGate.Instance, Is.SameAs(DeniedExplorerTenantOperatorGate.Instance));
            Assert.That(
                typeof(DeniedExplorerTenantOperatorGate).GetConstructors(),
                Is.Empty,
                "the denial cannot be subverted by constructing a second one");
        });
    }

    [Test]
    public void A_head_that_registers_no_gate_of_its_own_is_scoped_by_this_one()
    {
        using var provider = new ServiceCollection().AddExplorerTenantView().BuildServiceProvider();
        using var circuit = provider.CreateScope();

        Assert.That(
            circuit.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>(),
            Is.SameAs(DeniedExplorerTenantOperatorGate.Instance),
            "the fail-closed default is what a head without an administrative surface gets");
    }

    [Test]
    public void A_head_that_supplies_a_real_gate_first_keeps_it()
    {
        var real = new StubOperatorGate(isOperator: true);
        using var provider = new ServiceCollection()
            .AddScoped<IExplorerTenantOperatorGate>(_ => real)
            .AddExplorerTenantView()
            .BuildServiceProvider();
        using var circuit = provider.CreateScope();

        Assert.That(
            circuit.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>(),
            Is.SameAs(real),
            "the seam defaults with TryAdd, so the administrative surface's gate wins");
    }

    [Test]
    public async Task An_all_tenant_request_behind_this_gate_resolves_to_the_callers_own_tenant()
    {
        var context = Substitute.For<IExplorerTenantContext>();
        context.RequestedVisibility.Returns(ExplorerTenantVisibility.AllTenants);
        var view = new ExplorerTenantView(context, DeniedExplorerTenantOperatorGate.Instance);

        var effective = await view.ResolveEffectiveVisibilityAsync();

        Assert.That(
            effective,
            Is.EqualTo(ExplorerTenantVisibility.ActiveTenant),
            "an unvalidated caller asking for every tenant is scoped back to their own");
    }
}
