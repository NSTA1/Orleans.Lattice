using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Coverage for the operator-gated tenant switcher. Every mutation is asserted to
/// be fail-closed: a non-operator, and the inactive view, change nothing. Asserted
/// directly against <see cref="ExplorerTenantSwitcher"/> with a deterministic
/// operator gate and a real per-circuit context - no cluster, no timing, no
/// ordering, no wall-clock, and no GC dependence.
/// </summary>
[TestFixture]
public class ExplorerTenantSwitcherTests
{
    private static readonly ExplorerTenantId Acme = new("acme");
    private static readonly ExplorerTenantId Globex = new("globex");

    private static ExplorerTenantSwitcher ActiveSwitcher(
        ExplorerTenantContext context,
        bool isOperator)
    {
        var view = new ExplorerTenantView(context, new StubOperatorGate(isOperator));
        return new ExplorerTenantSwitcher(view, context, new StubOperatorGate(isOperator));
    }

    private static ExplorerTenantSwitcher InactiveSwitcher(
        ExplorerTenantContext context,
        bool isOperator) =>
        new(NullExplorerTenantView.Instance, context, new StubOperatorGate(isOperator));

    // --- Constructor guards ---

    [Test]
    public void Ctor_nullView_throws()
    {
        Assert.That(
            () => new ExplorerTenantSwitcher(null!, new ExplorerTenantContext(), new StubOperatorGate(false)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullContext_throws()
    {
        Assert.That(
            () => new ExplorerTenantSwitcher(NullExplorerTenantView.Instance, null!, new StubOperatorGate(false)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_nullGate_throws()
    {
        Assert.That(
            () => new ExplorerTenantSwitcher(NullExplorerTenantView.Instance, new ExplorerTenantContext(), null!),
            Throws.ArgumentNullException);
    }

    // --- Read-through properties ---

    [Test]
    public void IsActive_reflectsView()
    {
        Assert.That(ActiveSwitcher(new ExplorerTenantContext(), isOperator: false).IsActive, Is.True);
        Assert.That(InactiveSwitcher(new ExplorerTenantContext(), isOperator: false).IsActive, Is.False);
    }

    [Test]
    public void ActiveTenant_reflectsContext()
    {
        var context = new ExplorerTenantContext { ActiveTenant = Acme };

        Assert.That(ActiveSwitcher(context, isOperator: false).ActiveTenant, Is.EqualTo(Acme));
    }

    [Test]
    public void RequestedVisibility_reflectsContext()
    {
        var context = new ExplorerTenantContext { RequestedVisibility = ExplorerTenantVisibility.AllTenants };

        Assert.That(
            ActiveSwitcher(context, isOperator: true).RequestedVisibility,
            Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }

    // --- IsOperatorAsync ---

    [Test]
    public async Task IsOperatorAsync_activeOperator_isTrue()
    {
        Assert.That(await ActiveSwitcher(new ExplorerTenantContext(), isOperator: true).IsOperatorAsync(), Is.True);
    }

    [Test]
    public async Task IsOperatorAsync_activeNonOperator_isFalse()
    {
        Assert.That(await ActiveSwitcher(new ExplorerTenantContext(), isOperator: false).IsOperatorAsync(), Is.False);
    }

    [Test]
    public async Task IsOperatorAsync_inactiveView_isFalseEvenForOperatorGate()
    {
        // The inactive view short-circuits before consulting the gate.
        Assert.That(await InactiveSwitcher(new ExplorerTenantContext(), isOperator: true).IsOperatorAsync(), Is.False);
    }

    // --- SetVisibilityAsync ---

    [Test]
    public async Task SetVisibilityAsync_operator_appliesAndReturnsTrue()
    {
        var context = new ExplorerTenantContext();
        var switcher = ActiveSwitcher(context, isOperator: true);

        var applied = await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.That(applied, Is.True);
        Assert.That(context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }

    [Test]
    public async Task SetVisibilityAsync_nonOperator_failsClosedAndLeavesVisibilityUnchanged()
    {
        var context = new ExplorerTenantContext();
        var switcher = ActiveSwitcher(context, isOperator: false);

        var applied = await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.That(applied, Is.False);
        Assert.That(context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
    }

    [Test]
    public async Task SetVisibilityAsync_inactiveView_failsClosed()
    {
        var context = new ExplorerTenantContext();
        var switcher = InactiveSwitcher(context, isOperator: true);

        var applied = await switcher.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.That(applied, Is.False);
        Assert.That(context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
    }

    // --- SwitchTenantAsync ---

    [Test]
    public async Task SwitchTenantAsync_operator_appliesAndReturnsTrue()
    {
        var context = new ExplorerTenantContext { ActiveTenant = Acme };
        var switcher = ActiveSwitcher(context, isOperator: true);

        var applied = await switcher.SwitchTenantAsync(Globex);

        Assert.That(applied, Is.True);
        Assert.That(context.ActiveTenant, Is.EqualTo(Globex));
    }

    [Test]
    public async Task SwitchTenantAsync_nonOperator_failsClosedAndLeavesTenantUnchanged()
    {
        var context = new ExplorerTenantContext { ActiveTenant = Acme };
        var switcher = ActiveSwitcher(context, isOperator: false);

        var applied = await switcher.SwitchTenantAsync(Globex);

        Assert.That(applied, Is.False);
        Assert.That(context.ActiveTenant, Is.EqualTo(Acme));
    }

    [Test]
    public async Task SwitchTenantAsync_inactiveView_failsClosed()
    {
        var context = new ExplorerTenantContext { ActiveTenant = Acme };
        var switcher = InactiveSwitcher(context, isOperator: true);

        var applied = await switcher.SwitchTenantAsync(Globex);

        Assert.That(applied, Is.False);
        Assert.That(context.ActiveTenant, Is.EqualTo(Acme));
    }
}
