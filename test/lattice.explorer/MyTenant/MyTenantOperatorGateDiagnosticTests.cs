using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.MyTenant;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The registration-order diagnostic: whether the head is running on the
/// navigation core's fail-closed placeholder platform-operator gate, or on a
/// real one a plugin supplied.
/// <para>
/// A misordered head - <c>AddExplorerTenantView()</c> before
/// <c>AddExplorerAccess()</c> - keeps the placeholder through <c>TryAdd</c>, so
/// nobody ever validates as an operator and every tenant switch quietly changes
/// nothing. These tests pin the detection that makes that visible.
/// </para>
/// </summary>
[TestFixture]
public sealed class MyTenantOperatorGateDiagnosticTests
{
    private sealed class HeadSuppliedGate : IExplorerTenantOperatorGate
    {
        public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
            new(true);
    }

    private static IExplorerTenantOperatorGate ResolvePlaceholder()
    {
        // The placeholder is internal to the navigation core, so it is obtained
        // exactly as a misordered head would obtain it - by calling the seam's
        // own registration and resolving the contract.
        var services = new ServiceCollection();
        services.AddExplorerTenantView();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>();
    }

    [Test]
    public void The_cores_own_gate_is_recognised_as_the_fail_closed_placeholder() =>
        Assert.That(MyTenantOperatorGateDiagnostic.IsFailClosedPlaceholder(ResolvePlaceholder()), Is.True);

    [Test]
    public void A_head_supplied_gate_is_not_the_placeholder() =>
        Assert.That(
            MyTenantOperatorGateDiagnostic.IsFailClosedPlaceholder(new HeadSuppliedGate()),
            Is.False);

    [Test]
    public void A_head_that_registered_no_gate_at_all_is_not_a_misordering() =>
        // No gate means the head never opted into tenant scoping, which is the
        // non-tenant posture rather than a mistake.
        Assert.That(MyTenantOperatorGateDiagnostic.IsFailClosedPlaceholder(null), Is.False);

    [Test]
    public void The_placeholder_is_described_and_names_the_fix()
    {
        var diagnostic = MyTenantOperatorGateDiagnostic.Describe(ResolvePlaceholder());

        Assert.Multiple(() =>
        {
            Assert.That(diagnostic, Is.EqualTo(MyTenantOperatorGateDiagnostic.PlaceholderGateMessage));
            Assert.That(diagnostic, Does.Contain("AddExplorerAccess()"));
            Assert.That(diagnostic, Does.Contain("AddExplorerTenantView()"));
            Assert.That(diagnostic, Does.Contain("TryAdd"));
        });
    }

    [Test]
    public void A_real_gate_produces_no_diagnostic() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantOperatorGateDiagnostic.Describe(new HeadSuppliedGate()), Is.Null);
            Assert.That(MyTenantOperatorGateDiagnostic.Describe(null), Is.Null);
        });

    [Test]
    public void The_placeholder_really_does_deny_every_caller()
    {
        // Guards the guard: the detection above would be pointless if the type it
        // detects were not in fact the fail-closed one.
        var placeholder = ResolvePlaceholder();

        Assert.That(placeholder.IsPlatformOperatorAsync().AsTask().GetAwaiter().GetResult(), Is.False);
    }
}
