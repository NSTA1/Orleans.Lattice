using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// Issue #4162: the tenant Groups and Members pages register their own services
/// through the Shell's partials (<c>AddTenantAccessGroups</c>, <c>AddTenantAccessMembers</c>):
/// the history reader a group's history is shown through, and the reader of the
/// tenant's administrator entries - both per circuit.
/// </summary>
[TestFixture]
public sealed class TenantAccessPagesRegistrationTests
{
    [Test]
    public void The_shell_registers_the_pages_services_per_circuit()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        ServiceLifetime Lifetime<T>() => services.Single(descriptor => descriptor.ServiceType == typeof(T)).Lifetime;

        Assert.Multiple(() =>
        {
            Assert.That(Lifetime<IHistoryReader>(), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime<TenantAdminSubjects>(), Is.EqualTo(ServiceLifetime.Scoped));
        });
    }

    [Test]
    public void The_administrators_reader_resolves_in_a_head_that_serves_no_tenant_facade()
    {
        using var provider = new ServiceCollection().AddLatticeExplorerShell().BuildServiceProvider(validateScopes: true);
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<TenantAdminSubjects>(), Is.Not.Null);
    }

    [Test]
    public void Registering_the_shell_twice_registers_each_service_once()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell().AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(IHistoryReader)), Is.EqualTo(1));
            Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(TenantAdminSubjects)), Is.EqualTo(1));
        });
    }
}
