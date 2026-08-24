namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the core no-op tenancy seams
/// (<see cref="NullTenantContextResolver"/>,
/// <see cref="NullTenantAdmissionController"/>, and
/// <see cref="NullTenantEnumerationFilter"/>): the pass-through defaults used
/// when the tenancy add-on is not registered. They must resolve the reserved
/// <see cref="TenantId.Default"/> tenant, admit everything, enumerate every tree
/// unchanged, and report themselves inactive so a choke point pays nothing.
/// </summary>
[TestFixture]
public sealed class NullTenantSeamsTests
{
    [Test]
    public async Task Resolver_ResolveCurrentAsync_returns_the_default_tenant()
    {
        ITenantContextResolver resolver = new NullTenantContextResolver();

        var tenant = await resolver.ResolveCurrentAsync();

        Assert.That(tenant, Is.EqualTo(TenantId.Default));
        Assert.That(tenant.IsDefault, Is.True);
    }

    [Test]
    public async Task Resolver_ResolveCurrentAsync_honours_a_cancellation_token_without_throwing()
    {
        ITenantContextResolver resolver = new NullTenantContextResolver();

        var tenant = await resolver.ResolveCurrentAsync(CancellationToken.None);

        Assert.That(tenant.IsDefault, Is.True);
    }

    [Test]
    public void Resolver_TryResolveCurrent_resolves_the_default_tenant_synchronously()
    {
        ITenantContextResolver resolver = new NullTenantContextResolver();

        var resolved = resolver.TryResolveCurrent(out var tenant);

        Assert.That(resolved, Is.True);
        Assert.That(tenant, Is.EqualTo(TenantId.Default));
    }

    [Test]
    public void Admission_controller_is_inactive()
    {
        ITenantAdmissionController controller = new NullTenantAdmissionController();

        Assert.That(controller.IsActive, Is.False);
    }

    [Test]
    public async Task Admission_controller_admits_every_operation()
    {
        ITenantAdmissionController controller = new NullTenantAdmissionController();

        var admitted = await controller.IsAdmittedAsync(TenantId.Parse("contoso"), "t/contoso/orders");

        Assert.That(admitted, Is.True);
    }

    [Test]
    public void Enumeration_filter_is_inactive()
    {
        ITenantEnumerationFilter filter = new NullTenantEnumerationFilter();

        Assert.That(filter.IsActive, Is.False);
    }

    [Test]
    public void Enumeration_filter_returns_the_input_list_unchanged()
    {
        ITenantEnumerationFilter filter = new NullTenantEnumerationFilter();
        IReadOnlyList<string> trees = new List<string> { "t/contoso/orders", "sys-auth-users", "orders" };

        var filtered = filter.Filter(TenantId.Parse("contoso"), trees);

        Assert.That(filtered, Is.SameAs(trees));
    }
}
