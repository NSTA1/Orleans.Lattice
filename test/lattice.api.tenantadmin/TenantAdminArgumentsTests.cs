using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Covers <see cref="TenantAdminArguments"/>, the tenant-id validation shared by
/// the tenant administration facades.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class TenantAdminArgumentsTests
{
    [Test]
    public void ParseTenantId_parses_a_valid_id()
    {
        Assert.That(TenantAdminArguments.ParseTenantId("acme").Value, Is.EqualTo("acme"));
    }

    [Test]
    public void ParseTenantId_reports_the_default_parameter_name()
    {
        var empty = Assert.Throws<ArgumentException>(() => TenantAdminArguments.ParseTenantId(""));
        var malformed = Assert.Throws<ArgumentException>(() => TenantAdminArguments.ParseTenantId("NOT VALID!"));
        var missing = Assert.Throws<ArgumentNullException>(() => TenantAdminArguments.ParseTenantId(null!));

        Assert.Multiple(() =>
        {
            Assert.That(empty!.ParamName, Is.EqualTo("tenantId"));
            Assert.That(malformed!.ParamName, Is.EqualTo("tenantId"));
            Assert.That(malformed.Message, Does.Contain("'NOT VALID!' is not a valid tenant id."));
            Assert.That(missing!.ParamName, Is.EqualTo("tenantId"));
        });
    }

    [Test]
    public void ParseTenantId_reports_a_supplied_parameter_name()
    {
        var ex = Assert.Throws<ArgumentException>(() => TenantAdminArguments.ParseTenantId("NOT VALID!", "granteeTenantId"));

        Assert.That(ex!.ParamName, Is.EqualTo("granteeTenantId"));
    }

    [Test]
    public void ThrowIfReservedTenant_refuses_only_the_default_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ReservedTenantOperationException>(
                () => TenantAdminArguments.ThrowIfReservedTenant(TenantId.Default, "op"));
            Assert.DoesNotThrow(() => TenantAdminArguments.ThrowIfReservedTenant(TenantId.Parse("acme"), "op"));
        });
    }
}
