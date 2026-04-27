using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the internal <see cref="LatticeMaintenanceContext"/>
/// ambient flag that toggles <see cref="LatticeMutation.Category"/>
/// between <see cref="MutationCategory.User"/> and
/// <see cref="MutationCategory.Maintenance"/> for the duration of a
/// library-internal structural mutation.
/// </summary>
[TestFixture]
public class LatticeMaintenanceContextTests
{
    [SetUp]
    public void EnsureCleanContext()
    {
        // Sibling tests may have leaked context — clear it so each test
        // starts from the documented default.
        RequestContext.Remove("ol.maint");
    }

    [Test]
    public void Current_defaults_to_User_when_no_scope_is_active()
    {
        Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.User));
    }

    [Test]
    public void BeginScope_flips_Current_to_Maintenance()
    {
        using (LatticeMaintenanceContext.BeginScope())
        {
            Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.Maintenance));
        }
    }

    [Test]
    public void BeginScope_restores_User_default_after_dispose()
    {
        using (LatticeMaintenanceContext.BeginScope())
        {
            // active
        }

        Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.User));
    }

    [Test]
    public void BeginScope_is_safely_nestable()
    {
        using (LatticeMaintenanceContext.BeginScope())
        {
            Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.Maintenance));
            using (LatticeMaintenanceContext.BeginScope())
            {
                Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.Maintenance));
            }
            Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.Maintenance));
        }

        Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.User));
    }

    [Test]
    public void Scope_dispose_is_idempotent()
    {
        var scope = LatticeMaintenanceContext.BeginScope();
        scope.Dispose();
        scope.Dispose();

        Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.User));
    }

    [Test]
    public async Task Scope_propagates_across_async_boundaries()
    {
        using (LatticeMaintenanceContext.BeginScope())
        {
            await Task.Yield();
            Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.Maintenance));
        }

        await Task.Yield();
        Assert.That(LatticeMaintenanceContext.Current, Is.EqualTo(MutationCategory.User));
    }

    [Test]
    public void Default_LatticeMutation_has_User_category_for_wire_compat()
    {
        var m = new LatticeMutation();
        Assert.That(m.Category, Is.EqualTo(MutationCategory.User));
    }
}
