using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The shell's addressable state value: its nesting rules, its immutability, and
/// the value equality the router's echo suppression depends on.
/// </summary>
[TestFixture]
public sealed class ExplorerRouteTests
{
    [Test]
    public void Root_IsBareAndCarriesNothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerRoute.Root.IsBare, Is.True);
            Assert.That(ExplorerRoute.Root.Area, Is.EqualTo(string.Empty));
            Assert.That(ExplorerRoute.Root.Kind, Is.EqualTo(string.Empty));
            Assert.That(ExplorerRoute.Root.Id, Is.EqualTo(string.Empty));
            Assert.That(ExplorerRoute.Root.Surface, Is.EqualTo(string.Empty));
            Assert.That(ExplorerRoute.Root.Tenant, Is.EqualTo(string.Empty));
            Assert.That(ExplorerRoute.Root.AllTenants, Is.False);
            Assert.That(ExplorerRoute.Root.Parameters.Count, Is.Zero);
            Assert.That(ExplorerRoute.Root.HasSelection, Is.False);
        });
    }

    [Test]
    public void Home_IsTheExploreAreaAndIsNotBare()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerRoute.Home.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(ExplorerRoute.Home.IsBare, Is.False);
        });
    }

    [Test]
    public void WithArea_SetsTheArea()
    {
        Assert.That(ExplorerRoute.Root.WithArea("tenants").Area, Is.EqualTo("tenants"));
    }

    [Test]
    public void WithArea_SameArea_ReturnsTheSameInstance()
    {
        var route = ExplorerRoute.Root.WithArea("tenants");

        Assert.That(route.WithArea("tenants"), Is.SameAs(route));
    }

    [Test]
    public void WithArea_DifferentArea_DropsTheSelectionAndSurface()
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithArea("tenants");

        Assert.Multiple(() =>
        {
            Assert.That(route.Kind, Is.EqualTo(string.Empty));
            Assert.That(route.Id, Is.EqualTo(string.Empty));
            Assert.That(route.Surface, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void WithArea_DifferentArea_KeepsTheTenantScope()
    {
        var route = ExplorerRoute.Home
            .WithTenant("acme")
            .WithAllTenants(true)
            .WithArea("tenants");

        Assert.Multiple(() =>
        {
            Assert.That(route.Tenant, Is.EqualTo("acme"));
            Assert.That(route.AllTenants, Is.True);
        });
    }

    [Test]
    public void WithArea_UpperCase_Throws()
    {
        Assert.That(() => ExplorerRoute.Root.WithArea("Tenants"), Throws.ArgumentException);
    }

    [Test]
    public void WithSelection_FromRoot_ImpliesTheHomeArea()
    {
        var route = ExplorerRoute.Root.WithSelection(ExplorerRouteSegments.Trees, "orders");

        Assert.Multiple(() =>
        {
            Assert.That(route.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(route.Kind, Is.EqualTo(ExplorerRouteSegments.Trees));
            Assert.That(route.Id, Is.EqualTo("orders"));
            Assert.That(route.HasSelection, Is.True);
        });
    }

    [Test]
    public void WithSelection_KeepsTheCurrentSurface()
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithSelection(ExplorerRouteSegments.Trees, "invoices");

        Assert.Multiple(() =>
        {
            Assert.That(route.Id, Is.EqualTo("invoices"));
            Assert.That(route.Surface, Is.EqualTo("data"));
        });
    }

    [Test]
    public void WithSelection_PreservesTheIdVerbatim()
    {
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "t/Acme/Orders");

        Assert.That(route.Id, Is.EqualTo("t/Acme/Orders"));
    }

    [Test]
    public void WithSelection_UpperCaseKind_Throws()
    {
        Assert.That(
            () => ExplorerRoute.Home.WithSelection("Trees", "orders"),
            Throws.ArgumentException);
    }

    [Test]
    public void WithSelection_EmptyId_Throws()
    {
        Assert.That(
            () => ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, string.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public void WithSelection_NullId_Throws()
    {
        Assert.That(
            () => ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void WithoutSelection_DropsTheKindIdAndSurface()
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithoutSelection();

        Assert.Multiple(() =>
        {
            Assert.That(route.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(route.Kind, Is.EqualTo(string.Empty));
            Assert.That(route.Id, Is.EqualTo(string.Empty));
            Assert.That(route.Surface, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void WithoutSelection_WhenNothingSelected_ReturnsTheSameInstance()
    {
        Assert.That(ExplorerRoute.Home.WithoutSelection(), Is.SameAs(ExplorerRoute.Home));
    }

    [Test]
    public void WithKind_BrowsesThatKindWithNothingSelected()
    {
        var route = ExplorerRoute.Home.WithKind(ExplorerRouteSegments.Views);

        Assert.Multiple(() =>
        {
            Assert.That(route.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(route.Kind, Is.EqualTo(ExplorerRouteSegments.Views));
            Assert.That(route.HasSelection, Is.False);
            Assert.That(route.ToString(), Is.EqualTo("/explore/views"));
        });
    }

    [Test]
    public void WithKind_OnTheBareRoute_ImpliesTheHomeArea()
    {
        Assert.That(
            ExplorerRoute.Root.WithKind(ExplorerRouteSegments.Trees).Area,
            Is.EqualTo(ExplorerRouteSegments.Explore));
    }

    [Test]
    public void WithKind_DropsTheSelectionAndItsSurface()
    {
        // An id names something inside the kind being left, so it goes with it -
        // the same nesting rule WithArea applies one level up.
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithKind(ExplorerRouteSegments.Views);

        Assert.Multiple(() =>
        {
            Assert.That(route.Kind, Is.EqualTo(ExplorerRouteSegments.Views));
            Assert.That(route.Id, Is.EqualTo(string.Empty));
            Assert.That(route.Surface, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void WithKind_KeepsTheContributedAreaItIsCalledOn()
    {
        var route = ExplorerRoute.Home.WithArea("tenants").WithKind("detail");

        Assert.That(route.ToString(), Is.EqualTo("/area/tenants/detail"));
    }

    [Test]
    public void WithKind_KeepsTheTenantScope()
    {
        var route = ExplorerRoute.Home
            .WithTenant("acme")
            .WithAllTenants(true)
            .WithKind(ExplorerRouteSegments.Views);

        Assert.Multiple(() =>
        {
            Assert.That(route.Tenant, Is.EqualTo("acme"));
            Assert.That(route.AllTenants, Is.True);
        });
    }

    [Test]
    public void WithKind_WithNoChange_ReturnsTheSameInstance()
    {
        var route = ExplorerRoute.Home.WithKind(ExplorerRouteSegments.Trees);

        Assert.That(route.WithKind(ExplorerRouteSegments.Trees), Is.SameAs(route));
    }

    [TestCase(null)]
    [TestCase("")]
    public void WithKind_WithNoKind_BrowsesTheAreaAlone(string? kind)
    {
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders").WithKind(kind);

        Assert.Multiple(() =>
        {
            Assert.That(route.Kind, Is.EqualTo(string.Empty));
            Assert.That(route.ToString(), Is.EqualTo("/explore"));
        });
    }

    [Test]
    public void WithKind_WithANonCanonicalKind_Throws()
    {
        Assert.That(() => ExplorerRoute.Home.WithKind("Trees"), Throws.ArgumentException);
    }

    [Test]
    public void WithSurface_WithNoSelection_IsIgnored()
    {
        var route = ExplorerRoute.Home.WithSurface("data");

        Assert.Multiple(() =>
        {
            Assert.That(route, Is.SameAs(ExplorerRoute.Home));
            Assert.That(route.Surface, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void WithSurface_SameSurface_ReturnsTheSameInstance()
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data");

        Assert.That(route.WithSurface("data"), Is.SameAs(route));
    }

    [Test]
    public void WithSurface_Null_ClearsIt()
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithSurface(null);

        Assert.That(route.Surface, Is.EqualTo(string.Empty));
    }

    [Test]
    public void WithSurface_UpperCase_Throws()
    {
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders");

        Assert.That(() => route.WithSurface("Data"), Throws.ArgumentException);
    }

    [Test]
    public void WithTenant_SetsAndClears()
    {
        var scoped = ExplorerRoute.Home.WithTenant("acme");

        Assert.Multiple(() =>
        {
            Assert.That(scoped.Tenant, Is.EqualTo("acme"));
            Assert.That(scoped.WithTenant(null).Tenant, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void WithTenant_SameTenant_ReturnsTheSameInstance()
    {
        var route = ExplorerRoute.Home.WithTenant("acme");

        Assert.That(route.WithTenant("acme"), Is.SameAs(route));
    }

    [Test]
    public void WithAllTenants_SameValue_ReturnsTheSameInstance()
    {
        Assert.That(ExplorerRoute.Home.WithAllTenants(false), Is.SameAs(ExplorerRoute.Home));
    }

    [Test]
    public void WithAllTenants_Toggles()
    {
        Assert.That(ExplorerRoute.Home.WithAllTenants(true).AllTenants, Is.True);
    }

    [Test]
    public void WithParameter_AddsToTheQuery()
    {
        var route = ExplorerRoute.Home.WithParameter("page", "3");

        Assert.That(route.Parameters.GetValueOrEmpty("page"), Is.EqualTo("3"));
    }

    [Test]
    public void WithParameter_UnchangedValue_ReturnsTheSameInstance()
    {
        var route = ExplorerRoute.Home.WithParameter("page", "3");

        Assert.That(route.WithParameter("page", "3"), Is.SameAs(route));
    }

    [Test]
    public void WithParameters_Null_ClearsThem()
    {
        var route = ExplorerRoute.Home.WithParameter("page", "3").WithParameters(null);

        Assert.That(route.Parameters.Count, Is.Zero);
    }

    [Test]
    public void WithParameters_EquivalentSet_ReturnsTheSameInstance()
    {
        var route = ExplorerRoute.Home.WithParameter("page", "3");

        Assert.That(
            route.WithParameters(ExplorerRouteParameters.Empty.With("page", "3")),
            Is.SameAs(route));
    }

    [Test]
    public void Equals_SameState_IsTrue()
    {
        var left = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithTenant("acme")
            .WithParameter("page", "3");
        var right = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data")
            .WithTenant("acme")
            .WithParameter("page", "3");

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void Equals_DifferentParameters_IsFalse()
    {
        var left = ExplorerRoute.Home.WithParameter("page", "3");
        var right = ExplorerRoute.Home.WithParameter("page", "4");

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void ToString_ReturnsTheFormattedAddress()
    {
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders");

        Assert.That(route.ToString(), Is.EqualTo("/explore/trees/orders"));
    }
}
