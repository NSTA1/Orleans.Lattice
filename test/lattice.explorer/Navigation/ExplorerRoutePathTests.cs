using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The route grammar: what the shell emits, what it accepts, and how it degrades
/// when an address cannot be understood.
/// </summary>
[TestFixture]
public sealed class ExplorerRoutePathTests
{
    [Test]
    public void Format_Root_IsTheBareAddress()
    {
        Assert.That(ExplorerRoutePath.Format(ExplorerRoute.Root), Is.EqualTo("/"));
    }

    [Test]
    public void Format_Null_Throws()
    {
        Assert.That(() => ExplorerRoutePath.Format(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Format_AreaOnly_EmitsOneSegment()
    {
        Assert.That(ExplorerRoutePath.Format(ExplorerRoute.Home), Is.EqualTo("/explore"));
    }

    [Test]
    public void Format_FullSelection_EmitsTheApprovedShape()
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface("data");

        Assert.That(ExplorerRoutePath.Format(route), Is.EqualTo("/explore/trees/orders/data"));
    }

    [Test]
    public void Format_IdWithSlash_EscapesItIntoOneSegment()
    {
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "t/acme/orders");

        Assert.That(
            ExplorerRoutePath.Format(route),
            Is.EqualTo("/explore/trees/t%2Facme%2Forders"));
    }

    [Test]
    public void Format_Tenant_GoesInTheQueryString()
    {
        var route = ExplorerRoute.Home.WithTenant("acme corp");

        Assert.That(ExplorerRoutePath.Format(route), Is.EqualTo("/explore?tenant=acme%20corp"));
    }

    [Test]
    public void Format_AllTenants_IsEmittedOnlyWhenOn()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerRoutePath.Format(ExplorerRoute.Home.WithAllTenants(true)),
                Is.EqualTo("/explore?all-tenants=true"));
            Assert.That(
                ExplorerRoutePath.Format(ExplorerRoute.Home.WithAllTenants(false)),
                Is.EqualTo("/explore"));
        });
    }

    [Test]
    public void Format_ExtraParameters_FollowTheShellKeysInKeyOrder()
    {
        var route = ExplorerRoute.Home
            .WithAllTenants(true)
            .WithTenant("acme")
            .WithParameter("zeta", "1")
            .WithParameter("alpha", "2");

        Assert.That(
            ExplorerRoutePath.Format(route),
            Is.EqualTo("/explore?all-tenants=true&tenant=acme&alpha=2&zeta=1"));
    }

    [Test]
    public void Format_TenantScopeWithNoArea_KeepsTheBarePath()
    {
        var route = ExplorerRoute.Root.WithTenant("acme");

        Assert.That(ExplorerRoutePath.Format(route), Is.EqualTo("/?tenant=acme"));
    }

    [Test]
    public void Format_LongAddress_GrowsBeyondTheStackBuffer()
    {
        var id = new string('x', 400);
        var route = ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, id);

        Assert.That(ExplorerRoutePath.Format(route), Is.EqualTo("/explore/trees/" + id));
    }

    [Test]
    public void Format_ContributedArea_IsNamespacedUnderTheAreaLiteral()
    {
        Assert.That(
            ExplorerRoutePath.Format(ExplorerRoute.Root.WithArea("tenants")),
            Is.EqualTo("/area/tenants"));
    }

    [Test]
    public void Format_HomeArea_KeepsTheApprovedShortShape()
    {
        Assert.That(ExplorerRoutePath.Format(ExplorerRoute.Home), Is.EqualTo("/explore"));
    }

    [Test]
    public void Format_ContributedAreaWithFullSelection_NamespacesOnlyTheArea()
    {
        var route = ExplorerRoute.Root
            .WithArea("tenants")
            .WithSelection("detail", "acme")
            .WithSurface("quotas");

        Assert.That(ExplorerRoutePath.Format(route), Is.EqualTo("/area/tenants/detail/acme/quotas"));
    }

    [Test]
    public void Format_AnAreaCalledArea_StillRoundTrips()
    {
        // The namespace literal is consumed positionally, so a contributed slug
        // that happens to be 'area' is not a special case.
        var route = ExplorerRoute.Root.WithArea(ExplorerRouteSegments.AreaPathPrefix);
        var address = ExplorerRoutePath.Format(route);

        Assert.Multiple(() =>
        {
            Assert.That(address, Is.EqualTo("/area/area"));
            Assert.That(ExplorerRoutePath.Parse(address).Route, Is.EqualTo(route));
        });
    }

    [Test]
    public void Parse_ContributedArea_ReadsPastTheNamespace()
    {
        var parsed = ExplorerRoutePath.Parse("/area/tenants/detail/acme/quotas");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Area, Is.EqualTo("tenants"));
            Assert.That(parsed.Route.Kind, Is.EqualTo("detail"));
            Assert.That(parsed.Route.Id, Is.EqualTo("acme"));
            Assert.That(parsed.Route.Surface, Is.EqualTo("quotas"));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
        });
    }

    [Test]
    public void Parse_AnUnNamespacedArea_IsAcceptedAndReportedNormalized()
    {
        // Forgiving on the way in, strict on the way out: a hand-typed
        // '/tenants' still lands, and the shell rewrites the address bar.
        var parsed = ExplorerRoutePath.Parse("/tenants");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Area, Is.EqualTo("tenants"));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
            Assert.That(ExplorerRoutePath.Format(parsed.Route), Is.EqualTo("/area/tenants"));
        });
    }

    [Test]
    public void Parse_TheNamespaceWithNoArea_IsMalformedAndDegradesToTheBareRoute()
    {
        var parsed = ExplorerRoutePath.Parse("/area");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(parsed.Route.IsBare, Is.True);
            Assert.That(parsed.ShouldCanonicalize, Is.True);
        });
    }

    [Test]
    public void Parse_TheHomeAreaWrittenUnderTheNamespace_NormalizesToTheShortShape()
    {
        var parsed = ExplorerRoutePath.Parse("/area/explore/trees");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(parsed.Route.Kind, Is.EqualTo(ExplorerRouteSegments.Trees));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
            Assert.That(ExplorerRoutePath.Format(parsed.Route), Is.EqualTo("/explore/trees"));
        });
    }

    [Test]
    public void Parse_ANamespacedAddressOneSegmentTooDeep_IsMalformed()
    {
        var parsed = ExplorerRoutePath.Parse("/area/tenants/detail/acme/quotas/extra");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(parsed.Route.Surface, Is.EqualTo("quotas"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("/")]
    [TestCase("   ")]
    public void Parse_BareAddress_IsBare(string? address)
    {
        var parsed = ExplorerRoutePath.Parse(address);

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Bare));
            Assert.That(parsed.Route, Is.SameAs(ExplorerRoute.Root));
            Assert.That(parsed.IsUnderstood, Is.True);
            Assert.That(parsed.ShouldCanonicalize, Is.False);
        });
    }

    [Test]
    public void Parse_FullSelection_RoundTripsTheRoute()
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees/orders/data");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
            Assert.That(parsed.Route.Area, Is.EqualTo("explore"));
            Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));
            Assert.That(parsed.Route.Id, Is.EqualTo("orders"));
            Assert.That(parsed.Route.Surface, Is.EqualTo("data"));
        });
    }

    [Test]
    public void Parse_EscapedId_RestoresTheSlashes()
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees/t%2Facme%2Forders");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Id, Is.EqualTo("t/acme/orders"));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
        });
    }

    [Test]
    public void Parse_BaseRelativeAddress_IsAccepted()
    {
        var parsed = ExplorerRoutePath.Parse("explore/trees");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Area, Is.EqualTo("explore"));
            Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));

            // The formatter would have written a leading slash, so the address as
            // given is not the canonical one.
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
        });
    }

    [Test]
    public void Parse_AbsoluteAddress_IsReducedToPathAndQuery()
    {
        var parsed = ExplorerRoutePath.Parse("https://explorer.example:8443/explore/trees?tenant=acme");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));
            Assert.That(parsed.Route.Tenant, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void Parse_AbsoluteAddressWithNoPath_IsBare()
    {
        Assert.That(
            ExplorerRoutePath.Parse("https://explorer.example").Status,
            Is.EqualTo(ExplorerRouteStatus.Bare));
    }

    [Test]
    public void Parse_Fragment_IsIgnored()
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees#anchor");

        Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));
    }

    [Test]
    public void Parse_UpperCaseSegments_AreFoldedAndReportedNormalized()
    {
        var parsed = ExplorerRoutePath.Parse("/Explore/Trees");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Area, Is.EqualTo("explore"));
            Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
            Assert.That(parsed.ShouldCanonicalize, Is.True);
            Assert.That(parsed.IsUnderstood, Is.True);
        });
    }

    [Test]
    public void Parse_TrailingSlash_IsNormalized()
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees/");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
        });
    }

    [Test]
    public void Parse_AllTenantsAsOne_IsAcceptedAndNormalized()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?all-tenants=1");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.AllTenants, Is.True);
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
        });
    }

    [Test]
    public void Parse_AllTenantsFalse_LeavesTheFlagOff()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?all-tenants=false");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.AllTenants, Is.False);
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
        });
    }

    [Test]
    public void Parse_EmptyQueryValue_DropsTheParameter()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?page=");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Parameters.Count, Is.Zero);
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
        });
    }

    [Test]
    public void Parse_ValuelessQueryKey_DropsTheParameter()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?page");

        Assert.That(parsed.Route.Parameters.Count, Is.Zero);
    }

    [Test]
    public void Parse_RepeatedAmpersands_AreIgnored()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?&tenant=acme&");

        Assert.That(parsed.Route.Tenant, Is.EqualTo("acme"));
    }

    [Test]
    public void Parse_ExtraParameters_AreCarried()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?alpha=2&zeta=1");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Route.Parameters.GetValueOrEmpty("alpha"), Is.EqualTo("2"));
            Assert.That(parsed.Route.Parameters.GetValueOrEmpty("zeta"), Is.EqualTo("1"));
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
        });
    }

    [Test]
    public void Parse_TooManySegments_IsMalformedButStillAddressable()
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees/orders/data/extra");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(parsed.IsUnderstood, Is.False);
            Assert.That(parsed.Route.Surface, Is.EqualTo("data"));
            Assert.That(parsed.ShouldCanonicalize, Is.True);
        });
    }

    [Test]
    public void Parse_BadEscapeInId_IsMalformedAndDropsTheSelection()
    {
        var parsed = ExplorerRoutePath.Parse("/explore/trees/%zz/data");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(parsed.Route.Id, Is.EqualTo(string.Empty));
            Assert.That(parsed.Route.Kind, Is.EqualTo("trees"));
        });
    }

    [Test]
    public void Parse_BadEscapeInQueryValue_IsMalformed()
    {
        var parsed = ExplorerRoutePath.Parse("/explore?tenant=%zz");

        Assert.Multiple(() =>
        {
            Assert.That(parsed.Status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(parsed.Route.Area, Is.EqualTo("explore"));
        });
    }

    [Test]
    public void Parse_SurfaceWithNoId_DropsTheSurfaceAndIsMalformed()
    {
        // Three segments where the third normalises away leaves a surface with
        // nothing to qualify.
        var parsed = ExplorerRoutePath.Parse("/explore/trees/%zz/data");

        Assert.That(parsed.Route.Surface, Is.EqualTo(string.Empty));
    }

    [Test]
    public void Parse_NeverThrows_ForAnyShapeOfGarbage()
    {
        string[] garbage =
        [
            "//////",
            "?=",
            "#",
            "/%",
            "/a?b=%",
            "///a///b///c///d///e///",
            "://",
            "/?&&&",
        ];

        Assert.Multiple(() =>
        {
            foreach (var address in garbage)
            {
                Assert.That(
                    () => ExplorerRoutePath.Parse(address),
                    Throws.Nothing,
                    $"'{address}' must degrade rather than throw");
                Assert.That(ExplorerRoutePath.Parse(address).Route, Is.Not.Null);
            }
        });
    }

    [Test]
    public void Parse_ThenFormat_RoundTripsEveryCanonicalAddress()
    {
        string[] addresses =
        [
            "/",
            "/explore",
            "/explore/trees",
            "/explore/trees/orders",
            "/explore/trees/orders/data",
            "/explore/trees/t%2Facme%2Forders/data",
            "/area/tenants?tenant=acme",
            "/explore?all-tenants=true",
            "/explore?all-tenants=true&tenant=acme&alpha=2&zeta=1",
            "/?tenant=acme",
        ];

        Assert.Multiple(() =>
        {
            foreach (var address in addresses)
            {
                var parsed = ExplorerRoutePath.Parse(address);
                Assert.That(
                    ExplorerRoutePath.Format(parsed.Route),
                    Is.EqualTo(address),
                    $"'{address}' must round-trip");
                Assert.That(
                    parsed.Status,
                    Is.AnyOf(ExplorerRouteStatus.Canonical, ExplorerRouteStatus.Bare),
                    $"'{address}' is already canonical");
            }
        });
    }
}
