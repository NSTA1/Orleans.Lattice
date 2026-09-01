using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Schema.Components;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The four platform areas' sub-surface vocabulary and the shell-state contract
/// they persist and address it through.
/// </summary>
/// <remarks>
/// <para>
/// The acceptance criterion is "sub-surface and query state are URL-addressable
/// with lower-case paths and persist". Two halves have to hold and are easy to
/// get half-right: the ids have to be legal <em>route slugs</em>, or an address
/// naming one throws rather than navigating; and the same string has to be what
/// the preference stores, or a link and a return visit disagree about the
/// spelling of the surface they name.
/// </para>
/// <para>
/// Every assertion reads a static table or a pure route type, so nothing here
/// depends on timing, ordering, a wall clock, or garbage collection.
/// </para>
/// </remarks>
[TestFixture]
public sealed class PlatformAreaSurfaceStateTests
{
    private static IEnumerable<TestCaseData> EverySurfaceSlug()
    {
        foreach (var slug in AllSlugs())
        {
            yield return new TestCaseData(slug).SetArgDisplayNames(slug);
        }
    }

    private static IEnumerable<string> AllSlugs()
    {
        yield return BackupsSurfaces.New;
        yield return BackupsSurfaces.Existing;
        yield return SchemaSurfaces.Policy;
        yield return SchemaSurfaces.Versions;
        yield return SchemaSurfaces.DeadLetters;
        yield return AccessSurfaces.Groups;
        yield return AccessSurfaces.Policies;
        yield return AccessSurfaces.Explain;
    }

    private static IEnumerable<TestCaseData> EveryDeclaredKey()
    {
        yield return new TestCaseData(BackupsPluginKeys.SurfacePreference).SetArgDisplayNames("backups.surface");
        yield return new TestCaseData(SchemaPluginKeys.SurfacePreference).SetArgDisplayNames("schema.surface");
        yield return new TestCaseData(AccessPluginKeys.SurfacePreference).SetArgDisplayNames("access.surface");
        yield return new TestCaseData(TelemetryPluginKeys.SelectedQueryPreference)
            .SetArgDisplayNames("telemetry.query");
    }

    [TestCaseSource(nameof(EverySurfaceSlug))]
    public void Every_sub_surface_id_is_a_legal_lower_case_route_slug(string slug)
    {
        // Not merely a convention: ExplorerRoute.WithSurface throws on anything
        // that is not canonical, so a surface id that is not a slug is a surface
        // no address can name.
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerRouteSlug.IsCanonical(slug), Is.True);
            Assert.That(slug, Is.EqualTo(slug.ToLowerInvariant()));
        });
    }

    [TestCaseSource(nameof(EverySurfaceSlug))]
    public void Every_sub_surface_id_survives_a_round_trip_through_the_address(string slug)
    {
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface(slug);

        var round = ExplorerRoutePath.Parse(ExplorerRoutePath.Format(route)).Route;

        Assert.That(round.Surface, Is.EqualTo(slug));
    }

    [Test]
    public void Every_area_resolves_its_own_slugs_and_rejects_one_it_does_not_offer()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsSurfaces.FromSlug(BackupsSurfaces.New), Is.EqualTo(BackupsSubTab.New));
            Assert.That(
                BackupsSurfaces.FromSlug(BackupsSurfaces.Existing),
                Is.EqualTo(BackupsSubTab.Existing));
            Assert.That(BackupsSurfaces.FromSlug("nope"), Is.Null);
            Assert.That(BackupsSurfaces.FromSlug(null), Is.Null);

            Assert.That(
                SchemaSurfaces.FromSlug(SchemaSurfaces.Policy),
                Is.EqualTo(SchemaPanel.SchemaTab.Policy));
            Assert.That(
                SchemaSurfaces.FromSlug(SchemaSurfaces.Versions),
                Is.EqualTo(SchemaPanel.SchemaTab.Versions));
            Assert.That(
                SchemaSurfaces.FromSlug(SchemaSurfaces.DeadLetters),
                Is.EqualTo(SchemaPanel.SchemaTab.DeadLetters));
            Assert.That(SchemaSurfaces.FromSlug("nope"), Is.Null);
            Assert.That(SchemaSurfaces.FromSlug(null), Is.Null);

            Assert.That(AccessSurfaces.IsKnown(AccessSurfaces.Groups), Is.True);
            Assert.That(AccessSurfaces.IsKnown(AccessSurfaces.Policies), Is.True);
            Assert.That(AccessSurfaces.IsKnown(AccessSurfaces.Explain), Is.True);
            Assert.That(AccessSurfaces.IsKnown("nope"), Is.False);
            Assert.That(AccessSurfaces.IsKnown(null), Is.False);
        });
    }

    [Test]
    public void Naming_a_surface_and_resolving_it_back_is_a_round_trip_for_every_sub_tab()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in Enum.GetValues<BackupsSubTab>())
            {
                Assert.That(BackupsSurfaces.FromSlug(BackupsSurfaces.SlugFor(tab)), Is.EqualTo(tab));
            }

            foreach (var tab in Enum.GetValues<SchemaPanel.SchemaTab>())
            {
                Assert.That(SchemaSurfaces.FromSlug(SchemaSurfaces.SlugFor(tab)), Is.EqualTo(tab));
            }
        });
    }

    [Test]
    public void Every_strip_offers_its_surfaces_once_in_display_order()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                BackupsSurfaces.Tabs.Select(tab => tab.Id),
                Is.EqualTo(new[] { BackupsSurfaces.New, BackupsSurfaces.Existing }));
            Assert.That(
                SchemaSurfaces.Tabs.Select(tab => tab.Id),
                Is.EqualTo(new[]
                {
                    SchemaSurfaces.Policy,
                    SchemaSurfaces.Versions,
                    SchemaSurfaces.DeadLetters,
                }));
            Assert.That(
                AccessSurfaces.Tabs.Select(tab => tab.Id),
                Is.EqualTo(new[]
                {
                    AccessSurfaces.Groups,
                    AccessSurfaces.Policies,
                    AccessSurfaces.Explain,
                }));
        });
    }

    [Test]
    public void Every_offered_tab_describes_itself_so_the_strip_can_associate_the_description()
    {
        // The primitive renders a described tab's Description into a
        // visually-hidden element and points aria-describedby at it, so an
        // undescribed tab silently loses that association.
        var described = BackupsSurfaces.Tabs
            .Concat(SchemaSurfaces.Tabs)
            .Concat(AccessSurfaces.Tabs);

        Assert.That(described.Select(tab => tab.Description), Has.All.Not.Null.And.All.Not.Empty);
    }

    [Test]
    public void Every_strip_returns_the_same_cached_list_so_a_render_allocates_nothing()
    {
        // These strips re-render on every operation the area performs, so a
        // property composing a fresh list per read would allocate on each one.
        Assert.Multiple(() =>
        {
            Assert.That(BackupsSurfaces.Tabs, Is.SameAs(BackupsSurfaces.Tabs));
            Assert.That(SchemaSurfaces.Tabs, Is.SameAs(SchemaSurfaces.Tabs));
            Assert.That(AccessSurfaces.Tabs, Is.SameAs(AccessSurfaces.Tabs));
        });
    }

    [TestCaseSource(nameof(EveryDeclaredKey))]
    public void Every_declared_preference_key_is_canonical_and_explains_itself(ExplorerPreferenceKey key)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerRouteSlug.IsCanonical(key.Name), Is.True);
            Assert.That(key.Description, Is.Not.Empty);
            Assert.That(
                key.Scope,
                Is.EqualTo(ExplorerPreferenceScope.UserAndCluster),
                "a surface inside a cluster is remembered per cluster, not globally");
        });
    }

    [Test]
    public void Every_declared_key_is_a_single_shared_instance_so_the_catalog_accepts_it_twice()
    {
        // Keys are compared by reference, and each area registers its own when
        // its panel mounts. A property composing a fresh key per read would make
        // the second circuit's registration throw.
        Assert.Multiple(() =>
        {
            Assert.That(BackupsPluginKeys.SurfacePreference, Is.SameAs(BackupsPluginKeys.SurfacePreference));
            Assert.That(SchemaPluginKeys.SurfacePreference, Is.SameAs(SchemaPluginKeys.SurfacePreference));
            Assert.That(AccessPluginKeys.SurfacePreference, Is.SameAs(AccessPluginKeys.SurfacePreference));
            Assert.That(
                TelemetryPluginKeys.SelectedQueryPreference,
                Is.SameAs(TelemetryPluginKeys.SelectedQueryPreference));
        });
    }

    [Test]
    public void Registering_the_same_area_key_twice_is_accepted_and_a_rival_spelling_is_not()
    {
        // The exact shape a second mounted circuit produces, and the shape a
        // second declaration of the same name would.
        var catalog = new ExplorerPreferenceCatalog();

        catalog.Register(BackupsPluginKeys.SurfacePreference);
        catalog.Register(BackupsPluginKeys.SurfacePreference);

        var rival = new ExplorerPreferenceKey(
            BackupsPluginKeys.SurfacePreference.Name,
            "a second declaration of one key");

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Keys, Does.Contain(BackupsPluginKeys.SurfacePreference));
            Assert.That(
                catalog.Keys.Count(key => key == BackupsPluginKeys.SurfacePreference),
                Is.EqualTo(1));
            Assert.That(() => catalog.Register(rival), Throws.InvalidOperationException);
        });
    }

    [Test]
    public void Every_declared_key_name_is_unique_across_the_four_areas()
    {
        var names = new[]
        {
            BackupsPluginKeys.SurfacePreference.Name,
            SchemaPluginKeys.SurfacePreference.Name,
            AccessPluginKeys.SurfacePreference.Name,
            TelemetryPluginKeys.SelectedQueryPreference.Name,
        };

        Assert.That(names, Is.Unique);
    }

    [Test]
    public void Every_area_surface_parameter_is_canonical_area_scoped_and_unique()
    {
        // The parameter carries the surface when the address has no catalogue
        // selection, which is the ordinary case for these three areas. Switching
        // area KEEPS the parameters, so a shared "surface" key would leak one
        // area's surface into another's address - which is why each is scoped to
        // its own area rather than sharing one name.
        var parameters = new[]
        {
            BackupsPluginKeys.SurfaceParameter,
            SchemaPluginKeys.SurfaceParameter,
            AccessPluginKeys.SurfaceParameter,
            TelemetryPluginKeys.SelectedQueryParameter,
        };

        Assert.Multiple(() =>
        {
            Assert.That(parameters, Is.Unique);
            Assert.That(parameters, Has.All.Matches<string>(ExplorerRouteSlug.IsCanonical));
        });
    }

    [Test]
    public void An_area_surface_parameter_survives_a_round_trip_through_the_address()
    {
        var route = ExplorerRoute.Home
            .WithParameter(BackupsPluginKeys.SurfaceParameter, BackupsSurfaces.Existing);

        var round = ExplorerRoutePath.Parse(ExplorerRoutePath.Format(route)).Route;

        Assert.That(
            round.Parameters.GetValueOrEmpty(BackupsPluginKeys.SurfaceParameter),
            Is.EqualTo(BackupsSurfaces.Existing));
    }

    [Test]
    public void The_selected_telemetry_panel_is_addressable_as_a_query_parameter()
    {
        // A cluster-authored catalogue id is an opaque value, not a slug this
        // area coins, so it belongs in a parameter rather than in a path
        // segment - and it must survive a round trip through the address
        // unchanged, including an id carrying characters a path would escape.
        const string queryId = "lattice/tree throughput";

        var route = ExplorerRoute.Home
            .WithParameter(TelemetryPluginKeys.SelectedQueryParameter, queryId);

        var round = ExplorerRoutePath.Parse(ExplorerRoutePath.Format(route)).Route;

        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerRouteSlug.IsCanonical(TelemetryPluginKeys.SelectedQueryParameter),
                Is.True,
                "a parameter key shares the route grammar's lower-case rule");
            Assert.That(
                round.Parameters.TryGetValue(TelemetryPluginKeys.SelectedQueryParameter, out var restored),
                Is.True);
            Assert.That(restored, Is.EqualTo(queryId));
        });
    }

    [Test]
    public void Re_addressing_the_surface_already_named_returns_the_same_route_instance()
    {
        // What lets each panel skip a navigation when nothing changed, so
        // selecting the open surface emits no address write at all.
        var route = ExplorerRoute.Home
            .WithSelection(ExplorerRouteSegments.Trees, "orders")
            .WithSurface(BackupsSurfaces.Existing);

        Assert.Multiple(() =>
        {
            Assert.That(route.WithSurface(BackupsSurfaces.Existing), Is.SameAs(route));
            Assert.That(
                route.WithParameter(TelemetryPluginKeys.SelectedQueryParameter, "q")
                    .WithParameter(TelemetryPluginKeys.SelectedQueryParameter, "q"),
                Is.EqualTo(route.WithParameter(TelemetryPluginKeys.SelectedQueryParameter, "q")));
        });
    }
}
