namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The Explorer shell's addressable navigation state: which area is showing,
/// what is selected inside it, which detail surface that selection is open on,
/// and the tenant scope the whole view is read under.
/// </summary>
/// <remarks>
/// <para>
/// <b>The division of labour with preferences.</b> This type is
/// <em>where you are</em>. It maps one-to-one onto a URL, so back, forward,
/// reload, bookmark, deep link and share all work by construction. What is
/// remembered <em>between</em> sessions - where you were last time, and how you
/// like the shell to look - is the preference contract's job
/// (<see cref="Session.IExplorerShellPreferences"/>). A bare <c>/</c> is the one
/// address that carries no state, and is therefore the signal to restore from
/// preferences; any other URL is explicit and always wins.
/// </para>
/// <para>
/// <b>The grammar.</b> Formatted by <see cref="ExplorerRoutePath"/> as
/// <c>/explore/{kind}/{id}/{surface}</c> for the home area and
/// <c>/area/{area}/{kind}/{id}/{surface}</c> for a contributed one, with the
/// tenant scope in the query
/// string, for example <c>/explore/trees/orders/data?tenant=acme</c>. The
/// segments nest: a selection needs an area, an id needs a kind, and a surface
/// needs an id, so dropping an outer segment drops the inner ones with it. That
/// is enforced here rather than left to callers, which is what stops a
/// half-populated route reaching the URL.
/// </para>
/// <para>
/// <b>Case.</b> <see cref="Area"/>, <see cref="Kind"/> and <see cref="Surface"/>
/// are canonical lower-case slugs, guarded by <see cref="ExplorerRouteSlug"/>.
/// <see cref="Id"/> and <see cref="Tenant"/> are opaque values the cluster owns
/// (a tree id may contain a slash and mixed case), so they are escaped rather
/// than slugged.
/// </para>
/// <para>
/// Value equality is what lets the router recognise the echo of its own
/// navigation and suppress it without any timing or sequencing assumptions.
/// </para>
/// </remarks>
public sealed record ExplorerRoute
{
    private ExplorerRoute(
        string area,
        string kind,
        string id,
        string surface,
        string tenant,
        bool allTenants,
        ExplorerRouteParameters parameters)
    {
        Area = area;
        Kind = kind;
        Id = id;
        Surface = surface;
        Tenant = tenant;
        AllTenants = allTenants;
        Parameters = parameters;
    }

    /// <summary>
    /// The bare route, <c>/</c>. Carries no state at all, which is precisely its
    /// meaning: the shell restores the remembered view rather than showing a
    /// default one. <see cref="IsBare"/> is <see langword="true"/>.
    /// </summary>
    public static ExplorerRoute Root { get; } = new(
        string.Empty,
        string.Empty,
        string.Empty,
        string.Empty,
        string.Empty,
        allTenants: false,
        ExplorerRouteParameters.Empty);

    /// <summary>
    /// The shell's home area with nothing selected, <c>/explore</c>. Distinct
    /// from <see cref="Root"/>: this one is explicit, so it overrides the
    /// remembered view instead of restoring it.
    /// </summary>
    public static ExplorerRoute Home { get; } = Root.WithArea(ExplorerRouteSegments.Explore);

    /// <summary>
    /// The area slug, for example <see cref="ExplorerRouteSegments.Explore"/> or
    /// a plugin's own slug. Empty only on <see cref="Root"/>.
    /// </summary>
    public string Area { get; }

    /// <summary>
    /// The selection-kind slug within the area - <see cref="ExplorerRouteSegments.Trees"/>,
    /// <see cref="ExplorerRouteSegments.Views"/> or
    /// <see cref="ExplorerRouteSegments.TagIndexes"/> for the home area. Empty
    /// when nothing is selected.
    /// </summary>
    public string Kind { get; }

    /// <summary>
    /// The selected item's id, opaque and cluster-owned (a tree id may contain a
    /// slash). Escaped when formatted. Empty when nothing is selected.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// The detail-surface slug the selection is open on, for example <c>data</c>.
    /// Derive it from a plugin id with <see cref="ExplorerRouteSlug.FromIdentifier"/>.
    /// Empty when the selection is on its default surface.
    /// </summary>
    public string Surface { get; }

    /// <summary>
    /// The tenant id the view is scoped to, carried as
    /// <see cref="ExplorerRouteSegments.TenantQueryKey"/>. Empty means the link
    /// does not pin a tenant, so the session's own scope applies.
    /// </summary>
    public string Tenant { get; }

    /// <summary>
    /// Whether the view spans every reachable tenant, carried as
    /// <see cref="ExplorerRouteSegments.AllTenantsQueryKey"/> and emitted only
    /// when <see langword="true"/>.
    /// </summary>
    public bool AllTenants { get; }

    /// <summary>
    /// Extra query parameters a downstream surface has put in the URL. The
    /// extension point that keeps the shell's grammar closed while surface state
    /// stays addressable.
    /// </summary>
    public ExplorerRouteParameters Parameters { get; }

    /// <summary>
    /// Whether this is the bare route. The signal to restore the remembered view
    /// rather than treat the address as an instruction.
    /// </summary>
    public bool IsBare => Area.Length == 0;

    /// <summary>Whether the route names a selection (both a kind and an id).</summary>
    public bool HasSelection => Id.Length != 0;

    /// <summary>
    /// Returns this route in <paramref name="area"/>. Switching area drops the
    /// selection and surface with it, because they name something inside the area
    /// being left.
    /// </summary>
    /// <param name="area">The canonical lower-case area slug.</param>
    /// <exception cref="ArgumentException"><paramref name="area"/> is not canonical lower case.</exception>
    public ExplorerRoute WithArea(string area)
    {
        ExplorerRouteSlug.EnsureCanonical(area);

        if (string.Equals(Area, area, StringComparison.Ordinal))
        {
            return this;
        }

        return new ExplorerRoute(
            area,
            string.Empty,
            string.Empty,
            string.Empty,
            Tenant,
            AllTenants,
            Parameters);
    }

    /// <summary>
    /// Returns this route with <paramref name="kind"/> and <paramref name="id"/>
    /// selected, keeping the current surface. Selecting inside
    /// <see cref="Root"/> implies the home area, so a caller does not have to
    /// name it.
    /// </summary>
    /// <param name="kind">The canonical lower-case selection-kind slug.</param>
    /// <param name="id">The selected item's id. Must not be empty.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="kind"/> is not canonical lower case, or
    /// <paramref name="id"/> is <see langword="null"/> or empty.
    /// </exception>
    public ExplorerRoute WithSelection(string kind, string id)
    {
        ExplorerRouteSlug.EnsureCanonical(kind);
        ArgumentException.ThrowIfNullOrEmpty(id);

        var area = Area.Length == 0 ? ExplorerRouteSegments.Explore : Area;
        return new ExplorerRoute(area, kind, id, Surface, Tenant, AllTenants, Parameters);
    }

    /// <summary>
    /// Returns this route browsing <paramref name="kind"/> with nothing
    /// selected, as <c>/explore/trees</c>. Browsing inside <see cref="Root"/>
    /// implies the home area, so a caller does not have to name it.
    /// </summary>
    /// <remarks>
    /// The companion to <see cref="WithSelection"/> for the tier above it: a
    /// catalog-kind toggle addresses a <em>list</em>, not an item, and without
    /// this the only way to put a kind in the URL would be to select something
    /// in it. Changing kind drops the selection and its surface, because an id
    /// names something inside the kind being left - the same nesting rule
    /// <see cref="WithArea"/> applies one level up.
    /// </remarks>
    /// <param name="kind">
    /// The canonical lower-case selection-kind slug, or <see langword="null"/> /
    /// empty to browse the area with no kind chosen.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="kind"/> is non-empty and not canonical lower case.</exception>
    public ExplorerRoute WithKind(string? kind)
    {
        var next = kind ?? string.Empty;
        if (next.Length != 0)
        {
            ExplorerRouteSlug.EnsureCanonical(next);
        }

        var area = Area.Length == 0 ? ExplorerRouteSegments.Explore : Area;

        if (string.Equals(Area, area, StringComparison.Ordinal)
            && string.Equals(Kind, next, StringComparison.Ordinal)
            && Id.Length == 0
            && Surface.Length == 0)
        {
            return this;
        }

        return new ExplorerRoute(area, next, string.Empty, string.Empty, Tenant, AllTenants, Parameters);
    }

    /// <summary>
    /// Returns this route with no selection, and therefore no surface, staying in
    /// the current area.
    /// </summary>
    public ExplorerRoute WithoutSelection() =>
        Id.Length == 0 && Kind.Length == 0 && Surface.Length == 0
            ? this
            : new ExplorerRoute(Area, string.Empty, string.Empty, string.Empty, Tenant, AllTenants, Parameters);

    /// <summary>
    /// Returns this route open on <paramref name="surface"/>. Ignored when
    /// nothing is selected: a surface addresses a selection, so there is nothing
    /// for it to qualify.
    /// </summary>
    /// <param name="surface">
    /// The canonical lower-case surface slug, or <see langword="null"/> / empty
    /// to fall back to the selection's default surface.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="surface"/> is non-empty and not canonical lower case.</exception>
    public ExplorerRoute WithSurface(string? surface)
    {
        var next = surface ?? string.Empty;
        if (next.Length != 0)
        {
            ExplorerRouteSlug.EnsureCanonical(next);
        }

        if (Id.Length == 0 || string.Equals(Surface, next, StringComparison.Ordinal))
        {
            return this;
        }

        return new ExplorerRoute(Area, Kind, Id, next, Tenant, AllTenants, Parameters);
    }

    /// <summary>
    /// Returns this route pinned to <paramref name="tenant"/>, or unpinned when
    /// <paramref name="tenant"/> is <see langword="null"/> or empty.
    /// </summary>
    /// <param name="tenant">The tenant id, opaque and cluster-owned.</param>
    public ExplorerRoute WithTenant(string? tenant)
    {
        var next = tenant ?? string.Empty;
        return string.Equals(Tenant, next, StringComparison.Ordinal)
            ? this
            : new ExplorerRoute(Area, Kind, Id, Surface, next, AllTenants, Parameters);
    }

    /// <summary>Returns this route with all-tenants visibility set to <paramref name="allTenants"/>.</summary>
    /// <param name="allTenants">Whether the view spans every reachable tenant.</param>
    public ExplorerRoute WithAllTenants(bool allTenants) =>
        AllTenants == allTenants
            ? this
            : new ExplorerRoute(Area, Kind, Id, Surface, Tenant, allTenants, Parameters);

    /// <summary>
    /// Returns this route carrying <paramref name="key"/> set to
    /// <paramref name="value"/> in the query string. An empty or
    /// <see langword="null"/> value removes the key.
    /// </summary>
    /// <param name="key">The canonical lower-case query key.</param>
    /// <param name="value">The raw value, or empty to remove the key.</param>
    /// <exception cref="ArgumentException"><paramref name="key"/> is not canonical lower case.</exception>
    public ExplorerRoute WithParameter(string key, string? value)
    {
        var parameters = Parameters.With(key, value);
        return ReferenceEquals(parameters, Parameters)
            ? this
            : new ExplorerRoute(Area, Kind, Id, Surface, Tenant, AllTenants, parameters);
    }

    /// <summary>Returns this route with the whole extra-parameter set replaced.</summary>
    /// <param name="parameters">The parameters to carry. <see langword="null"/> clears them.</param>
    public ExplorerRoute WithParameters(ExplorerRouteParameters? parameters)
    {
        var next = parameters ?? ExplorerRouteParameters.Empty;
        return next.Equals(Parameters)
            ? this
            : new ExplorerRoute(Area, Kind, Id, Surface, Tenant, AllTenants, next);
    }

    /// <summary>The route's URL form, as produced by <see cref="ExplorerRoutePath.Format"/>.</summary>
    public override string ToString() => ExplorerRoutePath.Format(this);

    /// <summary>
    /// Creates a route from already-validated parts. The parser's entry point:
    /// it has normalised each segment itself and must not re-enter the
    /// hierarchy-enforcing <c>With</c> path.
    /// </summary>
    internal static ExplorerRoute FromParts(
        string area,
        string kind,
        string id,
        string surface,
        string tenant,
        bool allTenants,
        ExplorerRouteParameters parameters)
    {
        // Enforce the nesting here too, so a URL that names a surface with no id
        // (or an id with no kind) degrades to the nearest addressable view
        // instead of producing a route the formatter could not reproduce.
        if (area.Length == 0)
        {
            return parameters.Count == 0 && tenant.Length == 0 && !allTenants
                ? Root
                : new ExplorerRoute(string.Empty, string.Empty, string.Empty, string.Empty, tenant, allTenants, parameters);
        }

        if (kind.Length == 0)
        {
            return new ExplorerRoute(area, string.Empty, string.Empty, string.Empty, tenant, allTenants, parameters);
        }

        if (id.Length == 0)
        {
            return new ExplorerRoute(area, kind, string.Empty, string.Empty, tenant, allTenants, parameters);
        }

        return new ExplorerRoute(area, kind, id, surface, tenant, allTenants, parameters);
    }
}
