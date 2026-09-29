using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's address completions: <c>a/{slug}</c> (an installed app's
/// overview) and <c>a/{slug}/open</c> (its UI) for apps the caller can see, and
/// <c>app:{slug}</c> catalogue entries - one per offering source - only for an
/// <c>AppInstall</c> holder.
/// </summary>
/// <param name="access">The circuit's probe of the caller's rights and apps.</param>
/// <param name="tenancy">The active tenant, which every completion is rooted at.</param>
internal sealed class AppsCompletionSource(AppsAccess access, ExplorerTenancy tenancy) : IAddressCompletionSource
{
    /// <summary>The prefix of a catalogue completion's label.</summary>
    public const string CataloguePrefix = "app:";

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (query.Mode is not (AddressQueryMode.App or AddressQueryMode.Search))
        {
            return [];
        }

        var snapshot = await access.GetAsync(cancellationToken).ConfigureAwait(false);
        var tenant = tenancy.ActiveTenant;
        var results = new List<AddressCompletion>(query.Limit);
        var text = query.Text.Trim();

        var catalogueOnly = query.Mode == AddressQueryMode.Search && text.StartsWith(CataloguePrefix, StringComparison.OrdinalIgnoreCase);
        if (catalogueOnly)
        {
            text = text[CataloguePrefix.Length..].Trim();
        }

        if (!catalogueOnly)
        {
            AddInstalled(results, snapshot, tenant, text, query);
        }

        if (query.Mode == AddressQueryMode.Search && snapshot.CanReview && results.Count < query.Limit)
        {
            var index = await access.GetCompletionIndexAsync(cancellationToken).ConfigureAwait(false);
            foreach (var app in index)
            {
                if (results.Count >= query.Limit)
                {
                    break;
                }

                var name = AppsPresentation.DisplayName(app.Presentation, app.Slug);
                if (Matches(app.Slug, name, text))
                {
                    results.Add(new AddressCompletion(
                        CataloguePrefix + app.Slug,
                        AppsRoutes.Review(tenant, app.SourceKey, app.Slug),
                        $"{name} - {app.SourceKey}"));
                }
            }
        }

        return results;
    }

    private static void AddInstalled(List<AddressCompletion> results, AppsAccessSnapshot snapshot, string? tenant, string text, AddressQuery query)
    {
        // "crm/o" completes the UI of exactly crm; anything else matches slugs and names.
        var slash = text.IndexOf('/', StringComparison.Ordinal);
        var slugText = slash < 0 ? text : text[..slash];
        var rest = slash < 0 ? null : text[(slash + 1)..];

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var app in snapshot.MyApps)
        {
            if (results.Count >= query.Limit)
            {
                return;
            }

            var name = AppsPresentation.DisplayName(app.Presentation, app.Slug);
            if (rest is null && Matches(app.Slug, name, slugText))
            {
                seen.Add(app.Slug);
                results.Add(new AddressCompletion("a/" + app.Slug, AppsRoutes.App(tenant, app.Slug), name));
            }

            if (app.HasUi
                && results.Count < query.Limit
                && (rest is null ? Matches(app.Slug, name, slugText) : string.Equals(app.Slug, slugText, StringComparison.Ordinal)
                    && AppsRoutes.OpenSegment.StartsWith(rest, StringComparison.OrdinalIgnoreCase)))
            {
                results.Add(new AddressCompletion($"a/{app.Slug}/{AppsRoutes.OpenSegment}", AppsRoutes.Open(tenant, app.Slug), "Open " + name));
            }
        }

        if (rest is not null)
        {
            return;
        }

        foreach (var app in snapshot.Installed)
        {
            if (results.Count >= query.Limit)
            {
                return;
            }

            if (!seen.Contains(app.Slug) && app.State != Api.Apps.AppLifecycleState.Uninstalled && Matches(app.Slug, app.Slug, slugText))
            {
                seen.Add(app.Slug);
                results.Add(new AddressCompletion("a/" + app.Slug, AppsRoutes.App(tenant, app.Slug), AppsPresentation.LifecycleState(app.State, app.Version).Text));
            }
        }
    }

    private static bool Matches(string slug, string name, string text) =>
        text.Length == 0
        || slug.Contains(text, StringComparison.OrdinalIgnoreCase)
        || name.Contains(text, StringComparison.OrdinalIgnoreCase);
}
