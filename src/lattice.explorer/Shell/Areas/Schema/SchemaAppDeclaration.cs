namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// A schema declaration an installed app's manifest makes for one of its trees
/// (the manifest's <c>Schema</c> section, #2235): the app that declares it and
/// what it asks for. The tree is named by its logical id, never a physical one.
/// </summary>
/// <param name="Slug">The declaring app's slug, shown as text and used only as an address segment.</param>
/// <param name="AppVersion">The installed version of the declaring app.</param>
/// <param name="TreeId">The declared tree's logical id, <c>a/{slug}/{tree}</c>.</param>
/// <param name="Family">The schema family the manifest names.</param>
/// <param name="Version">The envelope version the manifest requests.</param>
/// <param name="StrictIngest">Whether the manifest requests strict ingest.</param>
internal sealed record SchemaAppDeclaration(
    string Slug,
    string AppVersion,
    string TreeId,
    string Family,
    int Version,
    bool StrictIngest)
{
    /// <summary>The structural prefix of an app-owned tree (#2235 D1).</summary>
    public const string AppPrefix = "a/";

    /// <summary>The logical id of the app tree <paramref name="localTree"/> of the app <paramref name="slug"/>.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="localTree">The app-local tree name from the manifest.</param>
    /// <returns>The logical tree id.</returns>
    public static string TreeIdFor(string slug, string localTree)
    {
        ArgumentException.ThrowIfNullOrEmpty(slug);
        ArgumentException.ThrowIfNullOrEmpty(localTree);
        return AppPrefix + slug + "/" + localTree;
    }
}
