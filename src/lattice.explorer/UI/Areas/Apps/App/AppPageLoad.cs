namespace Orleans.Lattice.Explorer.UI.Areas.Apps.App;

/// <summary>The outcome of loading an app's page.</summary>
/// <param name="Kind">What was found.</param>
/// <param name="Model">The app, when <paramref name="Kind"/> is <see cref="AppPageLoadKind.Loaded"/>.</param>
internal sealed record AppPageLoad(AppPageLoadKind Kind, AppPageModel? Model = null)
{
    /// <summary>Nothing the caller may see.</summary>
    public static AppPageLoad NotFound { get; } = new(AppPageLoadKind.NotFound);

    /// <summary>The cluster could not answer.</summary>
    public static AppPageLoad Unavailable { get; } = new(AppPageLoadKind.Unavailable);

    /// <summary>The app, as this caller may see it.</summary>
    /// <param name="model">The app.</param>
    /// <returns>A loaded outcome.</returns>
    public static AppPageLoad Loaded(AppPageModel model)
    {
        ArgumentNullException.ThrowIfNull(model);
        return new(AppPageLoadKind.Loaded, model);
    }
}
