namespace Orleans.Lattice.Apps;

/// <summary>
/// The inert <see cref="IAppSource"/>: holds no apps, so every slug resolves to
/// <see cref="AppSourceStatus.NotFound"/>. It is the safe default for a consumer of the seam when no
/// source has been configured; <see cref="InImageAppSource"/> is the richer opt-in implementation.
/// </summary>
internal sealed class NullAppSource : IAppSource
{
    /// <summary>The shared stateless instance.</summary>
    public static NullAppSource Instance { get; } = new();

    /// <inheritdoc />
    public ValueTask<AppSourceResult> ResolveAsync(
        AppSlug slug,
        AppVersion? version = null,
        CancellationToken cancellationToken = default) =>
        new(AppSourceResult.NotFound(slug));
}
