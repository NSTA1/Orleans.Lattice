using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice;

/// <summary>
/// Builds the <see cref="LatticeViewDefinition"/> for a durable per-key history
/// view: an append-only (accumulative) materialised view whose projection re-keys
/// every source mutation into a revision row at <c>{sourceKey}/{encodedHlc}</c>.
/// <para>
/// A history view must be created the <em>runtime</em> way - passed to
/// <see cref="ILatticeViewFactory.Create"/> - rather than declared at startup via
/// <c>AddLatticeViews</c>, because only a runtime-created view can be torn down
/// again with <see cref="ILatticeViewFactory.DeleteAsync"/> (the issue's
/// enable/disable contract). The projection depends on the internal history-row
/// codec, so this helper resolves it from the silo service provider; the
/// resulting definition carries a live projection instance and is never
/// serialized.
/// </para>
/// </summary>
public static class LatticeHistoryView
{
    /// <summary>
    /// Builds an accumulative history-view definition for <paramref name="viewName"/>,
    /// resolving the history-row codec from <paramref name="services"/>.
    /// </summary>
    /// <param name="viewName">
    /// The logical view name; the view tree is <c>view-{viewName}</c>. Must not be
    /// <see langword="null"/> or empty.
    /// </param>
    /// <param name="services">
    /// The silo service provider (the one <c>AddLatticeViews</c> registered the
    /// codec into). Must not be <see langword="null"/>.
    /// </param>
    /// <returns>A definition with the history projection and the accumulative flag set.</returns>
    public static LatticeViewDefinition Definition(string viewName, IServiceProvider services)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentNullException.ThrowIfNull(services);

        var projection = services.GetRequiredService<HistoryLatticeViewProjection>();
        return new LatticeViewDefinition(viewName, projection, accumulative: true);
    }
}
