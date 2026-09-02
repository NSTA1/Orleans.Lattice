using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// Puts a resolved <see cref="ExplorerAppearanceState"/> onto the document, and
/// records it where the next load's first paint can find it.
/// </summary>
/// <remarks>
/// <para>
/// The seam exists because the three attributes live on <c>&lt;html&gt;</c> and
/// <c>&lt;body&gt;</c>, outside every component's render tree, so they can only
/// be reached through interop - and because a prerender pass, a static render and
/// a component test have no interop to reach. An implementation is therefore
/// expected to be a no-op rather than a failure when it cannot reach a document.
/// </para>
/// <para>
/// Applying is also what refreshes the first-paint record. That record is a
/// cache, never the contract: <see cref="IExplorerShellPreferences"/> remains the
/// only thing that remembers a preference, and on any disagreement it wins and
/// the record is rewritten from it.
/// </para>
/// </remarks>
public interface IExplorerAppearanceApplier
{
    /// <summary>Applies <paramref name="state"/> to the document.</summary>
    /// <param name="state">The resolved appearance to apply.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ApplyAsync(ExplorerAppearanceState state, CancellationToken cancellationToken = default);
}
