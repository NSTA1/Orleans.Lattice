using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// The browser <see cref="IExplorerAppearanceApplier"/>: hands the resolved
/// appearance to the bootstrap script, which owns the document attributes and the
/// first-paint record.
/// </summary>
/// <remarks>
/// <para>
/// The three attributes sit on <c>&lt;html&gt;</c> and <c>&lt;body&gt;</c>, above
/// every component's render tree, so they cannot be written as markup - and the
/// script has to own them anyway, because it is the only thing that runs before
/// the first paint. This type is therefore deliberately thin: it names the
/// choices and lets <c>lattice-appearance.js</c> decide what an absent choice
/// means.
/// </para>
/// <para>
/// Every failure mode is swallowed. Interop is unreachable during a prerender
/// pass, during a static render, and after a circuit has gone; in all three the
/// shell must still work, and in the first two the document already carries the
/// attributes the script applied before the application existed.
/// </para>
/// </remarks>
/// <param name="jsRuntime">The JavaScript runtime for the current circuit.</param>
public sealed class ExplorerAppearanceApplier(IJSRuntime jsRuntime) : IExplorerAppearanceApplier
{
    /// <summary>
    /// The global the bootstrap script publishes, and the function this applier
    /// calls. Named as a constant so the script and its caller cannot drift, and
    /// so a hygiene test can assert the script really declares it.
    /// </summary>
    public const string ApplyFunction = "latticeAppearance.apply";

    private readonly IJSRuntime _jsRuntime = jsRuntime ?? throw new ArgumentNullException(nameof(jsRuntime));

    /// <inheritdoc />
    public async ValueTask ApplyAsync(
        ExplorerAppearanceState state,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _jsRuntime.InvokeVoidAsync(
                ApplyFunction,
                cancellationToken,
                ExplorerAppearanceNames.ThemeAttribute(state.Theme),
                ExplorerAppearanceNames.ContrastAttribute(state.Contrast),
                ExplorerAppearanceNames.DensityAttribute(state.Density)).ConfigureAwait(false);
        }
        catch (JSDisconnectedException)
        {
            // The circuit is gone; the document went with it.
        }
        catch (JSException)
        {
            // The script failed to load or run. The document keeps whatever the
            // first-paint bootstrap already applied.
        }
        catch (InvalidOperationException)
        {
            // Interop attempted outside an interactive circuit: a prerender pass
            // or a static render, where the script has already run on its own.
        }
        catch (OperationCanceledException)
        {
            // The circuit is tearing down mid-call.
        }
    }
}
