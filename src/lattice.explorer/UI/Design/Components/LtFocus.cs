using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// Moves keyboard focus without risking the circuit.
/// </summary>
/// <remarks>
/// <para>
/// A focus request travels to the browser after the render that asked for it, and a
/// later render can remove its element first. The browser then refuses it and
/// <see cref="ElementReference"/>'s own focus throws a <see cref="JSException"/>. Raised from
/// <c>OnAfterRenderAsync</c> or an event handler, that exception ends the Blazor circuit and
/// the whole console stops answering, all for a focus that no longer matters.
/// </para>
/// <para>
/// This is the design system's own route, because the primitives sit below the chrome and
/// cannot use its script module. The chrome focuses through <c>ShellChromeInterop</c>, which
/// checks the element is still in the document before focusing it.
/// </para>
/// </remarks>
internal static class LtFocus
{
    /// <summary>
    /// Focuses <paramref name="element"/>, and treats a refused focus - its element gone, or
    /// the circuit already closing - as nothing more than a focus not taken.
    /// </summary>
    /// <param name="element">The element to focus.</param>
    /// <returns>A task that completes when the request has been answered or refused.</returns>
    public static async ValueTask FocusSafelyAsync(this ElementReference element)
    {
        try
        {
            await element.FocusAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JSException or JSDisconnectedException or TaskCanceledException)
        {
            // The element or the circuit has gone; so has any reason to focus it.
        }
    }
}
