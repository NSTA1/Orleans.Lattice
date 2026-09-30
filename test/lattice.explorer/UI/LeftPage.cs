using Bunit.Rendering;

namespace Orleans.Lattice.Explorer.Tests.UI;

/// <summary>
/// What a page left while a read was still on its way does once the reply arrives
/// (issue #4011): the reply resumes the page's continuation on the renderer after the
/// page was disposed, and anything it throws completes the renderer's
/// <see cref="BunitRenderer.UnhandledException"/>, which in a browser ends the circuit.
/// </summary>
internal static class LeftPage
{
    // The released reply hops through the page's facade and back onto the renderer's
    // dispatcher; this only bounds how long that hop is given before the page is judged.
    private static readonly TimeSpan Settle = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Waits, briefly, for the renderer to report a fault, and returns it: the fault that
    /// would have ended the circuit, or <see langword="null"/> when the page stopped quietly.
    /// </summary>
    /// <param name="renderer">The test context's renderer.</param>
    /// <returns>The fault, or <see langword="null"/>.</returns>
    public static Exception? Fault(BunitRenderer renderer)
    {
        SpinWait.SpinUntil(() => renderer.UnhandledException.IsCompleted, Settle);
        return renderer.UnhandledException.IsCompleted ? renderer.UnhandledException.Result : null;
    }
}
