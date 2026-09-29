using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The chrome's one JavaScript module, imported lazily per circuit: the global
/// <c>/</c> and Ctrl+K shortcuts, applying an appearance to the document,
/// focusing and selecting the address input, and measuring the Shell root's
/// width band.
/// </summary>
/// <remarks>
/// Every call is best effort. During a prerender, after the circuit has gone, or
/// in a test without a browser, a call does nothing and returns the fallback: the
/// chrome is fully usable without script, and none of this is security state.
/// </remarks>
internal sealed class ShellChromeInterop : IAsyncDisposable
{
    private readonly IJSRuntime _js;
    private Task<IJSObjectReference>? _module;

    /// <summary>Creates the interop over the circuit's JavaScript runtime.</summary>
    /// <param name="js">The JavaScript runtime.</param>
    public ShellChromeInterop(IJSRuntime js)
    {
        ArgumentNullException.ThrowIfNull(js);
        _js = js;
    }

    /// <summary>
    /// Listens for <c>/</c> (outside a text field) and Ctrl+K or Cmd+K (anywhere),
    /// calling <paramref name="target"/>'s <c>OpenAddressLine</c> method. Dispose
    /// the returned handle to stop.
    /// </summary>
    /// <typeparam name="T">The .NET object type.</typeparam>
    /// <param name="target">The object to call back.</param>
    /// <returns>The listener's handle, or <see langword="null"/> when script is unavailable.</returns>
    public async ValueTask<IJSObjectReference?> RegisterShortcutsAsync<T>(DotNetObjectReference<T> target)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(target);
        return await InvokeAsync<IJSObjectReference?>(
            static (module, args) => module.InvokeAsync<IJSObjectReference?>("registerShortcuts", args),
            null,
            target).ConfigureAwait(false);
    }

    /// <summary>Sets the appearance attributes on the document element and remembers them for the first paint.</summary>
    /// <param name="theme">The stored theme name.</param>
    /// <param name="contrast">The stored contrast name.</param>
    /// <param name="density">The stored density name.</param>
    public async ValueTask ApplyAppearanceAsync(string theme, string contrast, string density) =>
        await InvokeAsync(
            static async (module, args) =>
            {
                await module.InvokeVoidAsync("applyAppearance", args).ConfigureAwait(false);
                return true;
            },
            false,
            theme,
            contrast,
            density).ConfigureAwait(false);

    /// <summary>Focuses a text field and selects its text, so typing replaces it.</summary>
    /// <param name="element">The field.</param>
    public async ValueTask FocusAndSelectAsync(ElementReference element) =>
        await InvokeAsync(
            static async (module, args) =>
            {
                await module.InvokeVoidAsync("focusAndSelect", args).ConfigureAwait(false);
                return true;
            },
            false,
            element).ConfigureAwait(false);

    /// <summary>
    /// Focuses <paramref name="element"/> if it is still in the document. Unlike
    /// <see cref="ElementReference"/>'s own focus, a request whose element a later render
    /// has removed is a no-op rather than an exception that would end the circuit.
    /// </summary>
    /// <param name="element">The element to focus.</param>
    public async ValueTask FocusAsync(ElementReference element) =>
        await InvokeAsync(
            static async (module, args) =>
            {
                await module.InvokeVoidAsync("focusElement", args).ConfigureAwait(false);
                return true;
            },
            false,
            element).ConfigureAwait(false);

    /// <summary>
    /// Watches <paramref name="element"/>'s inline size and calls
    /// <paramref name="target"/>'s <c>OnViewportBand</c> with the index of the
    /// band it falls in - how many of <paramref name="edges"/> it has reached -
    /// whenever that changes. Dispose the returned handle to stop.
    /// </summary>
    /// <typeparam name="T">The .NET object type.</typeparam>
    /// <param name="element">The element to measure.</param>
    /// <param name="target">The object to call back.</param>
    /// <param name="edges">The ascending band edges, in CSS pixels.</param>
    /// <returns>The observer's handle, or <see langword="null"/> when script is unavailable.</returns>
    public async ValueTask<IJSObjectReference?> ObserveViewportAsync<T>(ElementReference element, DotNetObjectReference<T> target, int[] edges)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(edges);
        return await InvokeAsync<IJSObjectReference?>(
            static (module, args) => module.InvokeAsync<IJSObjectReference?>("observeViewport", args),
            null,
            element,
            target,
            edges).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_module is { IsCompletedSuccessfully: true } module)
        {
            try
            {
                await module.Result.DisposeAsync().ConfigureAwait(false);
            }
            catch (JSDisconnectedException)
            {
                // The circuit has gone; so has the module.
            }
        }
    }

    private async ValueTask<TResult> InvokeAsync<TResult>(
        Func<IJSObjectReference, object?[], ValueTask<TResult>> call,
        TResult fallback,
        params object?[] arguments)
    {
        try
        {
            _module ??= _js.InvokeAsync<IJSObjectReference>("import", ShellChromeAssets.ModuleSpecifier).AsTask();
            var module = await _module.ConfigureAwait(false);
            return module is null ? fallback : await call(module, arguments).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is JSDisconnectedException or JSException or InvalidOperationException or TaskCanceledException)
        {
            // A prerender (no browser yet), a dropped circuit, or a document the
            // module cannot reach: the chrome carries on without script.
            if (_module is { IsCompleted: true, IsCompletedSuccessfully: false })
            {
                _module = null;
            }

            return fallback;
        }
    }
}
