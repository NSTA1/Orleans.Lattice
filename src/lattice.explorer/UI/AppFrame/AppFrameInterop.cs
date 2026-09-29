using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// The one <see cref="DotNetObjectReference"/> target the host module calls back into for a
/// frame. Only the Explorer's own page holds the reference: the frame is an opaque origin
/// and can never reach it. Every call is forwarded to the owning <see cref="AppFrame"/>,
/// which treats each argument as untrusted.
/// </summary>
/// <param name="owner">The frame component.</param>
internal sealed class AppFrameInterop(AppFrame owner)
{
    /// <summary>The frame's bootstrap posted <c>lattice.ready</c> and was sent its port.</summary>
    /// <param name="protocol">The protocol the frame announced.</param>
    /// <returns>A task that completes when the host has handled it.</returns>
    [JSInvokable]
    public Task OnFrameReady(long protocol) => owner.HandleFrameReadyAsync(protocol);

    /// <summary>The frame posted a message over its port.</summary>
    /// <param name="message">The message as JSON text.</param>
    /// <returns>The reply to post back, or <see langword="null"/> to post nothing.</returns>
    [JSInvokable]
    public Task<string?> OnPortMessage(string? message) => owner.HandlePortMessageAsync(message);

    /// <summary>The frame reported <c>lattice.failed</c>; the host module has already torn it down.</summary>
    /// <param name="code">The failure code the frame supplied.</param>
    /// <returns>A task that completes when the host has handled it.</returns>
    [JSInvokable]
    public Task OnFrameFailed(string? code) => owner.HandleFrameFailedAsync(code);

    /// <summary>The frame loaded a second document; the host module has already torn it down.</summary>
    /// <returns>A task that completes when the host has handled it.</returns>
    [JSInvokable]
    public Task OnFrameReloaded() => owner.HandleFrameReloadedAsync();

    /// <summary>The frame reported that its last script loaded; the host posts the in-app path again.</summary>
    /// <returns>A task that completes when the host has handled it.</returns>
    [JSInvokable]
    public Task OnFrameLoaded() => owner.HandleFrameLoadedAsync();
}
