namespace Orleans.Lattice.Explorer.Shell.Framing.Broker;

/// <summary>The broker's answer to one frame message.</summary>
/// <param name="Reply">The response envelope to post back over the port, or <see langword="null"/> when the message is dropped.</param>
/// <param name="Effect">What the host must do beyond posting the reply.</param>
/// <param name="Argument">The effect's argument: the synchronised path, or the revocation reason.</param>
internal readonly record struct AppBridgeOutcome(string? Reply, AppBridgeEffect Effect = AppBridgeEffect.None, string? Argument = null)
{
    /// <summary>A dropped message: no reply, no effect.</summary>
    public static AppBridgeOutcome Dropped => default;
}
