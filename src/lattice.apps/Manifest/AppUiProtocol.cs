namespace Orleans.Lattice.Apps;

/// <summary>Versioning of the protocol spoken between an app UI frame and its host.</summary>
public static class AppUiProtocol
{
    /// <summary>
    /// The protocol version this release implements. A manifest's <c>ui.minProtocol</c> may not
    /// exceed it, so a bundle that needs a newer host fails validation rather than loading.
    /// </summary>
    public const int Current = 1;
}
