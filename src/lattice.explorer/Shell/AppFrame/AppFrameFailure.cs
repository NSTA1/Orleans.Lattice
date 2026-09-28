namespace Orleans.Lattice.Explorer.Shell.Framing;

/// <summary>
/// Why an app frame is showing a shell-native error instead of the app. Every failure
/// replaces the frame entirely: an app's UI is never shown partially.
/// </summary>
public enum AppFrameFailure
{
    /// <summary>
    /// The caller holds no role in the app, it is not enabled, or it does not exist; the
    /// three are indistinguishable by design. This is the zero value, so an uninitialised
    /// failure fails closed.
    /// </summary>
    NoGrant = 0,

    /// <summary>The app is visible to the caller but its installed version declares no UI.</summary>
    NoUi = 1,

    /// <summary>An asset's bytes did not match the SHA-256 digest its manifest pins.</summary>
    DigestMismatch = 2,

    /// <summary>The bundle's declared digest did not match the digest of its asset list.</summary>
    BundleDigestMismatch = 3,

    /// <summary>The bundle is malformed or too large, or its entry fragment was refused.</summary>
    BundleInvalid = 4,

    /// <summary>The frame's bootstrap did not complete its handshake in time.</summary>
    HandshakeTimeout = 5,

    /// <summary>The frame's protocol is below the minimum the app's manifest requires, or above what the host speaks.</summary>
    ProtocolUnsupported = 6,

    /// <summary>The frame's bootstrap reported that it failed.</summary>
    FrameFailed = 7,

    /// <summary>The frame loaded a second document, so its port was closed and it was removed.</summary>
    Reloaded = 8,

    /// <summary>The app was disabled, uninstalled or upgraded, or its install revision changed, since it was opened.</summary>
    Revoked = 9,

    /// <summary>The cluster or the browser could not serve the frame; it may succeed if reopened.</summary>
    Unavailable = 10,
}
