namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>The session surface a circuit has asked the session overlay to show.</summary>
internal enum SessionOverlayKind
{
    /// <summary>No surface has been requested.</summary>
    None = 0,

    /// <summary>The connection settings, opened from the connection indicator.</summary>
    Configuration = 1,

    /// <summary>The sign-in dialog, opened from the identity menu or any area that needs a credential.</summary>
    SignIn = 2,
}
