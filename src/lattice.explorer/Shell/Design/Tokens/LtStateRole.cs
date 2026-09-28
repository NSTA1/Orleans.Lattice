namespace Orleans.Lattice.Explorer.Shell.Design.Tokens;

/// <summary>
/// The lifecycle and health states the Explorer draws, each backed by a state
/// role in <c>lattice-operate.css</c> that pairs a colour with a glyph, so no
/// state is ever carried by colour alone.
/// </summary>
public enum LtStateRole
{
    /// <summary>An app is installed but its enablement is not being shown.</summary>
    Installed,

    /// <summary>An app is installed and enabled.</summary>
    Enabled,

    /// <summary>An app is installed and disabled.</summary>
    Disabled,

    /// <summary>An app is not installed, or was uninstalled.</summary>
    Uninstalled,

    /// <summary>An app's consent has drifted from its manifest and needs review.</summary>
    Drift,

    /// <summary>A component is healthy.</summary>
    Healthy,

    /// <summary>A component is progressing, but behind.</summary>
    Lagging,

    /// <summary>A component has stopped making progress.</summary>
    Stalled,

    /// <summary>A component has failed.</summary>
    Failed,

    /// <summary>A component's state could not be established.</summary>
    Unknown,
}
