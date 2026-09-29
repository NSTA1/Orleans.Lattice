namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>How a value's bytes are drawn in the mono value cell.</summary>
internal enum DataValueRenderer
{
    /// <summary>The Core renderer's choice: pretty JSON, UTF-8 text, or a hex dump.</summary>
    Auto = 0,

    /// <summary>The bytes decoded as UTF-8 text.</summary>
    Text = 1,

    /// <summary>The bytes parsed and pretty-printed as JSON.</summary>
    Json = 2,

    /// <summary>A hex dump with an ASCII gutter.</summary>
    Hex = 3,

    /// <summary>The CRDT members the state API decoded.</summary>
    Members = 4,
}
