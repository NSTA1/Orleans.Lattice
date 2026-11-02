namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>How the value editor shows and encodes a draft.</summary>
internal enum DataDraftFormat
{
    /// <summary>A JSON document, validated before save and stored as UTF-8.</summary>
    Json,

    /// <summary>Plain text, stored as UTF-8 exactly as typed.</summary>
    Text,

    /// <summary>A binary value, shown as base64 and replaced whole.</summary>
    Base64,
}
