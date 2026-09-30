namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>How a text-match card compares: the start, the end or anywhere.</summary>
internal enum SchemaTextMatch
{
    /// <summary>The text must start with the given text.</summary>
    StartsWith = 0,

    /// <summary>The text must end with the given text.</summary>
    EndsWith = 1,

    /// <summary>The text must contain the given text.</summary>
    Contains = 2,
}
