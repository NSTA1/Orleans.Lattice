namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>The kinds of value a member can be required to hold, in plain words.</summary>
internal enum SchemaValueType
{
    /// <summary>A JSON string.</summary>
    Text = 0,

    /// <summary>A JSON number.</summary>
    Number = 1,

    /// <summary><c>true</c> or <c>false</c>.</summary>
    Boolean = 2,

    /// <summary>A JSON object.</summary>
    Object = 3,

    /// <summary>A JSON array.</summary>
    List = 4,
}
