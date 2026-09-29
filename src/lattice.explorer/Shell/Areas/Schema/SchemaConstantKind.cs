namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>The kind of constant a <see cref="SchemaTransformStepKind.Set"/> step writes.</summary>
internal enum SchemaConstantKind
{
    /// <summary>A string.</summary>
    Text = 0,

    /// <summary>A number: a whole number when it parses as one, otherwise a real.</summary>
    Number = 1,

    /// <summary><c>true</c> or <c>false</c>.</summary>
    Boolean = 2,

    /// <summary>JSON <c>null</c>.</summary>
    Null = 3,
}
