namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The set of grain indexes declared on this silo. Every
/// <c>AddGrainIndex</c> call appends its definition here, so the whole
/// declaration set is resolvable from dependency injection as
/// <c>IOptions&lt;GrainIndexDeclarationOptions&gt;</c> and can be validated as a
/// whole - which is what makes a duplicate index name detectable.
/// </summary>
public sealed class GrainIndexDeclarationOptions
{
    /// <summary>
    /// The declared indexes, in registration order. Never <c>null</c>.
    /// </summary>
    public IList<IGrainIndexDefinition> Definitions { get; } = new List<IGrainIndexDefinition>();
}
