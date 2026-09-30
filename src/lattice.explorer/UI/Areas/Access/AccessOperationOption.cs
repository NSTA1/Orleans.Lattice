namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>One operation the rule editor's checklist offers.</summary>
/// <param name="Flag">The operation flag.</param>
/// <param name="Label">Its label.</param>
/// <param name="Group">The checklist group it is listed under.</param>
internal sealed record AccessOperationOption(LatticeOperation Flag, string Label, AccessOperationGroup Group)
{
    /// <summary>The stable, lower-case value that names the operation in an address and a form field.</summary>
    public string Value => Flag.ToString().ToLowerInvariant();

    /// <summary>Whether the operation is a scopeless cluster-wide capability.</summary>
    public bool IsScopeless => (AccessRuleFormat.ScopelessOperations & Flag) == Flag;
}
