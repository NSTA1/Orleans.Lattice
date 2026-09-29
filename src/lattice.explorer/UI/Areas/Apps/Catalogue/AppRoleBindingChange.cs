namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>One declared role's recorded and proposed group while its bindings are being changed.</summary>
/// <param name="Role">The declared role.</param>
/// <param name="Current">The group it is bound to now, or <see langword="null"/> when it is unbound.</param>
/// <param name="Proposed">The group it will be bound to, or <see langword="null"/> when it will be unbound.</param>
internal sealed record AppRoleBindingChange(string Role, string? Current, string? Proposed)
{
    /// <summary>Whether applying the change moves the role to another group, or binds or unbinds it.</summary>
    public bool IsChanged => !string.Equals(Current, Proposed, StringComparison.Ordinal);
}
