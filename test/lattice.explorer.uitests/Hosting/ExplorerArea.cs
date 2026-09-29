namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>One native area of the Explorer.</summary>
/// <param name="Key">The area key and first address segment, such as <c>data</c>.</param>
/// <param name="DisplayName">The area's name on the directory spine.</param>
/// <param name="PrimaryPath">The area's primary page.</param>
/// <param name="DeepPath">A page deeper in the area.</param>
/// <param name="ShownToAdmin">Whether the test world shows the area to its administrator.</param>
/// <param name="TenantScoped">Whether the area's addresses are rooted at the active tenant when tenancy is on.</param>
internal sealed record ExplorerArea(string Key, string DisplayName, string PrimaryPath, string DeepPath, bool ShownToAdmin, bool TenantScoped);
