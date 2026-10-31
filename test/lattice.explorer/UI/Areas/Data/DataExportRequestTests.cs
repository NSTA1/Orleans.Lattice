using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// What a user asks to export (issue #5366): the key range each scope reads, the
/// problems that stop a request before anything runs, and the file name it is saved as.
/// </summary>
[TestFixture]
public sealed class DataExportRequestTests
{
    private static readonly DataExportRequest Tree = new() { TreeId = "factory-orders", TreeName = "factory-orders" };

    [Test]
    public void Each_scope_reads_its_half_open_key_range()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Tree.Bounds, Is.EqualTo(((string?)null, (string?)null)));
            Assert.That((Tree with { Scope = DataExportScopeKind.Prefix, Prefix = "order/" }).Bounds, Is.EqualTo(("order/", "order0")));
            Assert.That((Tree with { Scope = DataExportScopeKind.Range, From = "a", To = "m" }).Bounds, Is.EqualTo(("a", "m")));
            Assert.That((Tree with { Scope = DataExportScopeKind.Range, From = "a", To = "" }).Bounds, Is.EqualTo(("a", (string?)null)));
            Assert.That((Tree with { Scope = DataExportScopeKind.Prefix, Prefix = "\uffff" }).Bounds, Is.EqualTo(("\uffff", (string?)null)), "a prefix with no successor reads to the end");
        });
    }

    [Test]
    public void A_request_that_cannot_run_says_why()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Tree.Validate(), Is.Null);
            Assert.That((Tree with { Scope = DataExportScopeKind.Prefix }).Validate(), Is.EqualTo("Enter the prefix the keys start with."));
            Assert.That((Tree with { Scope = DataExportScopeKind.Range }).Validate(), Is.EqualTo("Enter a first key, a last key, or both."));
            Assert.That((Tree with { Scope = DataExportScopeKind.Range, From = "b", To = "a" }).Validate(), Does.StartWith("The range ends before it starts"));
            Assert.That((Tree with { Scope = DataExportScopeKind.Range, From = "a", To = "a" }).Validate(), Does.StartWith("The range ends before it starts"));
            Assert.That((Tree with { Scope = DataExportScopeKind.Range, To = "a" }).Validate(), Is.Null);
        });
    }

    [Test]
    public void The_file_is_named_after_the_tree_made_safe_with_the_format_extension()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Tree.FileName, Is.EqualTo("factory-orders.ndjson"));
            Assert.That((Tree with { Format = DataExportFormat.Csv }).FileName, Is.EqualTo("factory-orders.csv"));
            Assert.That((Tree with { TreeName = "a/crm/orders" }).FileName, Is.EqualTo("a-crm-orders.ndjson"));
            Assert.That((Tree with { TreeName = "///" }).FileName, Is.EqualTo("export.ndjson"));
            Assert.That((Tree with { TreeName = new string('x', 200) }).FileName, Has.Length.EqualTo(DataExportRequest.MaxFileStem + ".ndjson".Length));
            Assert.That(Tree.ContentType, Is.EqualTo("application/x-ndjson"));
        });
    }
}
