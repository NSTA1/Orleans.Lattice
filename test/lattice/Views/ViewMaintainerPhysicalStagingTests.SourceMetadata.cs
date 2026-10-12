namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    [Test]
    public async Task Harness_source_metadata_preserves_physical_rows_and_requested_key_order()
    {
        var h = await Harness.CreateAsync();
        var old = Prepare(Old, "shared", 11);
        var current = Prepare(New, "shared", 21);
        var deleted = Prepare(New, "deleted", 22) with
        {
            Kind = MutationKind.Delete, IsTombstone = true, Value = null,
        };
        h.Append(Old, 7, old);
        h.Append(New, 2, current);
        h.Append(New, 3, deleted);
        Assert.That(h.VisibleRows(New, ["shared", "deleted"]), Is.EqualTo(new object?[] { null, null }),
            "an unresolved prepare is not a coherent visible source row");
        h.Append(Old, 8, Terminal(Old));
        h.Append(New, 4, Terminal(New));

        var rows = h.VisibleRows(New, ["deleted", "missing", "shared", "shared"]);
        Assert.That(rows, Has.Count.EqualTo(4));
        Assert.That(rows[0]!.Value.Key, Is.EqualTo("deleted"));
        Assert.That(rows[0]!.Value.IsTombstone, Is.True);
        Assert.That(rows[0]!.Value.Value, Is.Null);
        Assert.That(rows[0]!.Value.Timestamp, Is.EqualTo(deleted.Timestamp));
        Assert.That(rows[1], Is.Null);
        Assert.That(rows[2]!.Value.Key, Is.EqualTo("shared"));
        Assert.That(rows[2]!.Value.Value, Is.EqualTo(current.Value));
        Assert.That(rows[2]!.Value.Timestamp, Is.EqualTo(current.Timestamp));
        Assert.That(rows[2]!.Value.IsTombstone, Is.False);
        Assert.That(rows[3], Is.EqualTo(rows[2]));
        Assert.That(h.VisibleRows(Old, ["shared"])[0]!.Value.Timestamp, Is.EqualTo(old.Timestamp),
            "the same key and txid on another physical source must not supply this log's tuple");
    }
}
