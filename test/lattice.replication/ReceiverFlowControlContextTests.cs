namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Value-semantic tests for <see cref="ReceiverFlowControlContext"/>.
/// </summary>
[TestFixture]
public class ReceiverFlowControlContextTests
{
    [Test]
    public void Default_value_has_null_strings_and_zero_counts()
    {
        var ctx = default(ReceiverFlowControlContext);

        Assert.Multiple(() =>
        {
            Assert.That(ctx.TreeName, Is.Null);
            Assert.That(ctx.OriginClusterId, Is.Null);
            Assert.That(ctx.EntryCount, Is.EqualTo(0));
            Assert.That(ctx.ApplyDurationMs, Is.EqualTo(0d));
        });
    }

    [Test]
    public void Init_assigns_every_property()
    {
        var ctx = new ReceiverFlowControlContext
        {
            TreeName = "tree",
            OriginClusterId = "site-b",
            EntryCount = 7,
            ApplyDurationMs = 12.5d,
        };

        Assert.Multiple(() =>
        {
            Assert.That(ctx.TreeName, Is.EqualTo("tree"));
            Assert.That(ctx.OriginClusterId, Is.EqualTo("site-b"));
            Assert.That(ctx.EntryCount, Is.EqualTo(7));
            Assert.That(ctx.ApplyDurationMs, Is.EqualTo(12.5d));
        });
    }

    [Test]
    public void Equality_uses_value_semantics()
    {
        var a = new ReceiverFlowControlContext
        {
            TreeName = "tree",
            OriginClusterId = "site-b",
            EntryCount = 7,
            ApplyDurationMs = 12.5d,
        };
        var b = new ReceiverFlowControlContext
        {
            TreeName = "tree",
            OriginClusterId = "site-b",
            EntryCount = 7,
            ApplyDurationMs = 12.5d,
        };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void With_expression_produces_modified_copy()
    {
        var a = new ReceiverFlowControlContext
        {
            TreeName = "tree",
            OriginClusterId = "site-b",
            EntryCount = 7,
            ApplyDurationMs = 12.5d,
        };

        var b = a with { EntryCount = 8 };

        Assert.Multiple(() =>
        {
            Assert.That(a.EntryCount, Is.EqualTo(7));
            Assert.That(b.EntryCount, Is.EqualTo(8));
            Assert.That(b.TreeName, Is.EqualTo(a.TreeName));
            Assert.That(b.OriginClusterId, Is.EqualTo(a.OriginClusterId));
            Assert.That(b.ApplyDurationMs, Is.EqualTo(a.ApplyDurationMs));
        });
    }
}