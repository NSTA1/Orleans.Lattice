namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for the serializable model types <see cref="LatticeSchemaPolicy"/>
/// and <see cref="LatticeSchemaDeadLetterEntry"/>: construction, defaults, and
/// parameter guards.
/// </summary>
public class LatticeSchemaModelTests
{
    [Test]
    public void Policy_defaults_strict_ingest_off()
    {
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());
        Assert.That(policy.StrictIngest, Is.False);
        Assert.That(policy.Rules, Is.Empty);
    }

    [Test]
    public void Policy_retains_rules_and_strict_flag()
    {
        var rules = new[] { LatticeSchemaRule.Json(), LatticeSchemaRule.MaxLength(8) };
        var policy = new LatticeSchemaPolicy(rules, strictIngest: true);

        Assert.That(policy.Rules, Has.Count.EqualTo(2));
        Assert.That(policy.StrictIngest, Is.True);
    }

    [Test]
    public void Policy_null_rules_throws()
    {
        Assert.That(() => new LatticeSchemaPolicy(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DeadLetterEntry_retains_all_fields()
    {
        var ts = DateTimeOffset.UtcNow;
        var preview = new byte[] { 1, 2, 3 };
        var entry = new LatticeSchemaDeadLetterEntry(
            "k1", preview, 10, "bad", LatticeSchemaDeadLetterSource.Replication, ts);

        Assert.That(entry.Key, Is.EqualTo("k1"));
        Assert.That(entry.ValuePreview, Is.EqualTo(preview));
        Assert.That(entry.ValueByteLength, Is.EqualTo(10));
        Assert.That(entry.Reason, Is.EqualTo("bad"));
        Assert.That(entry.Source, Is.EqualTo(LatticeSchemaDeadLetterSource.Replication));
        Assert.That(entry.TimestampUtc, Is.EqualTo(ts));
    }

    [Test]
    public void DeadLetterEntry_null_arguments_throw()
    {
        var ts = DateTimeOffset.UtcNow;
        Assert.That(
            () => new LatticeSchemaDeadLetterEntry(null!, Array.Empty<byte>(), 0, "r", LatticeSchemaDeadLetterSource.Restore, ts),
            Throws.ArgumentNullException);
        Assert.That(
            () => new LatticeSchemaDeadLetterEntry("k", null!, 0, "r", LatticeSchemaDeadLetterSource.Restore, ts),
            Throws.ArgumentNullException);
        Assert.That(
            () => new LatticeSchemaDeadLetterEntry("k", Array.Empty<byte>(), 0, null!, LatticeSchemaDeadLetterSource.Restore, ts),
            Throws.ArgumentNullException);
    }

    [Test]
    public void DeadLetterEntry_negative_length_throws()
    {
        Assert.That(
            () => new LatticeSchemaDeadLetterEntry("k", Array.Empty<byte>(), -1, "r", LatticeSchemaDeadLetterSource.Restore, DateTimeOffset.UtcNow),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
