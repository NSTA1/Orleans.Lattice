namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeSchemaRule"/> factory methods: correct
/// kind / member population and parameter guards.
/// </summary>
public class LatticeSchemaRuleTests
{
    [Test]
    public void Structured_sets_kind_and_predicate()
    {
        var predicate = LatticePredicateNode.Const(LatticeConstant.Bool(true));
        var rule = LatticeSchemaRule.Structured(predicate, "desc");

        Assert.That(rule.Kind, Is.EqualTo(LatticeSchemaRuleKind.Structured));
        Assert.That(rule.Predicate, Is.EqualTo(predicate));
        Assert.That(rule.Description, Is.EqualTo("desc"));
    }

    [Test]
    public void Regex_sets_kind_pattern_and_member_path()
    {
        var rule = LatticeSchemaRule.Regex("^a$", "member.path", "desc");

        Assert.That(rule.Kind, Is.EqualTo(LatticeSchemaRuleKind.Regex));
        Assert.That(rule.RegexPattern, Is.EqualTo("^a$"));
        Assert.That(rule.MemberPath, Is.EqualTo("member.path"));
    }

    [Test]
    public void Regex_null_or_empty_pattern_throws()
    {
        Assert.That(() => LatticeSchemaRule.Regex(null!), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => LatticeSchemaRule.Regex(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Utf8_sets_encoding_kind()
    {
        var rule = LatticeSchemaRule.Utf8();
        Assert.That(rule.Kind, Is.EqualTo(LatticeSchemaRuleKind.Encoding));
        Assert.That(rule.EncodingKind, Is.EqualTo(LatticeSchemaEncodingKind.Utf8));
    }

    [Test]
    public void Json_sets_encoding_kind()
    {
        var rule = LatticeSchemaRule.Json();
        Assert.That(rule.Kind, Is.EqualTo(LatticeSchemaRuleKind.Encoding));
        Assert.That(rule.EncodingKind, Is.EqualTo(LatticeSchemaEncodingKind.Json));
    }

    [Test]
    public void MaxLength_sets_encoding_kind_and_bound()
    {
        var rule = LatticeSchemaRule.MaxLength(128);
        Assert.That(rule.Kind, Is.EqualTo(LatticeSchemaRuleKind.Encoding));
        Assert.That(rule.EncodingKind, Is.EqualTo(LatticeSchemaEncodingKind.MaxByteLength));
        Assert.That(rule.MaxByteLength, Is.EqualTo(128));
    }

    [Test]
    public void MaxLength_negative_throws()
    {
        Assert.That(() => LatticeSchemaRule.MaxLength(-1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
