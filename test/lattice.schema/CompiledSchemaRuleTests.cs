using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledSchemaRule"/>: each rule kind's valid /
/// invalid evaluation, and the compile-at-set-time rejection of an uncompilable
/// regex.
/// </summary>
public class CompiledSchemaRuleTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public void Compile_regex_valid_pattern_matches_whole_value()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^[a-z]+$"));
        Assert.That(compiled.Validate(Utf8("abc")), Is.Null);
    }

    [Test]
    public void Compile_regex_non_matching_value_returns_reason()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^[a-z]+$", description: "letters only"));
        Assert.That(compiled.Validate(Utf8("abc123")), Is.EqualTo("letters only"));
    }

    [Test]
    public void Compile_regex_member_path_projects_before_matching()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^[0-9]{5}$", memberPath: "zip"));
        Assert.That(compiled.Validate(Utf8("{\"zip\":\"12345\"}")), Is.Null);
        Assert.That(compiled.Validate(Utf8("{\"zip\":\"abc\"}")), Is.Not.Null);
    }

    [Test]
    public void Compile_regex_member_path_absent_member_returns_reason()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^.+$", memberPath: "zip"));
        Assert.That(compiled.Validate(Utf8("{\"other\":\"x\"}")), Is.Not.Null);
    }

    [Test]
    public void Compile_regex_whole_value_non_utf8_returns_reason()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^.+$"));
        Assert.That(compiled.Validate(new byte[] { 0xC3, 0x28 }), Is.Not.Null);
    }

    [Test]
    public void Compile_uncompilable_regex_throws_at_set_time()
    {
        // An unbalanced group is a parse error; rejected at compile time.
        Assert.That(
            () => CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("(unclosed")),
            Throws.ArgumentException);
    }

    [Test]
    public void Compile_nonlinear_lookbehind_regex_throws_at_set_time()
    {
        // Lookbehind is unsupported by NonBacktracking, so it is rejected up front.
        Assert.That(
            () => CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("(?<=a)b")),
            Throws.ArgumentException);
    }

    [Test]
    public void Encoding_utf8_rule_accepts_text_rejects_invalid_bytes()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Utf8());
        Assert.That(compiled.Validate(Utf8("ok")), Is.Null);
        Assert.That(compiled.Validate(new byte[] { 0xFF }), Is.Not.Null);
    }

    [Test]
    public void Encoding_json_rule_accepts_json_rejects_non_json()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Json());
        Assert.That(compiled.Validate(Utf8("{\"a\":1}")), Is.Null);
        Assert.That(compiled.Validate(Utf8("not json")), Is.Not.Null);
    }

    [Test]
    public void Encoding_max_byte_length_rule_boundary_is_inclusive()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.MaxLength(3));
        Assert.That(compiled.Validate(Utf8("abc")), Is.Null);
        Assert.That(compiled.Validate(Utf8("abcd")), Is.Not.Null);
    }

    [Test]
    public void Encoding_max_byte_length_zero_accepts_empty()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.MaxLength(0));
        Assert.That(compiled.Validate(Array.Empty<byte>()), Is.Null);
        Assert.That(compiled.Validate(Utf8("a")), Is.Not.Null);
    }

    [Test]
    public void Structured_rule_evaluates_predicate_against_json()
    {
        var predicate = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(18)));
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Structured(predicate, "must be adult"));

        Assert.That(compiled.Validate(Utf8("{\"age\":21}")), Is.Null);
        Assert.That(compiled.Validate(Utf8("{\"age\":12}")), Is.EqualTo("must be adult"));
    }

    [Test]
    public void Validate_null_value_throws()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Utf8());
        Assert.That(() => compiled.Validate(null!), Throws.ArgumentNullException);
    }
}
