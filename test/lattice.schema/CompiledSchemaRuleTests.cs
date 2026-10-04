using System.Text;
using System.Reflection;

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

    /// <summary>
    /// Regression: a regex rule runs over the raw write payload with no trimming
    /// or normalising, and in .NET <c>$</c> also matches immediately before a
    /// line feed that ends the input. A pattern anchored with <c>$</c> therefore
    /// admits a value with a newline smuggled onto the end, which is a
    /// validation bypass for any caller that trusts the rule; <c>\z</c> is the
    /// anchor that admits only the true end of input, and the non-backtracking
    /// engine accepts it, so the linear-time guarantee is unchanged. This pins
    /// the semantics the Explorer's format patterns depend on.
    /// </summary>
    [Test]
    public void Compile_regex_end_of_input_anchor_refuses_a_trailing_newline_that_dollar_admits()
    {
        var dollar = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^[A-Z]{2}$"));
        var endOfInput = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^[A-Z]{2}\\z", description: "a two-letter country code"));

        Assert.Multiple(() =>
        {
            Assert.That(dollar.Validate(Utf8("GB")), Is.Null);
            Assert.That(dollar.Validate(Utf8("GB\n")), Is.Null, "why $ is not a safe tail anchor here");

            Assert.That(endOfInput.Validate(Utf8("GB")), Is.Null);
            Assert.That(endOfInput.Validate(Utf8("GB\n")), Is.EqualTo("a two-letter country code"));
            Assert.That(endOfInput.Validate(Utf8("GB\r\n")), Is.EqualTo("a two-letter country code"));
            Assert.That(endOfInput.Validate(Utf8("GB\nX")), Is.EqualTo("a two-letter country code"));
        });
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

    [Test]
    public void Compile_structured_rule_without_predicate_throws()
    {
        var rule = new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Structured };

        Assert.That(() => CompiledSchemaRule.Compile(rule), Throws.ArgumentException);
    }

    [Test]
    public void Compile_regex_rule_without_pattern_throws()
    {
        var rule = new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Regex };

        Assert.That(() => CompiledSchemaRule.Compile(rule), Throws.ArgumentException);
    }

    [Test]
    public void Compile_max_byte_length_rule_without_limit_throws()
    {
        var rule = new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.MaxByteLength,
        };

        Assert.That(() => CompiledSchemaRule.Compile(rule), Throws.ArgumentException);
    }

    [TestCase(-1)]
    [TestCase(int.MinValue)]
    public void Compile_max_byte_length_rule_with_negative_limit_throws(int limit)
    {
        // Regression for #4097: an initializer-built (or wire-decoded) rule skipped
        // the MaxLength factory's guard and compiled into a rule no value satisfies.
        var rule = new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.MaxByteLength,
            MaxByteLength = limit,
        };

        Assert.That(
            () => CompiledSchemaRule.Compile(rule),
            Throws.ArgumentException.With.Message.Contains("non-negative"));
    }

    [Test]
    public void Compile_ignores_a_negative_max_byte_length_on_another_encoding_kind()
    {
        var rule = new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.Utf8,
            MaxByteLength = -1,
        };

        var compiled = CompiledSchemaRule.Compile(rule);

        Assert.That(compiled.Validate(Utf8("text")), Is.Null);
    }

    [Test]
    public void Compile_unknown_rule_kind_throws()
    {
        var rule = new LatticeSchemaRule { Kind = (LatticeSchemaRuleKind)99 };

        Assert.That(() => CompiledSchemaRule.Compile(rule), Throws.ArgumentException);
    }

    [Test]
    public void Compiled_regex_exposes_the_compiled_pattern()
    {
        var compiled = CompiledSchemaRule.Compile(LatticeSchemaRule.Regex("^ok$"));

        Assert.That(compiled.Regex, Is.Not.Null);
        Assert.That(compiled.Regex!.IsMatch("ok"), Is.True);
    }

    [Test]
    public void Encoding_unknown_kind_returns_default_reason()
    {
        var compiled = CompiledSchemaRule.Compile(new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = (LatticeSchemaEncodingKind)99,
        });

        Assert.That(compiled.Validate(Utf8("anything")), Is.EqualTo("The value failed the encoding rule."));
        Assert.That(
            compiled.Validate(Utf8("anything")),
            Is.EqualTo("The value failed the encoding rule."),
            "The reason is stable across repeated validations of the same compiled rule.");
    }
}
