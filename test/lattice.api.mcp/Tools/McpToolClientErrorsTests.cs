using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.Tests.Tools;

/// <summary>
/// Unit tests for <see cref="McpToolClientErrors"/>, the marker that classifies a
/// tool-call fault as the caller's mistake so <see cref="CredentialStampingTool"/>
/// answers it instead of throwing it (issue #3761).
/// </summary>
[TestFixture]
public sealed class McpToolClientErrorsTests
{
    [Test]
    public void InvalidArgument_marks_a_plain_McpException()
        => AssertMarked(McpToolClientErrors.InvalidArgument("bad"), McpToolClientErrorReason.InvalidArgument, "bad");

    [Test]
    public void RejectedContent_marks_a_plain_McpException()
        => AssertMarked(McpToolClientErrors.RejectedContent("refused"), McpToolClientErrorReason.RejectedContent, "refused");

    [Test]
    public void NotFound_marks_a_plain_McpException()
        => AssertMarked(McpToolClientErrors.NotFound("gone"), McpToolClientErrorReason.NotFound, "gone");

    // The enum is internal, so the cases name it and the method parses it back.
    [TestCase(nameof(McpToolClientErrorReason.InvalidArgument))]
    [TestCase(nameof(McpToolClientErrorReason.UnknownArgument))]
    [TestCase(nameof(McpToolClientErrorReason.RejectedContent))]
    [TestCase(nameof(McpToolClientErrorReason.NotFound))]
    public void Create_marks_every_reason(string reasonName)
    {
        var reason = Enum.Parse<McpToolClientErrorReason>(reasonName);
        AssertMarked(McpToolClientErrors.Create(reason, "m"), reason, "m");
    }

    [Test]
    public void Create_rejects_a_null_message()
        => Assert.Throws<ArgumentNullException>(() => McpToolClientErrors.Create(McpToolClientErrorReason.NotFound, null!));

    [Test]
    public void TryGetReason_is_false_for_an_unmarked_McpException()
    {
        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(new McpException("server"), out var reason), Is.False);
            Assert.That(reason, Is.EqualTo(default(McpToolClientErrorReason)));
        });
    }

    [Test]
    public void TryGetReason_is_false_for_a_marked_exception_that_is_not_an_McpException()
    {
        // The SDK only builds the caller-facing text for an McpException, so a mark
        // on any other type must not be honoured.
        var fault = new InvalidOperationException("server");
        fault.Data[McpToolClientErrors.ReasonDataKey] = McpToolClientErrorReason.NotFound;

        Assert.That(McpToolClientErrors.TryGetReason(fault, out _), Is.False);
    }

    [Test]
    public void TryGetReason_is_false_when_the_data_slot_holds_another_type()
    {
        var fault = new McpException("server");
        fault.Data[McpToolClientErrors.ReasonDataKey] = "NotFound";

        Assert.That(McpToolClientErrors.TryGetReason(fault, out _), Is.False);
    }

    [Test]
    public void FromArgumentBindingFault_marks_an_ArgumentException_raised_by_the_binder()
    {
        var fault = new ArgumentException("The arguments dictionary is missing a value for the required parameter 'key'.")
        {
            Source = "Microsoft.Extensions.AI.Abstractions",
        };

        var marked = McpToolClientErrors.FromArgumentBindingFault(fault, "lattice_data_get");

        Assert.That(marked, Is.Not.Null);
        AssertMarked(marked!, McpToolClientErrorReason.InvalidArgument,
            "The 'lattice_data_get' tool could not bind its arguments: " + fault.Message);
    }

    [Test]
    public void FromArgumentBindingFault_ignores_an_ArgumentException_raised_by_the_tool_itself()
    {
        // An ArgumentException from inside a tool's own call chain is a server
        // defect and must keep failing loudly.
        var fault = new ArgumentException("bug") { Source = "Orleans.Lattice.Api.Data" };

        Assert.That(McpToolClientErrors.FromArgumentBindingFault(fault, "t"), Is.Null);
    }

    [Test]
    public void FromArgumentBindingFault_ignores_an_ArgumentException_with_no_source()
    {
        var fault = new ArgumentException("bug") { Source = null };

        Assert.That(McpToolClientErrors.FromArgumentBindingFault(fault, "t"), Is.Null);
    }

    [Test]
    public void FromArgumentBindingFault_ignores_any_other_fault_type()
    {
        var fault = new InvalidOperationException("bug") { Source = "Microsoft.Extensions.AI" };

        Assert.That(McpToolClientErrors.FromArgumentBindingFault(fault, "t"), Is.Null);
    }

    [TestCase(nameof(McpToolClientErrorReason.InvalidArgument), LatticeApiMcpMetrics.ReasonInvalidArgument)]
    [TestCase(nameof(McpToolClientErrorReason.UnknownArgument), LatticeApiMcpMetrics.ReasonUnknownArgument)]
    [TestCase(nameof(McpToolClientErrorReason.RejectedContent), LatticeApiMcpMetrics.ReasonRejectedContent)]
    [TestCase(nameof(McpToolClientErrorReason.NotFound), LatticeApiMcpMetrics.ReasonNotFound)]
    public void ReasonTag_maps_every_reason(string reasonName, string expected)
        => Assert.That(
            McpToolClientErrors.ReasonTag(Enum.Parse<McpToolClientErrorReason>(reasonName)), Is.EqualTo(expected));

    [Test]
    public void ReasonTag_has_an_arm_for_every_declared_reason()
    {
        foreach (var reason in Enum.GetValues<McpToolClientErrorReason>())
        {
            Assert.That(() => McpToolClientErrors.ReasonTag(reason), Throws.Nothing, reason.ToString());
        }
    }

    [Test]
    public void ReasonTag_throws_for_an_unmapped_value()
        => Assert.Throws<ArgumentOutOfRangeException>(() => McpToolClientErrors.ReasonTag((McpToolClientErrorReason)99));

    [Test]
    public void SanitizeForEcho_returns_an_ordinary_message_unchanged()
    {
        const string message = "The 'scope' value 'Widgets' is not recognised.";

        Assert.That(McpToolClientErrors.SanitizeForEcho(message), Is.SameAs(message),
            "A message needing no repair must not be re-allocated.");
    }

    // Every character that ends or reframes a record in a line-oriented sink. A
    // caller that lands one of these in a rejection message forges a whole extra
    // log record beside the genuine one, and can hide the call that wrote it.
    [TestCase("\r", TestName = "SanitizeForEcho_replaces_carriage_return")]
    [TestCase("\n", TestName = "SanitizeForEcho_replaces_line_feed")]
    [TestCase("\r\n", TestName = "SanitizeForEcho_replaces_a_crlf_pair")]
    [TestCase("\u0000", TestName = "SanitizeForEcho_replaces_nul")]
    [TestCase("\u001B", TestName = "SanitizeForEcho_replaces_the_escape_character")]
    [TestCase("\u0085", TestName = "SanitizeForEcho_replaces_next_line")]
    [TestCase("\u2028", TestName = "SanitizeForEcho_replaces_the_unicode_line_separator")]
    [TestCase("\u2029", TestName = "SanitizeForEcho_replaces_the_unicode_paragraph_separator")]
    public void SanitizeForEcho_replaces_a_record_breaking_character(string injected)
    {
        var sanitized = McpToolClientErrors.SanitizeForEcho($"No record exists at 'k{injected}FAKE'.");

        Assert.Multiple(() =>
        {
            Assert.That(sanitized, Is.EqualTo($"No record exists at 'k{new string('?', injected.Length)}FAKE'."));

            // Ordinal, deliberately: NUnit's Does.Not.Contain is culture-sensitive,
            // and linguistic comparison gives NUL and ESC zero weight, so they are
            // "found" in every string and the constraint would fail on a correct
            // result.
            Assert.That(sanitized.Contains(injected, StringComparison.Ordinal), Is.False);
        });
    }

    [Test]
    public void SanitizeForEcho_keeps_the_surrounding_text_intact()
    {
        // The message must stay actionable: only the offending characters change.
        var sanitized = McpToolClientErrors.SanitizeForEcho("The key 'a\rb' is not well-formed.");

        Assert.That(sanitized, Is.EqualTo("The key 'a?b' is not well-formed."));
    }

    [Test]
    public void SanitizeForEcho_caps_a_message_whose_length_the_caller_chose()
    {
        var overlong = new string('a', McpToolClientErrors.MaxEchoedMessageLength * 4);

        var sanitized = McpToolClientErrors.SanitizeForEcho(overlong);

        Assert.Multiple(() =>
        {
            Assert.That(sanitized, Has.Length.EqualTo(
                McpToolClientErrors.MaxEchoedMessageLength + McpToolClientErrors.Ellipsis.Length));
            Assert.That(sanitized, Does.EndWith(McpToolClientErrors.Ellipsis));
            Assert.That(sanitized, Is.Not.EqualTo(overlong));
        });
    }

    [Test]
    public void SanitizeForEcho_keeps_a_message_exactly_at_the_cap()
    {
        var atCap = new string('a', McpToolClientErrors.MaxEchoedMessageLength);

        Assert.That(McpToolClientErrors.SanitizeForEcho(atCap), Is.EqualTo(atCap),
            "The cap is inclusive, so no ellipsis is appended to a message that fits.");
    }

    [Test]
    public void SanitizeForEcho_repairs_a_message_that_is_both_overlong_and_injected()
    {
        // Truncation alone would not help: the injection sits inside the kept prefix.
        var sanitized = McpToolClientErrors.SanitizeForEcho(
            "k\r\nFAKE" + new string('a', McpToolClientErrors.MaxEchoedMessageLength * 2));

        Assert.Multiple(() =>
        {
            Assert.That(sanitized, Does.StartWith("k??FAKE"));
            Assert.That(sanitized, Does.Not.Contain("\r").And.Not.Contain("\n"));
            Assert.That(sanitized, Has.Length.EqualTo(
                McpToolClientErrors.MaxEchoedMessageLength + McpToolClientErrors.Ellipsis.Length));
        });
    }

    [Test]
    public void SanitizeForEcho_screens_every_character_its_rewrite_would_replace()
    {
        // The fast path returns the input unchanged when a SearchValues screen finds
        // nothing to repair, so a character the rewrite rejects but the screen misses
        // would be echoed raw. This walks the whole BMP to pin the two in agreement.
        for (var c = '\u0000'; c < '\uFFFF'; c++)
        {
            var sanitized = McpToolClientErrors.SanitizeForEcho($"x{c}y");
            var expected = char.IsControl(c) || c is '\u2028' or '\u2029' ? "x?y" : $"x{c}y";
            Assert.That(sanitized, Is.EqualTo(expected), $"U+{(int)c:X4}");
        }
    }

    [Test]
    public void SanitizeForEcho_rejects_a_null_message()
        => Assert.Throws<ArgumentNullException>(() => McpToolClientErrors.SanitizeForEcho(null!));

    [Test]
    public void SanitizeForEcho_accepts_an_empty_message()
        => Assert.That(McpToolClientErrors.SanitizeForEcho(string.Empty), Is.Empty);

    private static void AssertMarked(McpException exception, McpToolClientErrorReason expected, string message)
    {
        Assert.Multiple(() =>
        {
            Assert.That(exception.GetType(), Is.EqualTo(typeof(McpException)),
                "The exact type must be unchanged so every existing catch and type assertion still holds.");
            Assert.That(exception.Message, Is.EqualTo(message));
            Assert.That(McpToolClientErrors.TryGetReason(exception, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(expected));
        });
    }
}
