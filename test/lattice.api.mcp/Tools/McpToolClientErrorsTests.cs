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
