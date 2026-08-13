using System.Text.Json;
using ModelContextProtocol.Protocol;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Fast unit coverage for <see cref="RepoContextMcpClientExtensions"/>: the
/// structured-content and error-text projections a fixture uses to assert on a
/// tool result, exercised against constructed <see cref="CallToolResult"/>
/// values so they run in the unit tier without a live server.
/// </summary>
[TestFixture]
public sealed class RepoContextMcpClientExtensionsTests
{
    [Test]
    public void RequireStructuredContent_returns_the_element_of_a_successful_result()
    {
        var result = new CallToolResult
        {
            StructuredContent = JsonSerializer.SerializeToElement(new { available = true }),
        };

        var json = result.RequireStructuredContent();
        Assert.That(json.GetProperty("available").GetBoolean(), Is.True);
    }

    [Test]
    public void RequireStructuredContent_throws_when_the_result_is_an_error()
    {
        var result = new CallToolResult
        {
            IsError = true,
            Content = { new TextContentBlock { Text = "denied" } },
        };

        var ex = Assert.Throws<InvalidOperationException>(() => result.RequireStructuredContent());
        Assert.That(ex!.Message, Does.Contain("denied"));
    }

    [Test]
    public void RequireStructuredContent_throws_when_there_is_no_structured_content()
    {
        var result = new CallToolResult();
        Assert.Throws<InvalidOperationException>(() => result.RequireStructuredContent());
    }

    [Test]
    public void ErrorText_joins_the_text_content_blocks()
    {
        var result = new CallToolResult
        {
            IsError = true,
            Content =
            {
                new TextContentBlock { Text = "first" },
                new TextContentBlock { Text = "second" },
            },
        };

        Assert.That(result.ErrorText(), Does.Contain("first").And.Contain("second"));
    }

    [Test]
    public void ErrorText_is_empty_when_there_are_no_text_blocks()
        => Assert.That(new CallToolResult().ErrorText(), Is.Empty);
}
