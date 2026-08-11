using System.Text.Json;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Client-side ergonomics for driving the repository-context MCP surface in a
/// test: discover the advertised tool set, and read the structured result or the
/// error text of a <see cref="CallToolResult"/> without repeating the same
/// projection in every fixture. These keep the tool sub-issues' assertions terse
/// and consistent.
/// </summary>
public static class RepoContextMcpClientExtensions
{
    /// <summary>
    /// Lists the names of the tools advertised to the connected session, as an
    /// ordinal <see cref="ISet{T}"/> for direct membership assertions (for
    /// example <c>Does.Contain("repocontext_health")</c> or
    /// <c>Is.Empty</c> for a fail-closed session).
    /// </summary>
    /// <param name="client">The connected MCP client.</param>
    /// <param name="cancellationToken">Cancels the list call.</param>
    /// <returns>The advertised tool names.</returns>
    public static async Task<IReadOnlySet<string>> ListToolNamesAsync(
        this McpClient client,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        var tools = await client.ListToolsAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return tools.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);
    }

    /// <summary>
    /// Returns the structured-content JSON of a successful tool result, failing
    /// with a clear message when the call errored or returned no structured
    /// content. Use it to assert on a tool's typed payload.
    /// </summary>
    /// <param name="result">The tool call result.</param>
    /// <returns>The structured-content root element.</returns>
    /// <exception cref="InvalidOperationException">
    /// The call reported an error, or carried no structured content.
    /// </exception>
    public static JsonElement RequireStructuredContent(this CallToolResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (result.IsError == true)
        {
            throw new InvalidOperationException(
                $"Expected a successful tool result but it was an error: {result.ErrorText()}");
        }

        if (result.StructuredContent is not { } structured)
        {
            throw new InvalidOperationException(
                "The tool result carried no structured content.");
        }

        return structured;
    }

    /// <summary>
    /// Returns the concatenated text-content of a tool result (the human-readable
    /// message an errored call carries), or the empty string when it has none.
    /// Use it to assert on the wording of an <c>isError</c> response.
    /// </summary>
    /// <param name="result">The tool call result.</param>
    /// <returns>The joined text content, or the empty string.</returns>
    public static string ErrorText(this CallToolResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        var blocks = result.Content.OfType<TextContentBlock>().Select(b => b.Text);
        return string.Join(Environment.NewLine, blocks);
    }
}
