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

    /// <summary>
    /// Polls <c>repocontext_index_status</c> for a repository until its indexing
    /// job reaches a terminal state (<c>Completed</c> or <c>Failed</c>) and returns
    /// the final progress snapshot. Because onboarding now runs asynchronously off
    /// the request thread, a fixture starts a job with <c>repocontext_bootstrap</c>
    /// or <c>repocontext_add_repo</c> and then awaits this to observe the outcome.
    /// </summary>
    /// <param name="client">The connected MCP client.</param>
    /// <param name="repoId">The repository whose job to await.</param>
    /// <param name="cancellationToken">Cancels the poll loop.</param>
    /// <param name="timeout">The maximum time to wait for a terminal state.</param>
    /// <returns>The terminal progress snapshot's structured-content root element.</returns>
    /// <exception cref="TimeoutException">The job did not settle within <paramref name="timeout"/>.</exception>
    public static async Task<JsonElement> WaitForIndexAsync(
        this McpClient client,
        string repoId,
        CancellationToken cancellationToken = default,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(repoId);

        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        var args = new Dictionary<string, object?> { ["repoId"] = repoId };

        while (true)
        {
            var result = await client.CallToolAsync("repocontext_index_status", args, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var json = result.RequireStructuredContent();

            if (IsTerminal(json.GetProperty("status")))
            {
                return json;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"The indexing job for '{repoId}' did not reach a terminal state within the timeout.");
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reports whether an indexing <c>status</c> value denotes a terminal state,
    /// tolerating either the string enum form (<c>"Completed"</c>/<c>"Failed"</c>)
    /// or the numeric form (<c>2</c>/<c>3</c>) so the helper is robust to the
    /// tool's JSON enum representation.
    /// </summary>
    /// <param name="status">The <c>status</c> element from a progress snapshot.</param>
    /// <returns><see langword="true"/> when the job has completed or failed.</returns>
    private static bool IsTerminal(JsonElement status) => status.ValueKind switch
    {
        JsonValueKind.String => status.GetString() is "Completed" or "Failed",
        JsonValueKind.Number => status.GetInt32() is 2 or 3,
        _ => false,
    };
}
