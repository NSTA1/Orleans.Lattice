using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A <see cref="DelegatingMcpServerTool"/> decorator that logs a timestamped line
/// when a repository-context tool call starts and a second line when it completes,
/// fails, or is cancelled - recording the tool name and the wall-clock duration -
/// then delegates to the inner tool unchanged.
/// </summary>
/// <remarks>
/// <para>
/// The repository-context group wraps every one of its tools in this decorator as
/// it assembles the tool list, so each <c>repocontext_*</c> call is bracketed by a
/// start and end log line. The two-line shape (rather than a single completion
/// line) means a call that hangs - a slow walk, a stalled embed - is still visible
/// in the log the moment it begins, not only once it returns.
/// </para>
/// <para>
/// The base <see cref="DelegatingMcpServerTool"/> forwards the advertised name,
/// schema, annotations, title, and description verbatim, so this decorator is
/// indistinguishable from the inner tool to a client and to the group's discovery
/// and schema tests; it overrides only <see cref="InvokeAsync"/> to bracket the
/// call. The lines are emitted under the dedicated
/// <see cref="LogCategory"/> category so an operator who finds the near-continuous
/// index-status poll too chatty can raise that one category's level without
/// silencing the meaningful indexing lifecycle lines.
/// </para>
/// <para>
/// Timing uses <see cref="Stopwatch.GetTimestamp"/> /
/// <see cref="Stopwatch.GetElapsedTime(long)"/> so no <see cref="Stopwatch"/> is
/// allocated per call. When no <see cref="ILoggerFactory"/> can be resolved from
/// the request scope the decorator simply delegates, adding nothing.
/// </para>
/// </remarks>
internal sealed class RepoContextToolInvocationLogger : DelegatingMcpServerTool
{
    /// <summary>
    /// The logger category the per-call start / completion lines are written under,
    /// distinct from the indexing lifecycle categories so its verbosity can be
    /// tuned independently.
    /// </summary>
    internal const string LogCategory = "Orleans.Lattice.Api.Mcp.RepoContext.ToolInvocation";

    /// <summary>Wraps <paramref name="inner"/> with per-invocation start / completion logging.</summary>
    /// <param name="inner">The repository-context tool whose invocations are logged.</param>
    public RepoContextToolInvocationLogger(McpServerTool inner)
        : base(inner)
    {
    }

    /// <inheritdoc />
    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var logger = request.Services?
            .GetService<ILoggerFactory>()?
            .CreateLogger(LogCategory);

        if (logger is null)
        {
            return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
        }

        var toolName = ProtocolTool.Name;
        var start = Stopwatch.GetTimestamp();
        logger.LogInformation("Repo-context tool {Tool} invoked.", toolName);

        try
        {
            var result = await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
            logger.LogInformation(
                "Repo-context tool {Tool} completed in {ElapsedMs} ms.",
                toolName, (long)Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            return result;
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation(
                "Repo-context tool {Tool} cancelled after {ElapsedMs} ms.",
                toolName, (long)Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Repo-context tool {Tool} failed after {ElapsedMs} ms.",
                toolName, (long)Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            throw;
        }
    }
}
