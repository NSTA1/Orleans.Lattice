using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Strips composed physical tree ids from text and exceptions before they cross
/// the app-control facade, replacing each with the app-local name the caller
/// already knows. Extends the response-echoing discipline to the error path,
/// the one place a physical id could otherwise reach a caller holding no
/// operator capability.
/// </summary>
/// <remarks>
/// Two shapes are rewritten: an app tree <c>[t/{tenant}/]a/{app}/{tree}</c>
/// becomes <c>{tree}</c> for the app the verb named, or <c>{app}:{tree}</c> for
/// another app; and a remaining tenant qualification <c>t/{tenant}/</c> is
/// removed, which restores an adopted tree's tenant-local id. The patterns only
/// run on the error path, behind a cheap substring pre-check.
/// </remarks>
internal static class AppsControlExceptionSanitizer
{
    private const int MaxInspectedExceptions = 32;

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    private static readonly Regex AppTreePattern = new(
        @"(?<![A-Za-z0-9_/-])(?:t/[a-z0-9-]{1,63}/)?a/(?<app>[a-z][a-z0-9-]{1,30})/(?<tree>[a-z][a-z0-9_-]{0,127})?",
        RegexOptions.CultureInvariant | RegexOptions.Compiled,
        MatchTimeout);

    private static readonly Regex TenantQualificationPattern = new(
        @"(?<![A-Za-z0-9_/-])t/[a-z0-9-]{1,63}/(?=\S)",
        RegexOptions.CultureInvariant | RegexOptions.Compiled,
        MatchTimeout);

    /// <summary>Reports whether <paramref name="text"/> carries a composed tree id.</summary>
    /// <param name="text">The text to inspect.</param>
    /// <returns><c>true</c> when sanitizing would change the text.</returns>
    public static bool ContainsComposedId(string? text) =>
        MayContainComposedId(text)
        && (AppTreePattern.IsMatch(text!) || TenantQualificationPattern.IsMatch(text!));

    /// <summary>Rewrites every composed tree id in <paramref name="text"/> to its app-local name.</summary>
    /// <param name="text">The text to sanitize.</param>
    /// <param name="ownApp">The slug the verb named, rendered without an app qualifier; may be null.</param>
    /// <returns>The sanitized text, or the same instance when nothing needed rewriting.</returns>
    [return: NotNullIfNotNull(nameof(text))]
    public static string? SanitizeText(string? text, string? ownApp)
    {
        if (!MayContainComposedId(text))
        {
            return text;
        }

        var rewritten = AppTreePattern.Replace(text!, match => RenderLocal(match, ownApp));
        return TenantQualificationPattern.Replace(rewritten, string.Empty);
    }

    /// <summary>
    /// Builds a sanitized replacement for <paramref name="exception"/> when its
    /// message, or the message of any exception in its inner or aggregated graph, carries
    /// a composed tree id, or when the graph is too large to inspect fully. The replacement
    /// keeps the exception's category (cancellation, with
    /// its token; authorization; tenant; argument; not-found; timeout; otherwise
    /// invalid-operation) and drops the inner graph, which could still carry the id.
    /// </summary>
    /// <param name="exception">The exception about to cross the facade.</param>
    /// <param name="ownApp">The slug the verb named; may be null.</param>
    /// <param name="sanitized">The replacement exception when one is required.</param>
    /// <returns><c>true</c> when <paramref name="sanitized"/> must be thrown instead.</returns>
    public static bool TryRewrite(Exception exception, string? ownApp, [NotNullWhen(true)] out Exception? sanitized)
    {
        sanitized = null;
        if (!ChainContainsComposedId(exception))
        {
            return false;
        }

        var message = SanitizeText(exception.Message, ownApp);
        sanitized = exception switch
        {
            OperationCanceledException canceled => new OperationCanceledException(message, canceled.CancellationToken),
            LatticeAuthorizationDeniedException { TreeId.Length: > 0 } denied => new LatticeAuthorizationDeniedException(
                SanitizeText(denied.TreeId, ownApp),
                denied.Operation,
                denied.SubjectId,
                SanitizeText(denied.Reason, ownApp)),
            LatticeAuthorizationDeniedException => new LatticeAuthorizationDeniedException(message),
            LatticeTenantAccessDeniedException => new LatticeTenantAccessDeniedException(message),
            ArgumentException => new ArgumentException(message),
            KeyNotFoundException => new KeyNotFoundException(message),
            TimeoutException => new TimeoutException(message),
            _ => new InvalidOperationException(message),
        };
        return true;
    }

    private static bool ChainContainsComposedId(Exception exception)
    {
        // Walks the whole exception graph - every inner exception and every aggregated one - up
        // to a node budget. A graph larger than the budget cannot be certified free of composed
        // ids, so it is treated as carrying one (fail closed) and replaced.
        Span<Exception?> pending = new Exception?[MaxInspectedExceptions];
        var count = 0;
        var inspected = 0;
        pending[count++] = exception;
        while (count > 0)
        {
            var current = pending[--count]!;
            if (++inspected > MaxInspectedExceptions)
            {
                return true;
            }

            if (ContainsComposedId(current.Message))
            {
                return true;
            }

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (!TryPush(pending, ref count, inner))
                    {
                        return true;
                    }
                }
            }
            else if (current.InnerException is { } inner && !TryPush(pending, ref count, inner))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryPush(Span<Exception?> pending, ref int count, Exception? exception)
    {
        if (exception is null)
        {
            return true;
        }

        if (count == pending.Length)
        {
            return false;
        }

        pending[count++] = exception;
        return true;
    }

    private static bool MayContainComposedId([NotNullWhen(true)] string? text) =>
        text is not null
        && (text.Contains("a/", StringComparison.Ordinal) || text.Contains("t/", StringComparison.Ordinal));

    private static string RenderLocal(Match match, string? ownApp)
    {
        var app = match.Groups["app"].Value;
        var tree = match.Groups["tree"];
        if (!tree.Success)
        {
            return app;
        }

        return string.Equals(app, ownApp, StringComparison.Ordinal) ? tree.Value : app + ":" + tree.Value;
    }
}
