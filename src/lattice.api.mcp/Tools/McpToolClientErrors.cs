using System.Buffers;
using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Marks and recognises tool-call faults that are the caller's mistake - a
/// missing or malformed argument, refused content, or an unknown record - so
/// <see cref="CredentialStampingTool"/> can return them as an MCP error result
/// instead of throwing them (issue #3761).
/// </summary>
/// <remarks>
/// <para>
/// The ModelContextProtocol SDK logs every exception a tool throws at Error level
/// with its stack, under "threw an unhandled exception", before it turns the
/// exception into an error result. A caller that omits a required argument
/// therefore reads, in the server log, exactly like a server fault, and the real
/// faults are lost among them. A marked fault is answered with the same error
/// result the SDK would have built, logged at Debug without a stack, and counted
/// on <see cref="LatticeApiMcpMetrics.ToolClientErrors"/>.
/// </para>
/// <para>
/// A client error is still an <see cref="McpException"/> of exactly that type:
/// the classification rides on <see cref="Exception.Data"/>, so every existing
/// catch and every caller that asserts the exact type is unaffected. A fault that
/// is not marked keeps its current path. In particular an authorization denial is
/// never marked: it is surfaced as a denial, never downgraded (see
/// <c>.github/instructions/security.instructions.md</c>).
/// </para>
/// </remarks>
internal static class McpToolClientErrors
{
    /// <summary>The <see cref="Exception.Data"/> key the classification is stored under.</summary>
    internal const string ReasonDataKey = "Orleans.Lattice.Api.Mcp.ClientErrorReason";

    /// <summary>
    /// How many characters of a client-error message are echoed before truncation.
    /// Generous enough to carry every message this package composes in full, while
    /// keeping the size of the rejection - and of the log record built from it - a
    /// bounded quantity rather than a caller-chosen one.
    /// </summary>
    internal const int MaxEchoedMessageLength = 2048;

    /// <summary>Marks a message that was truncated by the cap above.</summary>
    internal const string Ellipsis = "...";

    /// <summary>The replacement written in place of a character that must not be echoed.</summary>
    private const char Replacement = '?';

    /// <summary>
    /// Every character that could end or reframe a record in a line-oriented sink:
    /// the two control ranges (which cover CR, LF, NUL, and the escape character
    /// that drives a terminal control sequence) plus the Unicode line and paragraph
    /// separators, which a sink that splits on Unicode line breaks also honours.
    /// </summary>
    private static readonly SearchValues<char> UnsafeCharacters = SearchValues.Create(BuildUnsafeCharacters());

    /// <summary>Creates a fault for an argument that is missing, empty, or not recognised.</summary>
    /// <param name="message">The caller-facing message. It must not echo raw caller content.</param>
    /// <returns>A marked <see cref="McpException"/>.</returns>
    public static McpException InvalidArgument(string message)
        => Create(McpToolClientErrorReason.InvalidArgument, message);

    /// <summary>Creates a fault for an argument whose content was refused.</summary>
    /// <param name="message">The caller-facing message. It must not echo raw caller content.</param>
    /// <returns>A marked <see cref="McpException"/>.</returns>
    public static McpException RejectedContent(string message)
        => Create(McpToolClientErrorReason.RejectedContent, message);

    /// <summary>Creates a fault for a record or resource the call named that does not exist.</summary>
    /// <param name="message">The caller-facing message.</param>
    /// <returns>A marked <see cref="McpException"/>.</returns>
    public static McpException NotFound(string message)
        => Create(McpToolClientErrorReason.NotFound, message);

    /// <summary>Creates an <see cref="McpException"/> marked as a client error of <paramref name="reason"/>.</summary>
    /// <param name="reason">Why the call was rejected.</param>
    /// <param name="message">The caller-facing message.</param>
    /// <returns>A marked <see cref="McpException"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static McpException Create(McpToolClientErrorReason reason, string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var exception = new McpException(message);
        exception.Data[ReasonDataKey] = reason;
        return exception;
    }

    /// <summary>
    /// Reads the client-error classification off <paramref name="exception"/>.
    /// </summary>
    /// <param name="exception">The fault that escaped a tool invocation.</param>
    /// <param name="reason">The classification when the fault is a client error.</param>
    /// <returns><see langword="true"/> when <paramref name="exception"/> is a marked client error.</returns>
    public static bool TryGetReason(Exception exception, out McpToolClientErrorReason reason)
    {
        if (exception is McpException && exception.Data[ReasonDataKey] is McpToolClientErrorReason marked)
        {
            reason = marked;
            return true;
        }

        reason = default;
        return false;
    }

    /// <summary>
    /// Recognises the fault the SDK raises when a call's arguments cannot be bound
    /// to the tool method's parameters - most often a missing required argument -
    /// and returns it as a marked client error. Returns <see langword="null"/> for
    /// any other fault.
    /// </summary>
    /// <remarks>
    /// The binder in <c>Microsoft.Extensions.AI</c> throws an
    /// <see cref="ArgumentException"/> ("The arguments dictionary is missing a value
    /// for the required parameter ...") before the tool method runs. It is
    /// recognised by the assembly that raised it, not by type alone: an
    /// <see cref="ArgumentException"/> thrown from inside a tool's own call chain
    /// is a server defect and must keep failing loudly.
    /// </remarks>
    /// <param name="fault">The fault that escaped the inner tool.</param>
    /// <param name="toolName">The invoked tool, named in the message.</param>
    /// <returns>A marked <see cref="McpException"/>, or <see langword="null"/>.</returns>
    public static McpException? FromArgumentBindingFault(Exception fault, string toolName)
    {
        if (fault is not ArgumentException argument
            || argument.Source is not { } source
            || !source.StartsWith("Microsoft.Extensions.AI", StringComparison.Ordinal))
        {
            return null;
        }

        return InvalidArgument($"The '{toolName}' tool could not bind its arguments: {argument.Message}");
    }

    /// <summary>Maps <paramref name="reason"/> to its <c>reason</c> tag value.</summary>
    /// <param name="reason">The classification.</param>
    /// <returns>The snake_case tag value.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="reason"/> has no tag arm.</exception>
    public static string ReasonTag(McpToolClientErrorReason reason) => reason switch
    {
        McpToolClientErrorReason.InvalidArgument => LatticeApiMcpMetrics.ReasonInvalidArgument,
        McpToolClientErrorReason.UnknownArgument => LatticeApiMcpMetrics.ReasonUnknownArgument,
        McpToolClientErrorReason.RejectedContent => LatticeApiMcpMetrics.ReasonRejectedContent,
        McpToolClientErrorReason.NotFound => LatticeApiMcpMetrics.ReasonNotFound,
        _ => throw new ArgumentOutOfRangeException(
            nameof(reason),
            reason,
            "Every client-error reason must have a metric arm; an unmapped one would be counted under no reason at all."),
    };

    /// <summary>
    /// Renders a client-error message safe to echo, by replacing every character that
    /// could end or reframe a record in a line-oriented sink and capping the length.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The <c>InvalidArgument</c> and <c>RejectedContent</c> factories above document
    /// that a message "must not echo raw caller content", and the comparators that
    /// build them honour it structurally - <c>RepoContextBodyCredentials</c> reports
    /// only offsets and lengths, and <c>RepoContextBodyFraming</c> only names drawn
    /// from a fixed allow-list. Several messages are nonetheless composed by
    /// interpolating a caller-supplied key, scope, kind, or path directly, and those
    /// reach the server log verbatim. A value carrying CR/LF forges a whole extra
    /// record beside the genuine one in any plain-text sink, letting a caller write
    /// arbitrary attacker-chosen lines into the log and hide the call that did it.
    /// </para>
    /// <para>
    /// Applying the guard here, at the point every client-error message converges on
    /// its way to the caller and the log, is what makes it hold: the alternative is
    /// to remember it at each of the several dozen sites that compose such a message,
    /// which is precisely how the narrower fix for the unknown-argument path left
    /// its siblings exposed.
    /// </para>
    /// <para>
    /// <see cref="char.IsControl(char)"/> covers CR, LF, NUL, and the escape
    /// character that drives a terminal control sequence; U+2028 and U+2029 are added
    /// because a sink that splits on Unicode line breaks treats them as terminators
    /// too. A message needing neither replacement nor truncation is returned
    /// unchanged, so the common path allocates nothing.
    /// </para>
    /// </remarks>
    /// <param name="message">The composed client-error message.</param>
    /// <returns>The sanitized, length-capped message.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static string SanitizeForEcho(string message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var length = Math.Min(message.Length, MaxEchoedMessageLength);
        var truncated = length < message.Length;
        if (!truncated && !message.AsSpan().ContainsAny(UnsafeCharacters))
        {
            return message;
        }

        return string.Create(
            truncated ? length + Ellipsis.Length : length,
            (message, length, truncated),
            static (destination, state) =>
            {
                var (source, take, wasTruncated) = state;
                for (var i = 0; i < take; i++)
                {
                    var c = source[i];
                    destination[i] = IsUnsafe(c) ? Replacement : c;
                }

                if (wasTruncated)
                {
                    Ellipsis.CopyTo(destination[take..]);
                }
            });
    }

    /// <summary>Whether <paramref name="c"/> could end or reframe a record in a line-oriented sink.</summary>
    /// <param name="c">The candidate character.</param>
    /// <returns><see langword="true"/> when it must be replaced.</returns>
    private static bool IsUnsafe(char c) => char.IsControl(c) || c is '\u2028' or '\u2029';

    /// <summary>Materialises the unsafe set that backs <see cref="UnsafeCharacters"/>.</summary>
    /// <returns>Every character <see cref="IsUnsafe"/> rejects.</returns>
    private static char[] BuildUnsafeCharacters()
    {
        // The two control ranges are contiguous and small, so the set is built from
        // the same predicate the replacement loop applies. Deriving both from one
        // predicate is what keeps the fast-path screen and the rewrite in agreement:
        // a character the screen missed would be silently echoed unchanged.
        var characters = new List<char>(67);
        for (var c = '\u0000'; c <= '\u009F'; c++)
        {
            if (IsUnsafe(c))
            {
                characters.Add(c);
            }
        }

        characters.Add('\u2028');
        characters.Add('\u2029');
        return [.. characters];
    }
}
