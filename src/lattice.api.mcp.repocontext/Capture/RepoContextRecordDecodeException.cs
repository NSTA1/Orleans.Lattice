using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Thrown when a stored repository-context memory value cannot be decoded, naming
/// the key that holds the malformed bytes.
/// <para>
/// The underlying decoders report a malformed payload positionally and nothing
/// else: <c>System.Text.Json</c> reports the offending byte and <c>Path: $</c> -
/// the document root, which is where every whole-value decode begins, so it
/// distinguishes nothing - and the Orleans record decoder reports only a field-id
/// mismatch. Neither carries the key, so a caller holding one bad record among
/// thousands learns that <em>a</em> record is malformed, not which one. That is
/// what turns a single corrupt entry into an unfindable one: the entry still reads
/// back through a plain fetch, so the failure surfaces only on the next
/// read-modify-write, by which time nothing ties the exception to the key.
/// </para>
/// <para>
/// This exception restores that tie: the message names the key, the decode stage,
/// the stored length, and a byte window at the failure offset, so the first line
/// identifies the record and the remedy instead of starting a forensic hunt.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <strong>Why the byte window is bounded and anchored, and why that bound is a
/// constraint rather than a convention.</strong> The population this exception
/// fires on is not arbitrary: the corruption it reports is produced by bodies
/// carrying credential-bearing URLs, so a diagnostic that renders stored bytes is
/// rendering bytes from a record that, by construction, may contain a secret. An
/// exception message propagates into logs, telemetry, CI transcripts, and pull
/// requests, so an unbounded preview would be a credential-exfiltration path
/// opened by the diagnostic for the credential problem.
/// </para>
/// <para>
/// Two properties make that structurally impossible rather than merely unlikely.
/// The window is <see cref="WindowByteCount"/> bytes - too few to carry any
/// credential of practical length, and enough only to identify which framing a
/// payload begins with, which is the entire diagnostic question. And it is
/// anchored at the decoder's reported failure offset rather than dumping a prefix
/// at a caller-influenced position, so it answers "what byte is the decoder
/// looking at" without becoming a general read primitive over the record. Both are
/// pinned by tests, not left as assumptions.
/// </para>
/// <para>
/// The window is rendered as hex and never as decoded text. A malformed payload is
/// untrusted input, and a value that reached this seam by carrying a shape some
/// upstream rewrote is exactly the value that must not be echoed back verbatim.
/// </para>
/// </remarks>
internal sealed class RepoContextRecordDecodeException : Exception
{
    /// <summary>
    /// The number of stored bytes rendered into the message, counted from the
    /// failure offset.
    /// <para>
    /// Deliberately small. A framing discriminator is decided within the first few
    /// bytes - a JSON object opens with a brace and a quote, an Orleans record and
    /// a framed envelope each open with their own marker - so this is sufficient
    /// for the only question the window exists to answer. It is far too small to
    /// carry a credential: the shortest token shapes in practice run to tens of
    /// characters, so no window of this size can reconstruct one even if it landed
    /// squarely on the secret.
    /// </para>
    /// </summary>
    internal const int WindowByteCount = 8;

    /// <summary>
    /// Initializes a new instance of the <see cref="RepoContextRecordDecodeException"/>
    /// class for the record at <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The full repository-context key whose stored value failed to decode.</param>
    /// <param name="stage">The decode stage that failed, as a short noun phrase.</param>
    /// <param name="stored">The stored bytes that failed to decode.</param>
    /// <param name="inner">The underlying decode failure.</param>
    internal RepoContextRecordDecodeException(string key, string stage, byte[]? stored, Exception inner)
        : base(BuildMessage(key, stage, stored, inner), inner)
    {
        Key = key;
        Stage = stage;
        StoredLength = stored?.Length ?? 0;
    }

    /// <summary>The full repository-context key whose stored value failed to decode.</summary>
    internal string Key { get; }

    /// <summary>The decode stage that failed.</summary>
    internal string Stage { get; }

    /// <summary>The length in bytes of the stored value that failed to decode.</summary>
    internal int StoredLength { get; }

    /// <summary>
    /// Builds the message: the key first, because that is the one fact every other
    /// diagnostic in this failure already lacks, then the stage, the length, the
    /// anchored byte window, the consequence, and the remedy.
    /// </summary>
    /// <param name="key">The full repository-context key.</param>
    /// <param name="stage">The decode stage that failed.</param>
    /// <param name="stored">The stored bytes that failed to decode.</param>
    /// <param name="inner">The underlying decode failure.</param>
    /// <returns>The composed message.</returns>
    private static string BuildMessage(string key, string stage, byte[]? stored, Exception inner)
    {
        var offset = ResolveFailureOffset(inner);
        return new StringBuilder()
            .Append("The stored repository-context record at '")
            .Append(key)
            .Append("' could not be decoded (stage: ")
            .Append(stage)
            .Append("; stored length: ")
            .Append(stored?.Length ?? 0)
            .Append(" bytes; ")
            .Append(DescribeWindow(stored, offset))
            .Append("). The stored value is malformed, so every read-modify-write against this key ")
            .Append("fails while it remains stored; a plain read can still succeed, which is why the ")
            .Append("failure surfaces only on write. Use 'repocontext_forget' to lapse or delete it. ")
            .Append("Underlying decode failure: ")
            .Append(inner.Message)
            .ToString();
    }

    /// <summary>
    /// Resolves the byte offset the decoder failed at, so the window is anchored at
    /// the fault rather than at a caller-influenced position.
    /// <para>
    /// Only a whole-document offset is honoured. <see cref="JsonException"/> reports
    /// a position <em>within a line</em>, which equals the document offset only on
    /// the first line; on any later line it is an offset into text this window has
    /// no business addressing, so anything else anchors at zero.
    /// </para>
    /// </summary>
    /// <param name="inner">The underlying decode failure.</param>
    /// <returns>The zero-based byte offset to anchor the window at.</returns>
    private static int ResolveFailureOffset(Exception inner)
    {
        if (inner is JsonException { LineNumber: 0, BytePositionInLine: { } position }
            && position >= 0
            && position <= int.MaxValue)
        {
            return (int)position;
        }

        return 0;
    }

    /// <summary>
    /// Renders a bounded window of stored bytes as space-separated uppercase hex,
    /// anchored at <paramref name="offset"/>.
    /// </summary>
    /// <param name="stored">The stored bytes, which may be empty or absent.</param>
    /// <param name="offset">The zero-based byte offset to anchor the window at.</param>
    /// <returns>The rendered window, or a marker when there are no bytes to render.</returns>
    private static string DescribeWindow(byte[]? stored, int offset)
    {
        if (stored is null || stored.Length == 0)
        {
            return "bytes: (none)";
        }

        var start = Math.Clamp(offset, 0, stored.Length - 1);
        var count = Math.Min(WindowByteCount, stored.Length - start);

        var builder = new StringBuilder("bytes at offset ")
            .Append(start)
            .Append(": ");
        for (var i = 0; i < count; i++)
        {
            if (i > 0)
            {
                builder.Append(' ');
            }

            builder.Append(stored[start + i].ToString("X2"));
        }

        return builder.ToString();
    }
}
