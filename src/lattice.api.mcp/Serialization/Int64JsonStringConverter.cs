using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="System.Text.Json"/> converter that serializes <see cref="long"/>
/// (<c>int64</c>) values as JSON <b>strings</b> rather than JSON numbers, and
/// reads them back from either a string or a number token.
/// </summary>
/// <remarks>
/// <para>
/// MCP tool results are emitted twice in one response - once as a text block and
/// once as a structured-content block (a real JSON object). A JSON <i>number</i>
/// token wider than 2^53 cannot survive a round-trip through an IEEE-754 double,
/// which is exactly how many MCP hosts and LLM harnesses re-parse the structured
/// block, so a 64-bit HLC timestamp or value hash silently loses its low-order
/// digits and the two copies disagree (issue #1339). Emitting the value as a
/// string keeps it lexical: no client can round it through a double, so the two
/// copies stay byte-identical.
/// </para>
/// <para>
/// The converter is registered on the MCP tool serializer options
/// (<see cref="LatticeApiMcpToolSerialization.Options"/>) rather than annotating
/// the underlying primitives, so it applies only to the MCP tool surface and
/// leaves every other <c>int64</c> serialization path (core primitives, gRPC
/// bindings, the Explorer) untouched. Only <see cref="long"/> is affected;
/// <see cref="int"/> fields (such as a counter or a value length) stay JSON
/// numbers because they cannot exceed the double mantissa.
/// </para>
/// </remarks>
internal sealed class Int64JsonStringConverter : JsonConverter<long>
{
    /// <inheritdoc />
    public override long Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.String:
                // Parse straight from the raw UTF-8 value span, so a string-encoded
                // int64 round-trips without allocating an intermediate string. Only
                // the rare multi-segment token copies (via ToArray) onto the heap.
                ReadOnlySpan<byte> span = reader.HasValueSequence
                    ? reader.ValueSequence.ToArray()
                    : reader.ValueSpan;
                if (Utf8Parser.TryParse(span, out long parsed, out var consumed) && consumed == span.Length)
                {
                    return parsed;
                }

                throw new JsonException("Expected a base-10 Int64 value in the string token.");
            case JsonTokenType.Number:
                return reader.GetInt64();
            default:
                throw new JsonException(
                    $"Expected a string or number token for an Int64 value but found {reader.TokenType}.");
        }
    }

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, long value, JsonSerializerOptions options)
    {
        // Format into a stack buffer and write the UTF-8 span directly, avoiding a
        // per-field string allocation on the tool-result emit hot path. Int64 needs
        // at most 20 bytes ("-9223372036854775808").
        Span<byte> buffer = stackalloc byte[20];
        var formatted = Utf8Formatter.TryFormat(value, buffer, out var written);
        Debug.Assert(formatted, "a 20-byte buffer always fits an Int64");
        writer.WriteStringValue(buffer[..written]);
    }
}
