namespace Orleans.Lattice.Schema;

/// <summary>
/// The shared description of a stored value the read-path
/// <see cref="ILatticeValueDecoder"/> cannot serve at the tree's target shape: a
/// value stamped newer than the target, one with no upcast path to it, or one whose
/// upcast cannot run over its body. Schema enforcement rejects or dead-letters such
/// a value (issue #5239) and the compliance scan counts it as non-compliant (issue
/// #5253) under the same reason text.
/// </summary>
internal static class SchemaUndecodableValue
{
    /// <summary>The fixed opening of the reason; the decoder's own message follows it.</summary>
    internal const string ReasonPrefix = "The value's version envelope cannot be decoded to the tree's target shape. ";

    /// <summary>
    /// Whether <paramref name="exception"/>, raised by
    /// <see cref="ILatticeValueDecoder.DecodeAsync"/>, means the value cannot be
    /// decoded to the target, rather than some other fault.
    /// </summary>
    /// <param name="exception">The exception the decode raised.</param>
    /// <returns><c>true</c> for a decode failure.</returns>
    internal static bool IsDecodeFailure(Exception exception) =>
        exception is NotSupportedException or InvalidOperationException;

    /// <summary>The reason recorded for a value whose decode raised <paramref name="exception"/>.</summary>
    /// <param name="exception">The decode failure.</param>
    /// <returns>The reason text.</returns>
    internal static string Reason(Exception exception) => ReasonPrefix + exception.Message;
}
