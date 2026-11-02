namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Recognises the definite failure a silo running an older build raises when it
/// receives one of the append fence's new grain methods (epic #5605): the request
/// body names an invokable type whose alias the receiver's type manifest does not
/// contain, so the request is refused while it is deserialised, before any grain
/// code runs and so before any offset could be allocated. The receiver answers
/// with the deserialisation exception, a <see cref="TypeLoadException"/> raised by
/// Orleans' type converter. <c>WalAppendFenceMixedVersionTests</c> pins that shape
/// against a silo whose manifest lacks the invokables.
/// </summary>
/// <remarks>
/// Only this failure is re-issued unstamped. Any other failure - above all a
/// timeout - is ambiguous, because the append may have been allocated, so it keeps
/// today's retry semantics.
/// </remarks>
internal static class WalAppendFenceMethodProbe
{
    /// <summary>The prefix of the type converter's message for an alias it cannot resolve.</summary>
    internal const string UnresolvedAliasPrefix = "Unable to resolve type alias";

    /// <summary>
    /// <see langword="true"/> when <paramref name="exception"/>, or an exception it
    /// wraps, is the receiver's refusal to deserialise an invokable it does not know.
    /// </summary>
    internal static bool IsDefiniteMethodUnknown(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is TypeLoadException typeLoad
                && typeLoad.Message.StartsWith(UnresolvedAliasPrefix, StringComparison.Ordinal))
            {
                return true;
            }

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (!ReferenceEquals(inner, aggregate.InnerException) && IsDefiniteMethodUnknown(inner))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
