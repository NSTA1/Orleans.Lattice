namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// Reads the one discriminator Orleans already puts in a timeout's text.
/// <para>
/// Orleans appends a <c>Diagnostics: [...]</c> clause to a grain-call
/// <see cref="TimeoutException"/> ONLY when the target activation exists and
/// answers a status probe. So the clause's absence means the call was never
/// served because the activation was still ACTIVATING, and its presence means
/// the activation existed and was merely slow. Both present identically as a 30s
/// grain-call timeout, and nothing else separates them without a controlled
/// experiment - which is why this is read on every recorded deadline rather than
/// reconstructed afterwards.
/// </para>
/// </summary>
internal static class OrleansTimeoutText
{
    /// <summary>The clause Orleans appends when the target activation answered a status probe.</summary>
    internal const string DiagnosticsMarker = "Diagnostics:";

    /// <summary>
    /// Whether <paramref name="exception"/> - or any exception it wraps - carries
    /// the <c>Diagnostics:</c> clause.
    /// </summary>
    /// <param name="exception">The timeout to classify.</param>
    /// <returns>
    /// <see langword="true"/> when the target activation answered a status probe
    /// (activated but slow); <see langword="false"/> when it did not (stuck
    /// activating, so the call was never served).
    /// </returns>
    public static bool CarriesDiagnosticsClause(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current.Message.Contains(DiagnosticsMarker, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
