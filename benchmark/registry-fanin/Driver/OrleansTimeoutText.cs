namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// Reads the one discriminator Orleans already puts in a timeout's text.
/// <para>
/// When a grain call passes its response deadline, Orleans asks the target silo
/// for that message's status and appends the answer to the
/// <see cref="TimeoutException"/> it raises. The message template is
/// <c>Response did not arrive on time in '{Timeout}' for message: '{Message}'.
/// {StatusMessage}</c>, where <c>StatusMessage</c> is rendered from
/// <c>Status: '{Diagnostics}'.</c> - but ONLY when a status actually came back.
/// A target still running its activation cannot answer the probe, so the clause
/// is absent.
/// </para>
/// <para>
/// That makes the clause a free discriminator between the two mechanisms this
/// rig exists to separate. Its ABSENCE means the call was never served because
/// the activation had not finished activating, and an <c>[AlwaysInterleave]</c>
/// attribute cannot help there: interleaving admits a call past a running turn,
/// not past activation, which is the likeliest reason PR #3183 did not prevent
/// the observed storm. Its PRESENCE means the activation existed and was merely
/// slow, which is turn-token contention and wants a different remedy. Both
/// present identically as a 30s grain-call timeout, and nothing else separates
/// them without a controlled experiment - so it is read on every recorded
/// deadline rather than reconstructed afterwards.
/// </para>
/// <para>
/// Two markers are accepted because the wording is version-dependent: Orleans
/// 10.2.x renders <c>Status:</c> while earlier versions rendered
/// <c>Diagnostics:</c>. Matching only the version in front of you would make the
/// classifier answer "never served" for every timeout on the other one - a false
/// reading that points at the wrong remedy rather than failing obviously - so
/// both are matched deliberately.
/// </para>
/// </summary>
internal static class OrleansTimeoutText
{
    /// <summary>The clause Orleans 10.2.x appends when the target answered a status probe.</summary>
    internal const string StatusMarker = "Status:";

    /// <summary>The equivalent clause used by earlier Orleans versions.</summary>
    internal const string DiagnosticsMarker = "Diagnostics:";

    /// <summary>The message fragment that identifies a response-deadline timeout.</summary>
    internal const string DeadlineMarker = "did not arrive on time";

    /// <summary>
    /// Whether <paramref name="exception"/> - or any exception it wraps - carries
    /// the status / diagnostics clause.
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
            var message = current.Message;
            if (message.Contains(StatusMarker, StringComparison.Ordinal)
                || message.Contains(DiagnosticsMarker, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether <paramref name="exception"/> - or any exception it wraps - is a
    /// response-deadline timeout rather than some other timeout.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns><see langword="true"/> when it is a response-deadline timeout.</returns>
    public static bool IsResponseDeadline(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is TimeoutException
                && current.Message.Contains(DeadlineMarker, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
