namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// A checkpoint whose checks failed: an accessibility violation, a browser console error, or
/// a host fault. Its screenshot is still written and listed in the manifest, marked failed.
/// </summary>
/// <remarks>
/// Thrown directly rather than through an NUnit assertion, so a self-test can expect it
/// without NUnit recording a failed assertion against the test that expected it.
/// </remarks>
public sealed class JourneyCheckpointException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">The checkpoint's report.</param>
    public JourneyCheckpointException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    public JourneyCheckpointException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">The checkpoint's report.</param>
    /// <param name="innerException">The cause.</param>
    public JourneyCheckpointException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
