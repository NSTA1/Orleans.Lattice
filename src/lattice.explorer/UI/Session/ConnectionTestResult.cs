using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>What a connection test found.</summary>
/// <remarks>
/// A result carries its outcome and nothing else. Every word the dialog shows is
/// fixed per outcome, so neither the endpoint's own status text nor an exception
/// message ever reaches the page: those would turn the test into an oracle that
/// describes whatever answered at an address the visitor chose.
/// </remarks>
/// <param name="Outcome">The broad outcome.</param>
internal sealed record ConnectionTestResult(ConnectionTestOutcome Outcome)
{
    /// <summary>The health state role the outcome is drawn in.</summary>
    public LtStateRole Role => Outcome switch
    {
        ConnectionTestOutcome.Reachable => LtStateRole.Healthy,
        ConnectionTestOutcome.SignInRequired => LtStateRole.Stalled,
        _ => LtStateRole.Failed,
    };

    /// <summary>The outcome in words, always shown beside the role's glyph.</summary>
    public string Text => Outcome switch
    {
        ConnectionTestOutcome.Reachable => "Reachable",
        ConnectionTestOutcome.SignInRequired => "Reachable - sign-in required",
        _ => "Unreachable",
    };

    /// <summary>A fixed explanation of the outcome, or <see langword="null"/> when the words say it all.</summary>
    public string? Hint => Outcome switch
    {
        ConnectionTestOutcome.Reachable => null,
        ConnectionTestOutcome.SignInRequired => "The endpoint answered and asks for a sign-in, which you can do after saving.",
        _ => "No Lattice API answered at this address. Check the endpoint and its transport settings.",
    };
}
