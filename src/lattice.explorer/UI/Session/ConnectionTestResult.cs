using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>What a connection test found.</summary>
/// <param name="Outcome">The broad outcome.</param>
/// <param name="Message">The endpoint's own description of a failure, or <see langword="null"/>.</param>
internal sealed record ConnectionTestResult(ConnectionTestOutcome Outcome, string? Message = null)
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
}
