namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>The broad outcome of a connection test.</summary>
internal enum ConnectionTestOutcome
{
    /// <summary>The endpoint answered an anonymous probe.</summary>
    Reachable = 0,

    /// <summary>The endpoint answered, and refused the anonymous probe: it is up and needs a sign-in.</summary>
    SignInRequired = 1,

    /// <summary>The endpoint did not answer, or answered with a failure other than an authentication refusal.</summary>
    Unreachable = 2,
}
