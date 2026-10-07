using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// A replication read that produced no data, with the one sentence the page shows.
/// The sentence is fixed per kind: a server's fault detail is never echoed.
/// </summary>
/// <param name="Kind">Why the read produced nothing.</param>
/// <param name="Message">The sentence the page shows.</param>
internal sealed record ReplicationFault(ReplicationFaultKind Kind, string Message)
{
    /// <summary>
    /// Classifies <paramref name="exception"/> from a facade call about
    /// <paramref name="subject"/> (such as "replication status").
    /// </summary>
    /// <param name="exception">The fault the facade raised.</param>
    /// <param name="subject">What was being read, for the sentence.</param>
    public static ReplicationFault From(Exception exception, string subject)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        return exception switch
        {
            _ when BootstrapReadFenceErrors.IsBootstrapReadFence(exception) =>
                new(ReplicationFaultKind.Bootstrapping, "This tree is bootstrapping from a peer; reads resume when it completes."),
            UnauthorizedAccessException => new(ReplicationFaultKind.Denied, $"You are not allowed to see {subject} on this cluster."),
            NotSupportedException => NotServed(subject),
            _ => new(ReplicationFaultKind.Failed, $"The cluster could not report {subject}. Try again in a moment."),
        };
    }

    /// <summary>The fault for a facade the cluster does not serve.</summary>
    /// <param name="subject">What was being read.</param>
    public static ReplicationFault NotServed(string subject) =>
        new(ReplicationFaultKind.NotServed, $"This cluster does not serve {subject}.");
}
