using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Turns a fault from the schema facade into one plain sentence the area shows as
/// text. The facade's messages come from the cluster and are shown as text only,
/// never as markup.
/// </summary>
internal static class SchemaFailure
{
    /// <summary>The sentence for a cluster that serves no schema administration.</summary>
    public const string NotServed = "This cluster does not serve schema administration.";

    /// <summary>The sentence for a cluster that did not answer in time.</summary>
    public const string NotAnswering = "The cluster did not answer. Try again in a moment.";

    /// <summary>Describes <paramref name="exception"/> as a sentence about <paramref name="action"/>.</summary>
    /// <param name="exception">The fault.</param>
    /// <param name="action">What was being done, lower case, such as "read the policy".</param>
    /// <returns>A plain sentence.</returns>
    public static string Describe(Exception exception, string action)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentException.ThrowIfNullOrWhiteSpace(action);

        return exception switch
        {
            LatticeAuthorizationDeniedException => $"You are not permitted to {action}.",
            NotSupportedException => NotServed,
            ShellTransportException { IsTransient: true } => NotAnswering,
            ShellTransportException => $"The cluster could not {action}. {Sentence(exception.Message)}",
            _ =>  $"Could not {action}. {Sentence(exception.Message)}",
        };
    }

    private static string Sentence(string? message)
    {
        var text = (message ?? string.Empty).Trim();
        if (text.Length == 0)
        {
            return "No reason was given.";
        }

        return text.EndsWith('.') ? text : text + ".";
    }
}
