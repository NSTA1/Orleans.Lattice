using System.Globalization;
using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Operations;

/// <summary>
/// The words the Explorer uses for a long-running operation's status (#4122):
/// its state, its phase and its progress, kind-agnostic so every area that shows
/// a <see cref="LatticeOperationStatus"/> reads the same.
/// </summary>
internal static class OperationText
{
    /// <summary>The state in a word, such as "Running" or "Failed"; a running operation asked to stop reads "Cancelling".</summary>
    /// <param name="status">The status.</param>
    /// <returns>The state text.</returns>
    public static string State(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        return status.State switch
        {
            LatticeOperationState.Queued => status.CancelRequested ? "Cancelling" : "Queued",
            LatticeOperationState.Running => status.CancelRequested ? "Cancelling" : "Running",
            LatticeOperationState.Succeeded => "Succeeded",
            LatticeOperationState.Failed => "Failed",
            LatticeOperationState.Cancelled => "Cancelled",
            _ => "Unknown",
        };
    }

    /// <summary>The status pill role for a state.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The role.</returns>
    public static LtStateRole Role(LatticeOperationState state) => state switch
    {
        LatticeOperationState.Succeeded => LtStateRole.Healthy,
        LatticeOperationState.Failed => LtStateRole.Failed,
        LatticeOperationState.Cancelled => LtStateRole.Stalled,
        LatticeOperationState.Queued or LatticeOperationState.Running => LtStateRole.Lagging,
        _ => LtStateRole.Unknown,
    };

    /// <summary>
    /// A phase name as words: <c>CapturingMembers</c> reads "Capturing members". A
    /// name that already contains a space is returned unchanged.
    /// </summary>
    /// <param name="phase">The phase name.</param>
    /// <returns>The words.</returns>
    public static string Phase(string? phase)
    {
        if (string.IsNullOrEmpty(phase) || phase.Contains(' ', StringComparison.Ordinal))
        {
            return phase ?? string.Empty;
        }

        var builder = new StringBuilder(phase.Length + 4);
        for (var i = 0; i < phase.Length; i++)
        {
            var ch = phase[i];
            if (i > 0 && char.IsUpper(ch))
            {
                builder.Append(' ').Append(char.ToLowerInvariant(ch));
            }
            else
            {
                builder.Append(ch);
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// The current phase's progress in units, such as "1,200 of 5,000 entries", or
    /// "1,200 entries so far" when the total is not known, or <see langword="null"/>
    /// when the phase reports no units. Never a fabricated percentage.
    /// </summary>
    /// <param name="status">The status.</param>
    /// <returns>The text, or <see langword="null"/>.</returns>
    public static string? Units(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (status.UnitName is not { Length: > 0 } unit)
        {
            return null;
        }

        var done = Count(status.CompletedUnits);
        return status.TotalUnits is { } total
            ? done + " of " + Count(total) + " " + unit
            : done + " " + unit + " so far";
    }

    /// <summary>The step among the declared phases, such as "Step 2 of 3", or <see langword="null"/> when not known.</summary>
    /// <param name="status">The status.</param>
    /// <returns>The text, or <see langword="null"/>.</returns>
    public static string? Step(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        return status.PhaseIndex is { } index && status.PhaseCount is { } count && index >= 0 && index < count
            ? "Step " + (index + 1).ToString(CultureInfo.InvariantCulture) + " of " + count.ToString(CultureInfo.InvariantCulture)
            : null;
    }

    /// <summary>A whole count with group separators, such as "1,200".</summary>
    /// <param name="value">The count.</param>
    /// <returns>The text.</returns>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);
}
