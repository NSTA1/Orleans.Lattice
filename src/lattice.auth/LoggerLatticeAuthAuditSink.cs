using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The default <see cref="ILatticeAuthAuditSink"/>: writes each decision event to
/// the silo <see cref="ILogger"/>. A denied decision is logged at
/// <see cref="LogLevel.Warning"/> (it is the security-relevant signal); an
/// allowed decision is logged at <see cref="LogLevel.Debug"/> so an all-decisions
/// audit does not flood the default log. Registered by <c>AddLatticeAuth</c>.
/// </summary>
internal sealed partial class LoggerLatticeAuthAuditSink(ILogger<LoggerLatticeAuthAuditSink> logger)
    : ILatticeAuthAuditSink
{
    /// <inheritdoc />
    public ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default)
    {
        if (decisionEvent.Effect == LatticeEffect.Deny)
        {
            LogDenied(
                logger,
                decisionEvent.SubjectId,
                decisionEvent.Operation,
                decisionEvent.TreeId,
                decisionEvent.Key,
                decisionEvent.MatchedRuleId,
                decisionEvent.PolicyEpoch,
                decisionEvent.Reason);
        }
        else if (logger.IsEnabled(LogLevel.Debug))
        {
            LogAllowed(
                logger,
                decisionEvent.SubjectId,
                decisionEvent.Operation,
                decisionEvent.TreeId,
                decisionEvent.Key,
                decisionEvent.MatchedRuleId,
                decisionEvent.PolicyEpoch);
        }

        return ValueTask.CompletedTask;
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Authorization denied: subject '{SubjectId}' operation {Operation} on tree '{TreeId}' key '{Key}' (rule '{MatchedRuleId}', epoch {PolicyEpoch}): {Reason}")]
    private static partial void LogDenied(
        ILogger logger,
        string subjectId,
        LatticeOperation operation,
        string treeId,
        string? key,
        string? matchedRuleId,
        long policyEpoch,
        string? reason);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Authorization allowed: subject '{SubjectId}' operation {Operation} on tree '{TreeId}' key '{Key}' (rule '{MatchedRuleId}', epoch {PolicyEpoch}).")]
    private static partial void LogAllowed(
        ILogger logger,
        string subjectId,
        LatticeOperation operation,
        string treeId,
        string? key,
        string? matchedRuleId,
        long policyEpoch);
}
