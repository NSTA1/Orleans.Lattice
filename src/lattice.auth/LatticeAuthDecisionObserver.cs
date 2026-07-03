using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The post-decision observability seam the enforcement gate calls once per
/// gated decision. It records the decision metrics on the
/// <see cref="LatticeAuthMetrics"/> meter and, when auditing is enabled, builds a
/// <see cref="LatticeAuthDecisionEvent"/> and dispatches it to every registered
/// <see cref="ILatticeAuthAuditSink"/> at the configured verbosity and sampling.
/// </summary>
/// <remarks>
/// <para>
/// The gate calls this <b>after</b> it has computed the decision, so nothing here
/// can change, delay, or block the decision: sink dispatch is fire-and-forget and
/// never throws back into the gate. When neither a metrics listener is attached
/// nor auditing is enabled, <see cref="Observe"/> returns after a couple of
/// branch-predictable boolean reads, allocating nothing - the zero-cost-when-off
/// contract.
/// </para>
/// </remarks>
internal sealed partial class LatticeAuthDecisionObserver
{
    private readonly ILatticeAuthAuditSink[] _sinks;
    private readonly IOptionsMonitor<LatticeAuthOptions> _options;
    private readonly ILogger<LatticeAuthDecisionObserver> _logger;
    private readonly TimeProvider _time;

    /// <summary>Initializes a new <see cref="LatticeAuthDecisionObserver"/>.</summary>
    /// <param name="sinks">The registered audit sinks (may be empty).</param>
    /// <param name="options">The authorization options monitor.</param>
    /// <param name="logger">The logger for background audit-dispatch failures.</param>
    /// <param name="timeProvider">The clock used to stamp decision events; defaults to <see cref="TimeProvider.System"/>.</param>
    public LatticeAuthDecisionObserver(
        IEnumerable<ILatticeAuthAuditSink> sinks,
        IOptionsMonitor<LatticeAuthOptions> options,
        ILogger<LatticeAuthDecisionObserver> logger,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(sinks);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        _sinks = sinks as ILatticeAuthAuditSink[] ?? sinks.ToArray();
        _options = options;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// Whether audit dispatch is currently enabled. Read once by the gate to
    /// decide whether to take the detailed (rule-carrying) evaluation path.
    /// </summary>
    public bool IsAuditEnabled => _options.CurrentValue.EnableAuditSink;

    /// <summary>
    /// Captures a start timestamp for the decision-latency histogram, or <c>0</c>
    /// when the histogram has no listener (so no timing work is done off the hot
    /// path when nobody is measuring).
    /// </summary>
    /// <returns>A <see cref="Stopwatch"/> timestamp, or <c>0</c>.</returns>
    public static long CaptureStart() =>
        LatticeAuthMetrics.DecisionDuration.Enabled ? Stopwatch.GetTimestamp() : 0L;

    /// <summary>
    /// Records the metrics for a decision and, when auditing is enabled and the
    /// decision is admissible, dispatches an audit event.
    /// </summary>
    /// <param name="request">The authorized request.</param>
    /// <param name="decision">The decision the gate produced.</param>
    /// <param name="match">The winning rule match (default when no rule decided it).</param>
    /// <param name="epoch">The compiled policy epoch in force.</param>
    /// <param name="startTimestamp">The <see cref="CaptureStart"/> timestamp, or <c>0</c>.</param>
    public void Observe(
        in LatticeAccessRequest request,
        in LatticeAccessDecision decision,
        in PolicyMatch match,
        long epoch,
        long startTimestamp)
    {
        var decisionsEnabled = LatticeAuthMetrics.Decisions.Enabled;
        var durationEnabled = LatticeAuthMetrics.DecisionDuration.Enabled;
        var current = _options.CurrentValue;
        var auditEnabled = current.EnableAuditSink;

        if (!decisionsEnabled && !durationEnabled && !auditEnabled)
        {
            // Nothing is listening and auditing is off: zero-cost fast exit.
            return;
        }

        var effectTag = LatticeAuthMetrics.EffectTag(decision.Allowed);

        if (decisionsEnabled || durationEnabled)
        {
            var tags = new TagList
            {
                { LatticeAuthMetrics.TagOperation, LatticeOperationTag.For(request.Operation) },
                { LatticeAuthMetrics.TagTree, request.TreeId },
                { LatticeAuthMetrics.TagEffect, effectTag },
            };

            if (decisionsEnabled)
            {
                LatticeAuthMetrics.Decisions.Add(1, tags);
            }

            if (durationEnabled && startTimestamp != 0)
            {
                var elapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;
                LatticeAuthMetrics.DecisionDuration.Record(elapsedMs, tags);
            }
        }

        if (auditEnabled && Admits(current.AuditVerbosity, decision.Allowed) && Sampled(current.AuditSamplingRatio))
        {
            Dispatch(BuildEvent(in request, in decision, in match, epoch));
        }
    }

    private static bool Admits(LatticeAuthAuditVerbosity verbosity, bool allowed) =>
        verbosity == LatticeAuthAuditVerbosity.AllDecisions || !allowed;

    private static bool Sampled(double ratio)
    {
        if (ratio >= 1.0)
        {
            return true;
        }

        if (ratio <= 0.0)
        {
            return false;
        }

        return Random.Shared.NextDouble() < ratio;
    }

    private LatticeAuthDecisionEvent BuildEvent(
        in LatticeAccessRequest request,
        in LatticeAccessDecision decision,
        in PolicyMatch match,
        long epoch)
    {
        var matched = match.Matched;
        return new LatticeAuthDecisionEvent(
            request.Subject.SubjectId,
            request.Operation,
            request.TreeId,
            decision.Allowed ? LatticeEffect.Allow : LatticeEffect.Deny,
            epoch,
            _time.GetUtcNow(),
            request.Key,
            request.RangeStart,
            request.RangeEnd,
            matched ? match.RuleId : null,
            matched ? match.ScopeKind : null,
            matched ? match.ScopeValue : null,
            decision.Reason);
    }

    private void Dispatch(LatticeAuthDecisionEvent decisionEvent)
    {
        foreach (var sink in _sinks)
        {
            try
            {
                var pending = sink.WriteAsync(decisionEvent);
                if (!pending.IsCompletedSuccessfully)
                {
                    ObserveInBackground(pending, sink);
                }
            }
            catch (Exception ex)
            {
                LogSinkFault(_logger, sink.GetType().Name, ex);
            }
        }
    }

    private void ObserveInBackground(ValueTask pending, ILatticeAuthAuditSink sink)
    {
        _ = AwaitAsync(pending, sink);

        async Task AwaitAsync(ValueTask task, ILatticeAuthAuditSink faultedSink)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogSinkFault(_logger, faultedSink.GetType().Name, ex);
            }
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Authorization audit sink '{SinkType}' failed to record a decision event; the decision itself was unaffected.")]
    private static partial void LogSinkFault(ILogger logger, string sinkType, Exception exception);
}
