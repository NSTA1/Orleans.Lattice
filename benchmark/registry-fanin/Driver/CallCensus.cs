using System.Diagnostics;

namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// How a single driven grain call ended.
/// </summary>
internal enum CallOutcome
{
    /// <summary>The call returned.</summary>
    Ok,

    /// <summary>
    /// The call ended in a <see cref="TimeoutException"/> - the response deadline
    /// expired before the silo answered.
    /// </summary>
    Deadline,

    /// <summary>The call threw something other than a deadline.</summary>
    Fault,
}

/// <summary>
/// One recorded call. Deliberately carries a wall-clock instant as well as a
/// duration, because the measurement protocol buckets faults by TIMESTAMP: a
/// single counter scrape reads as an active fault and a delta of the same
/// counter reads as healthy, and only a timestamped distribution is truthful.
/// </summary>
/// <param name="StartedAtUtc">When the call was issued.</param>
/// <param name="ElapsedMs">How long it took to return or throw.</param>
/// <param name="Member">The grain member that was called.</param>
/// <param name="Outcome">How it ended.</param>
/// <param name="CarriedDiagnostics">
/// For a <see cref="CallOutcome.Deadline"/>, whether the exception message carried
/// Orleans' <c>Diagnostics:</c> clause. Orleans appends that clause only when the
/// target activation exists and answers a status probe, so its ABSENCE means the
/// call was never served because the activation was still ACTIVATING, while its
/// PRESENCE means the activation existed and was merely slow. That is precisely
/// the activation-blocking / turn-token-contention discriminator, read off text
/// the runtime already produces. Always <see langword="false"/> for other outcomes.
/// </param>
/// <param name="FaultType">The exception type name for a non-ok outcome.</param>
/// <param name="FaultMessage">
/// The exception message for a non-ok outcome, truncated. Carried because a
/// fault TYPE alone routinely cannot distinguish a rig misconfiguration from a
/// genuine server fault, and a rig that confuses the two produces measurements
/// nobody should trust.
/// </param>
internal readonly record struct CallSample(
    DateTimeOffset StartedAtUtc,
    double ElapsedMs,
    string Member,
    CallOutcome Outcome,
    bool CarriedDiagnostics,
    string? FaultType,
    string? FaultMessage);

/// <summary>
/// A recorded call together with the value it returned.
/// </summary>
/// <typeparam name="T">The call's result type.</typeparam>
/// <param name="Sample">The recorded sample.</param>
/// <param name="Value">The returned value, or <c>default</c> when the call did not return.</param>
internal readonly record struct CallResult<T>(CallSample Sample, T Value)
{
    /// <summary>How the call ended.</summary>
    public CallOutcome Outcome => Sample.Outcome;

    /// <summary>The exception type name for a non-ok outcome.</summary>
    public string? FaultType => Sample.FaultType;
}

/// <summary>
/// Client-side census of driven grain calls: per-call latency, deadline
/// exceptions, and peak in-flight concurrency.
/// <para>
/// It is deliberately independent of every server instrument. The registry
/// fan-in question is whether calls were SERVED SLOWLY or NEVER SERVED, and a
/// server-side instrument can only report calls the server actually admitted -
/// so a server-only census is blind to exactly the population under suspicion.
/// </para>
/// </summary>
internal sealed class CallCensus
{
    private readonly List<CallSample> _samples = [];
    private readonly Lock _gate = new();
    private int _inFlight;
    private int _peakInFlight;

    /// <summary>The highest number of calls this driver had outstanding at once.</summary>
    public int PeakInFlight => Volatile.Read(ref _peakInFlight);

    /// <summary>Every recorded call, in completion order.</summary>
    public IReadOnlyList<CallSample> Samples
    {
        get
        {
            lock (_gate)
            {
                return _samples.ToArray();
            }
        }
    }

    /// <summary>
    /// Runs <paramref name="call"/>, recording its latency, outcome and the
    /// in-flight watermark it contributed to.
    /// </summary>
    /// <param name="member">The grain member being called.</param>
    /// <param name="call">The call to drive.</param>
    /// <returns>The recorded sample.</returns>
    public async Task<CallSample> MeasureAsync(string member, Func<Task> call)
    {
        ArgumentNullException.ThrowIfNull(member);
        ArgumentNullException.ThrowIfNull(call);

        var entered = Interlocked.Increment(ref _inFlight);
        RaisePeak(entered);

        var startedAt = DateTimeOffset.UtcNow;
        var from = Stopwatch.GetTimestamp();
        CallOutcome outcome;
        var carriedDiagnostics = false;
        string? faultType = null;
        string? faultMessage = null;

        try
        {
            await call().ConfigureAwait(false);
            outcome = CallOutcome.Ok;
        }
        catch (TimeoutException ex)
        {
            outcome = CallOutcome.Deadline;
            faultType = ex.GetType().Name;
            faultMessage = Truncate(ex.Message);
            carriedDiagnostics = OrleansTimeoutText.CarriesDiagnosticsClause(ex);
        }
        catch (Exception ex)
        {
            outcome = CallOutcome.Fault;
            faultType = ex.GetType().Name;
            faultMessage = Truncate(ex.Message);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }

        var sample = new CallSample(
            startedAt,
            Stopwatch.GetElapsedTime(from).TotalMilliseconds,
            member,
            outcome,
            carriedDiagnostics,
            faultType,
            faultMessage);

        lock (_gate)
        {
            _samples.Add(sample);
        }

        return sample;
    }

    /// <summary>
    /// Runs a value-returning <paramref name="call"/>, recording it exactly as
    /// <see cref="MeasureAsync(string, Func{Task})"/> does and carrying the
    /// returned value through.
    /// </summary>
    /// <typeparam name="T">The call's result type.</typeparam>
    /// <param name="member">The grain member being called.</param>
    /// <param name="call">The call to drive.</param>
    /// <returns>The recorded sample and the value, with <c>default</c> on any fault.</returns>
    public async Task<CallResult<T>> MeasureAsync<T>(string member, Func<Task<T>> call)
    {
        ArgumentNullException.ThrowIfNull(member);
        ArgumentNullException.ThrowIfNull(call);

        T? value = default;
        // The lambda must be typed as Func<Task> explicitly. Written bare, the
        // assignment expression makes it a Func<Task<T>>, so overload resolution
        // picks THIS method and it recurses into itself - which compiles as a
        // plain type error here, but would otherwise be a silent infinite loop.
        Func<Task> inner = async () => value = await call().ConfigureAwait(false);
        var sample = await MeasureAsync(member, inner).ConfigureAwait(false);

        return new CallResult<T>(sample, value!);
    }

    private void RaisePeak(int observed)    {
        var peak = Volatile.Read(ref _peakInFlight);
        while (observed > peak)
        {
            var seen = Interlocked.CompareExchange(ref _peakInFlight, observed, peak);
            if (seen == peak)
            {
                return;
            }

            peak = seen;
        }
    }

    private static string Truncate(string message) =>
        message.Length <= 400 ? message : message[..400] + "...";
}
