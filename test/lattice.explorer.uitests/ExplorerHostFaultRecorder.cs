using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Captures the web head's warning-and-above log records into memory so a test failure
/// can report what the server actually said.
/// <para>
/// This exists because the head calls <c>ClearProviders()</c> - which is right, since an
/// unfiltered ASP.NET Core console log would bury the test output - but the consequence
/// was that <b>every server-side fault was discarded</b>. A Blazor Server circuit that
/// throws is terminated by the runtime, after which the page is still rendered but
/// completely inert: every later click does nothing and every later assertion fails
/// against a frozen document. With no provider attached, the only evidence reaching CI
/// was the Playwright timeout at the end of that chain, which names the locator that
/// timed out and says nothing about the exception that froze the page. That is precisely
/// why the intermittent journey failures were repeatedly written off as environmental -
/// the diagnosis was being thrown away before anyone could read it.
/// </para>
/// <para>
/// Records are kept in a bounded ring so a pathological logger cannot exhaust memory in
/// a long run, and the buffer is safe to read and write from different threads because
/// the server logs from request and circuit threads while a test reads from its own.
/// </para>
/// </summary>
internal sealed class ExplorerHostFaultRecorder : ILoggerProvider
{
    // Generous enough to hold the interesting part of a circuit teardown (the exception
    // plus surrounding context) and small enough to stay negligible.
    private const int Capacity = 200;

    private readonly Queue<string> _records = new(Capacity);
    private readonly Lock _gate = new();

    /// <summary>True when the head logged at least one warning-or-above record.</summary>
    public bool HasFaults
    {
        get
        {
            lock (_gate)
            {
                return _records.Count > 0;
            }
        }
    }

    /// <summary>
    /// The captured records, oldest first, or an empty array when the head logged
    /// nothing at warning level or above.
    /// </summary>
    public string[] Drain()
    {
        lock (_gate)
        {
            var drained = _records.ToArray();
            _records.Clear();
            return drained;
        }
    }

    /// <summary>
    /// The captured records as one reportable block, or <see langword="null"/> when there
    /// is nothing to report - so a caller can append it to a failure message without
    /// composing an empty section.
    /// </summary>
    public string? DescribeFaults()
    {
        var drained = Drain();
        return drained.Length == 0
            ? null
            : "The Explorer web head logged the following while this test ran:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, drained);
    }

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => new Sink(this, categoryName);

    /// <inheritdoc />
    public void Dispose()
    {
    }

    private void Record(string record)
    {
        lock (_gate)
        {
            if (_records.Count == Capacity)
            {
                _records.Dequeue();
            }

            _records.Enqueue(record);
        }
    }

    private sealed class Sink(ExplorerHostFaultRecorder owner, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        // Warning and above only. An informational ASP.NET Core log is noise here; a
        // warning is the level at which a circuit reports that it is going away.
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            ArgumentNullException.ThrowIfNull(formatter);

            var message = formatter(state, exception);
            var record = exception is null
                ? $"  [{logLevel}] {category}: {message}"
                : $"  [{logLevel}] {category}: {message}{Environment.NewLine}    {exception}";

            owner.Record(record);
        }
    }
}
