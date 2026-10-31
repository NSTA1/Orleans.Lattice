using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// One journey host running as a real process: launched with <c>dotnet {dll}</c> and
/// configured by environment variables only, with its standard output and error captured to
/// <c>{artifacts}/{name}.log</c> across every start, so a failing journey carries what each
/// process said. It can be killed (as a crash, not a graceful stop) and started again with
/// the same configuration.
/// </summary>
internal sealed class JourneyProcess : IAsyncDisposable
{
    private readonly string _dll;
    private readonly IReadOnlyDictionary<string, string> _environment;
    private readonly Func<CancellationToken, Task> _ready;
    private readonly StreamWriter _log;
    private readonly Lock _gate = new();
    private Process? _process;
    private int _starts;

    /// <summary>Describes, but does not start, a process.</summary>
    /// <param name="name">A file-safe name, such as <c>west-silo-0</c>.</param>
    /// <param name="dll">The host's entry assembly.</param>
    /// <param name="environment">Its environment.</param>
    /// <param name="artifacts">Where its log is written.</param>
    /// <param name="ready">Completes once the started process serves; throws if it never does.</param>
    public JourneyProcess(
        string name,
        string dll,
        IReadOnlyDictionary<string, string> environment,
        string artifacts,
        Func<CancellationToken, Task> ready)
    {
        Name = name;
        _dll = dll;
        _environment = environment;
        _ready = ready;
        LogFile = Path.Combine(artifacts, name + ".log");
        _log = new StreamWriter(new FileStream(LogFile, FileMode.Append, FileAccess.Write, FileShare.ReadWrite), Encoding.UTF8)
        {
            AutoFlush = true,
        };
    }

    /// <summary>The process's name in the estate.</summary>
    public string Name { get; }

    /// <summary>Where its output is captured.</summary>
    public string LogFile { get; }

    /// <summary>Whether a started process is still running.</summary>
    public bool IsRunning
    {
        get
        {
            lock (_gate)
            {
                return _process is { HasExited: false };
            }
        }
    }

    /// <summary>The running process's id, or <see langword="null"/>.</summary>
    public int? ProcessId
    {
        get
        {
            lock (_gate)
            {
                return _process is { HasExited: false } running ? running.Id : null;
            }
        }
    }

    /// <summary>Starts the process and waits until it serves.</summary>
    /// <param name="timeout">How long it is given to become ready.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task StartAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var info = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.GetDirectoryName(_dll)!,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add(_dll);
        foreach (var (key, value) in _environment)
        {
            info.Environment[key] = value;
        }

        var process = new Process { StartInfo = info, EnableRaisingEvents = true };
        process.OutputDataReceived += (_, line) => Write("out", line.Data);
        process.ErrorDataReceived += (_, line) => Write("err", line.Data);

        lock (_gate)
        {
            if (_process is { HasExited: false })
            {
                throw new InvalidOperationException($"{Name} is already running.");
            }

            _starts++;
            Write("harness", $"--- start #{_starts}: dotnet {Path.GetFileName(_dll)} ---");
            _process?.Dispose();
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            _process = process;
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(timeout);
        var exited = process.WaitForExitAsync(deadline.Token);
        var ready = _ready(deadline.Token);
        try
        {
            var first = await Task.WhenAny(ready, exited);
            if (first == exited && !ready.IsCompletedSuccessfully)
            {
                deadline.Token.ThrowIfCancellationRequested();
                throw new InvalidOperationException(
                    $"{Name} exited with code {process.ExitCode} before it served. Its log: {LogFile}{Environment.NewLine}{Tail()}");
            }

            await ready;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"{Name} did not serve within {timeout.TotalSeconds:0} seconds. Its log: {LogFile}{Environment.NewLine}{Tail()}");
        }
    }

    /// <summary>Kills the process and its children at once, as a crash would, and waits for it to be gone.</summary>
    public async Task KillAsync()
    {
        Process? process;
        lock (_gate)
        {
            process = _process;
        }

        if (process is null || process.HasExited)
        {
            return;
        }

        Write("harness", "--- killed ---");
        process.Kill(entireProcessTree: true);
        await process.WaitForExitAsync();
    }

    /// <summary>The last lines of the log, for a failure message.</summary>
    /// <param name="lines">How many lines.</param>
    public string Tail(int lines = 40)
    {
        try
        {
            using var stream = new FileStream(LogFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new StreamReader(stream);
            var all = reader.ReadToEnd().Split('\n');
            return string.Join('\n', all.Skip(Math.Max(0, all.Length - lines)));
        }
        catch (IOException)
        {
            return string.Empty;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await KillAsync();
        lock (_gate)
        {
            _process?.Dispose();
            _process = null;
        }

        await _log.DisposeAsync();
    }

    private void Write(string stream, string? line)
    {
        if (line is null)
        {
            return;
        }

        lock (_log)
        {
            _log.WriteLine($"{DateTimeOffset.UtcNow:HH:mm:ss.fff} [{stream}] {line}");
        }
    }
}
