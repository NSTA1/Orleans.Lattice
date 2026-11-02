using System.Diagnostics;
using System.Globalization;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// The estate's Prometheus (#5362): a pinned <c>prom/prometheus</c> container that scrapes
/// every silo's <c>/metrics</c>, as the reference architecture's compose Prometheus does,
/// and the backend every silo's telemetry facade queries. Its targets live in a file the
/// harness rewrites as silos are added (Prometheus's file-based discovery re-reads it), so
/// it can start before the silos whose ports it needs to know.
/// </summary>
/// <remarks>
/// On Linux the container shares the host's network, so it reaches the silos on
/// <c>127.0.0.1</c>; elsewhere (Docker Desktop) it reaches them through
/// <c>host.docker.internal</c> and publishes its own port. A missing Docker fails closed
/// under <see cref="JourneyStorage.RequiredVariable"/>, exactly as missing storage does.
/// With <see cref="BinaryVariable"/> naming a Prometheus binary of the same version, the
/// harness runs it as a local process instead: the CI runner pool (<c>tools/ci-runners</c>)
/// has no Docker daemon and bakes that binary into its image.
/// </remarks>
internal sealed class JourneyPrometheus : IAsyncDisposable
{
    /// <summary>The Prometheus image the harness starts, pinned.</summary>
    public const string Image = "prom/prometheus:v3.1.0";

    /// <summary>A Prometheus binary (the version <see cref="Image"/> pins) to run as a process instead of the container.</summary>
    public const string BinaryVariable = "LATTICE_E2E_PROMETHEUS_BINARY";

    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(90);

    private readonly string _container;
    private readonly string _directory;
    private readonly string? _binary;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(5) };
    private readonly List<object> _targets = [];
    private Process? _process;
    private StreamWriter? _log;
    private bool _created;

    private JourneyPrometheus(string container, string directory, int port, string? binary)
    {
        _container = container;
        _directory = directory;
        _binary = binary;
        Port = port;
        Address = new Uri($"http://127.0.0.1:{port}/");
    }

    /// <summary>The loopback port Prometheus serves its API on.</summary>
    public int Port { get; }

    /// <summary>Prometheus's base address, which every silo's telemetry facade queries.</summary>
    public Uri Address { get; }

    /// <summary>Whether Prometheus is running (it is stopped by <see cref="StopAsync"/>).</summary>
    public bool IsRunning { get; private set; }

    private static bool HostNetwork => OperatingSystem.IsLinux();

    private static string? ConfiguredBinary =>
        Environment.GetEnvironmentVariable(BinaryVariable) is { Length: > 0 } binary && !string.IsNullOrWhiteSpace(binary) ? binary : null;

    /// <summary>Whether Prometheus reaches the silos on loopback: as a local process, or a container on the host's network.</summary>
    private bool OnLoopback => _binary is not null || HostNetwork;

    /// <summary>Starts Prometheus for one estate, with no targets yet.</summary>
    /// <param name="name">The estate's run-unique id, which names the container.</param>
    /// <param name="directory">A private directory for its configuration.</param>
    /// <param name="scrapeInterval">How often each silo is scraped.</param>
    /// <param name="cancellationToken">Cancels the start.</param>
    public static async Task<JourneyPrometheus> StartAsync(string name, string directory, TimeSpan scrapeInterval, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(directory);
        var port = LoopbackPorts.Next();
        var interval = $"{Math.Max(1, (int)Math.Ceiling(scrapeInterval.TotalSeconds))}s";
        var binary = ConfiguredBinary;

        // The container sees the directory at /etc/prometheus; a local process reads it where it is.
        var targetsFile = binary is null ? "/etc/prometheus/targets.json" : Path.Combine(directory, "targets.json");
        File.WriteAllText(Path.Combine(directory, "prometheus.yml"),
            $"""
            global:
              scrape_interval: {interval}
              evaluation_interval: {interval}
            scrape_configs:
              - job_name: orleans-lattice-silo
                metrics_path: /metrics
                file_sd_configs:
                  - files: [{JsonSerializer.Serialize(targetsFile)}]
                    refresh_interval: 2s
            """);

        var prometheus = new JourneyPrometheus($"lattice-e2e-{name}-prometheus", directory, port, binary);
        prometheus.WriteTargets();
        try
        {
            if (binary is not null)
            {
                prometheus.StartProcess();
            }
            else
            {
                var network = HostNetwork
                    ? "--network host"
                    : $"-p 127.0.0.1:{port}:{port} --add-host host.docker.internal:host-gateway";
                var (exit, output) = await JourneyStorage.DockerAsync(
                    $"run -d --name {prometheus._container} {network} -v \"{directory}:/etc/prometheus:ro\" {Image} "
                    + $"--config.file=/etc/prometheus/prometheus.yml --web.listen-address=:{port} --storage.tsdb.retention.time=1d",
                    cancellationToken);
                if (exit != 0)
                {
                    throw JourneyStorage.Unavailable($"'docker run {Image}' exited {exit}: {output.Trim()}", JourneyStorage.Required);
                }
            }

            prometheus._created = true;
            await prometheus.WaitUntilReadyAsync(cancellationToken);
            return prometheus;
        }
        catch
        {
            await prometheus.DisposeAsync();
            throw;
        }
    }

    /// <summary>Adds a silo's <c>/metrics</c> to the scrape targets.</summary>
    /// <param name="region">The silo's region, recorded as the <c>region</c> label.</param>
    /// <param name="index">The silo's index, recorded as the <c>silo</c> label.</param>
    /// <param name="httpPort">The silo's internal HTTP port.</param>
    public void AddSilo(string region, int index, int httpPort)
    {
        var host = OnLoopback ? "127.0.0.1" : "host.docker.internal";
        lock (_targets)
        {
            _targets.Add(new
            {
                targets = new[] { $"{host}:{httpPort}" },
                labels = new Dictionary<string, string>
                {
                    ["region"] = region,
                    ["silo"] = $"{region}-silo-{index.ToString(CultureInfo.InvariantCulture)}",
                },
            });
            WriteTargets();
        }
    }

    /// <summary>Stops Prometheus, as a telemetry outage would; its data is kept for <see cref="StartAgainAsync"/>.</summary>
    public async Task StopAsync()
    {
        if (_binary is not null)
        {
            await StopProcessAsync();
            IsRunning = false;
            return;
        }

        var (exit, output) = await JourneyStorage.DockerAsync($"stop -t 2 {_container}", CancellationToken.None);
        if (exit != 0)
        {
            throw new InvalidOperationException($"'docker stop {_container}' exited {exit}: {output.Trim()}");
        }

        IsRunning = false;
    }

    /// <summary>Starts the stopped Prometheus again and waits until it is ready.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task StartAgainAsync(CancellationToken cancellationToken = default)
    {
        if (_binary is not null)
        {
            StartProcess();
            await WaitUntilReadyAsync(cancellationToken);
            return;
        }

        var (exit, output) = await JourneyStorage.DockerAsync($"start {_container}", cancellationToken);
        if (exit != 0)
        {
            throw new InvalidOperationException($"'docker start {_container}' exited {exit}: {output.Trim()}");
        }

        await WaitUntilReadyAsync(cancellationToken);
    }

    /// <summary>
    /// Waits until every target has been scraped successfully at least twice, so a rate over
    /// the scrape window has two samples to draw.
    /// </summary>
    /// <param name="timeout">How long to wait.</param>
    public async Task WaitForScrapesAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        int expected;
        lock (_targets)
        {
            expected = _targets.Count;
        }

        string last = "no answer";
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var response = await _http.GetAsync(new Uri(Address, "api/v1/query?query=" + Uri.EscapeDataString("count_over_time(up{job=\"orleans-lattice-silo\"}[5m]) >= 2 and up == 1")));
                using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                var series = document.RootElement.GetProperty("data").GetProperty("result").GetArrayLength();
                if (series >= expected && expected > 0)
                {
                    return;
                }

                last = $"{series} of {expected} targets scraped twice";
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException or KeyNotFoundException or InvalidOperationException)
            {
                last = ex.Message;
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        throw new TimeoutException($"Prometheus did not scrape every silo within {timeout.TotalSeconds:0} seconds ({last}).");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        if (_created)
        {
            if (_binary is not null)
            {
                await StopProcessAsync();
            }
            else
            {
                await JourneyStorage.DockerAsync($"rm -f -v {_container}", CancellationToken.None);
            }

            _created = false;
        }

        _process?.Dispose();
        _process = null;
        if (_log is { } log)
        {
            lock (log)
            {
                log.Dispose();
            }

            _log = null;
        }
    }

    private string LogFile => Path.Combine(_directory, "prometheus.log");

    /// <summary>Runs the configured binary, its data under the private directory so a restart keeps it.</summary>
    private void StartProcess()
    {
        var info = new ProcessStartInfo(_binary!)
        {
            WorkingDirectory = _directory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add($"--config.file={Path.Combine(_directory, "prometheus.yml")}");
        info.ArgumentList.Add($"--web.listen-address=127.0.0.1:{Port.ToString(CultureInfo.InvariantCulture)}");
        info.ArgumentList.Add($"--storage.tsdb.path={Path.Combine(_directory, "data")}");
        info.ArgumentList.Add("--storage.tsdb.retention.time=1d");

        var process = new Process { StartInfo = info, EnableRaisingEvents = true };
        _log ??= new StreamWriter(new FileStream(LogFile, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { AutoFlush = true };
        var log = _log;
        process.OutputDataReceived += (_, line) => Write(log, line.Data);
        process.ErrorDataReceived += (_, line) => Write(log, line.Data);

        try
        {
            process.Start();
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            process.Dispose();
            throw JourneyStorage.Unavailable($"the Prometheus binary '{_binary}' did not start ({ex.Message})", JourneyStorage.Required);
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        _process?.Dispose();
        _process = process;

        static void Write(StreamWriter log, string? line)
        {
            if (line is null)
            {
                return;
            }

            lock (log)
            {
                try
                {
                    log.WriteLine(line);
                }
                catch (ObjectDisposedException)
                {
                    // Output that arrives after the estate is disposed has nowhere to go.
                }
            }
        }
    }

    private string Tail(int lines = 30)
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

    private async Task StopProcessAsync()
    {
        if (_process is not { HasExited: false } process)
        {
            return;
        }

        // Killed rather than signalled: its write-ahead log is on disk, and a restart replays it.
        process.Kill(entireProcessTree: true);
        using var exit = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await process.WaitForExitAsync(exit.Token);
    }

    private void WriteTargets()
    {
        // Written beside and renamed over, so Prometheus never reads half a file.
        var path = Path.Combine(_directory, "targets.json");
        var staging = path + ".tmp";
        File.WriteAllText(staging, JsonSerializer.Serialize(_targets));
        File.Move(staging, path, overwrite: true);
    }

    private async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + ReadyTimeout;
        string last = "no answer";
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_process is { HasExited: true } exited)
            {
                throw JourneyStorage.Unavailable(
                    $"Prometheus ({_binary}) exited with code {exited.ExitCode} before it was ready. Its log: {LogFile}{Environment.NewLine}{Tail()}",
                    JourneyStorage.Required);
            }

            try
            {
                using var response = await _http.GetAsync(new Uri(Address, "-/ready"), cancellationToken);
                if (response.IsSuccessStatusCode)
                {
                    IsRunning = true;
                    return;
                }

                last = $"{(int)response.StatusCode}";
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested)
            {
                last = ex.Message;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }

        throw JourneyStorage.Unavailable($"Prometheus did not become ready within {ReadyTimeout.TotalSeconds:0} seconds ({last})", JourneyStorage.Required);
    }
}
