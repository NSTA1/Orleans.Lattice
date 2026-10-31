using System.Diagnostics;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using NUnit.Framework.Internal;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// The estate's storage: every provider on Azurite, with the backup sink on a
/// <em>separate</em> account, as <c>reference-architecture/local/docker-compose.yml</c>
/// runs it. Endpoints named by <see cref="StorageVariable"/> and
/// <see cref="BackupStorageVariable"/> are used as they are; otherwise two Azurite
/// containers are started for the estate and removed with it. Every table and container
/// the estate creates carries the run-unique <see cref="Prefix"/>.
/// </summary>
/// <remarks>
/// <b>Fail closed, never inconclusive.</b> With <see cref="RequiredVariable"/> set to
/// <c>1</c> (as the CI lane sets it) storage that cannot be reached fails the fixture.
/// Only without it may a workstation with no Docker skip the fixture as inconclusive -
/// the false green the testing master warns about is not available to CI.
/// </remarks>
internal sealed class JourneyStorage : IAsyncDisposable
{
    /// <summary>A connection string to an existing primary account (Table, Blob, Queue).</summary>
    public const string StorageVariable = "LATTICE_E2E_STORAGE";

    /// <summary>A connection string to an existing, separate backup account (Blob).</summary>
    public const string BackupStorageVariable = "LATTICE_E2E_BACKUP_STORAGE";

    /// <summary>Set to <c>1</c> to make unreachable storage a failure rather than an inconclusive skip.</summary>
    public const string RequiredVariable = "LATTICE_E2E_REQUIRED";

    /// <summary>The Azurite image the harness starts, pinned.</summary>
    public const string AzuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.36.0";

    /// <summary>The emulator's fixed, publicly documented account key (not a secret).</summary>
    private const string EmulatorKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private static readonly TimeSpan DefaultReadyTimeout = TimeSpan.FromSeconds(90);

    private readonly List<string> _containers;
    private bool _reached;

    private JourneyStorage(string prefix, string connectionString, string backupConnectionString, List<string> containers)
    {
        Prefix = prefix;
        ConnectionString = connectionString;
        BackupConnectionString = backupConnectionString;
        _containers = containers;
    }

    /// <summary>The run-unique prefix of every table and container the estate uses: letters and digits only.</summary>
    public string Prefix { get; }

    /// <summary>The primary account's connection string.</summary>
    public string ConnectionString { get; }

    /// <summary>The backup account's connection string.</summary>
    public string BackupConnectionString { get; }

    /// <summary>The backup sink's container.</summary>
    public string BackupContainer => Prefix.ToLowerInvariant() + "-backups";

    /// <summary>A table name under the estate's prefix.</summary>
    /// <param name="purpose">What the table holds, such as <c>Clustering</c>.</param>
    public string Table(string purpose) => Prefix + purpose;

    /// <summary>Whether the CI lane requires storage (<see cref="RequiredVariable"/> is <c>1</c>).</summary>
    public static bool Required =>
        string.Equals(Environment.GetEnvironmentVariable(RequiredVariable), "1", StringComparison.Ordinal);

    /// <summary>
    /// The outcome when storage cannot be reached: an exception that fails the fixture when
    /// <paramref name="required"/>, or NUnit's inconclusive signal otherwise.
    /// </summary>
    /// <param name="reason">Why storage is unavailable.</param>
    /// <param name="required">Whether storage is required.</param>
    public static Exception Unavailable(string reason, bool required) =>
        required
            ? new InvalidOperationException(
                $"The journey estate requires Azure Storage ({RequiredVariable}=1) and it is unavailable: {reason}")
            : new InconclusiveException(
                $"The journey estate needs Azure Storage (Azurite) and it is unavailable: {reason}. "
                + $"Start Docker, or set {StorageVariable} and {BackupStorageVariable}.");

    /// <summary>Provisions storage for one estate.</summary>
    /// <param name="readyTimeout">How long storage is given to answer; 90 seconds by default.</param>
    /// <param name="cancellationToken">Cancels provisioning.</param>
    public static async Task<JourneyStorage> ProvisionAsync(TimeSpan? readyTimeout = null, CancellationToken cancellationToken = default)
    {
        var prefix = "e2e" + Guid.NewGuid().ToString("N")[..10];
        var primary = Environment.GetEnvironmentVariable(StorageVariable);
        var backup = Environment.GetEnvironmentVariable(BackupStorageVariable);
        var containers = new List<string>();

        if (string.IsNullOrWhiteSpace(primary) || string.IsNullOrWhiteSpace(backup))
        {
            var (tablePort, blobPort, queuePort, backupPort) = (LoopbackPorts.Next(), LoopbackPorts.Next(), LoopbackPorts.Next(), LoopbackPorts.Next());
            var primaryName = $"lattice-e2e-{prefix}";
            var backupName = $"lattice-e2e-{prefix}-backup";
            await StartAzuriteAsync(primaryName, $"-p 127.0.0.1:{blobPort}:10000 -p 127.0.0.1:{queuePort}:10001 -p 127.0.0.1:{tablePort}:10002", cancellationToken);
            containers.Add(primaryName);
            try
            {
                await StartAzuriteAsync(backupName, $"-p 127.0.0.1:{backupPort}:10000", cancellationToken);
                containers.Add(backupName);
            }
            catch
            {
                await RemoveContainersAsync(containers);
                throw;
            }

            primary = EmulatorConnectionString(blobPort, queuePort, tablePort);
            backup = EmulatorConnectionString(backupPort, null, null);
        }

        var storage = new JourneyStorage(prefix, primary, backup, containers);
        try
        {
            await storage.WaitUntilReachableAsync(readyTimeout ?? DefaultReadyTimeout, cancellationToken);
        }
        catch
        {
            await storage.DisposeAsync();
            throw;
        }

        return storage;
    }

    /// <summary>The estate's backup container, for reaching behind the backup catalogue.</summary>
    public BlobContainerClient BackupBlobs() => new BlobServiceClient(BackupConnectionString).GetBlobContainerClient(BackupContainer);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_containers.Count > 0)
        {
            await RemoveContainersAsync(_containers);
            return;
        }

        // Named, shared storage: drop what this estate created and leave the rest. Storage that
        // never answered holds nothing of this estate's, and is not asked again.
        if (!_reached)
        {
            return;
        }

        try
        {
            var tables = new TableServiceClient(ConnectionString);
            await foreach (var table in tables.QueryAsync($"TableName ge '{Prefix}' and TableName lt '{Prefix}~'"))
            {
                await tables.DeleteTableAsync(table.Name);
            }

            await new BlobServiceClient(BackupConnectionString).GetBlobContainerClient(BackupContainer).DeleteIfExistsAsync();
        }
        catch (Exception ex) when (ex is Azure.RequestFailedException or HttpRequestException or AggregateException)
        {
            TestContext.Progress.WriteLine($"The journey estate could not remove its storage ({Prefix}): {ex.Message}");
        }
    }

    private static string EmulatorConnectionString(int blobPort, int? queuePort, int? tablePort)
    {
        var connection = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={EmulatorKey};BlobEndpoint=http://127.0.0.1:{blobPort}/devstoreaccount1;";
        if (queuePort is { } queue)
        {
            connection += $"QueueEndpoint=http://127.0.0.1:{queue}/devstoreaccount1;";
        }

        if (tablePort is { } table)
        {
            connection += $"TableEndpoint=http://127.0.0.1:{table}/devstoreaccount1;";
        }

        return connection;
    }

    private async Task WaitUntilReachableAsync(TimeSpan readyTimeout, CancellationToken cancellationToken)
    {
        var deadline = Stopwatch.StartNew();
        string? last = null;
        while (deadline.Elapsed < readyTimeout)
        {
            try
            {
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attempt.CancelAfter(TimeSpan.FromSeconds(5));
                await new TableServiceClient(ConnectionString).GetPropertiesAsync(attempt.Token);
                await new BlobServiceClient(ConnectionString).GetPropertiesAsync(attempt.Token);
                await new BlobServiceClient(BackupConnectionString).GetPropertiesAsync(attempt.Token);
                _reached = true;
                return;
            }
            catch (Exception ex) when (ex is Azure.RequestFailedException or HttpRequestException or OperationCanceledException or AggregateException)
            {
                cancellationToken.ThrowIfCancellationRequested();
                last = ex.Message;
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }
        }

        throw Unavailable($"storage did not answer within {readyTimeout.TotalSeconds:0} seconds ({last})", Required);
    }

    private static async Task StartAzuriteAsync(string name, string ports, CancellationToken cancellationToken)
    {
        var (exit, output) = await DockerAsync(
            $"run -d --rm --name {name} {ports} {AzuriteImage} azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --skipApiVersionCheck --loose",
            cancellationToken);
        if (exit != 0)
        {
            throw Unavailable($"'docker run {AzuriteImage}' exited {exit}: {output.Trim()}", Required);
        }
    }

    private static async Task RemoveContainersAsync(IEnumerable<string> names)
    {
        foreach (var name in names)
        {
            await DockerAsync($"rm -f {name}", CancellationToken.None);
        }
    }

    private static async Task<(int Exit, string Output)> DockerAsync(string arguments, CancellationToken cancellationToken)
    {
        Process process;
        try
        {
            process = Process.Start(new ProcessStartInfo("docker", arguments)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            }) ?? throw Unavailable("the docker CLI did not start", Required);
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            throw Unavailable($"the docker CLI is not available ({ex.Message})", Required);
        }

        using (process)
        {
            var stdout = process.StandardOutput.ReadToEndAsync(cancellationToken);
            var stderr = process.StandardError.ReadToEndAsync(cancellationToken);
            await process.WaitForExitAsync(cancellationToken);
            return (process.ExitCode, await stdout + await stderr);
        }
    }
}
