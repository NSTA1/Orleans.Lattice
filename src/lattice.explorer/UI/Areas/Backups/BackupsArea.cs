using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The shell-owned Backups area (epic #3807, E2): the backup catalogue,
/// capture, restore, schedules, health and catalogue maintenance, bound to
/// <see cref="ILatticeBackupControl"/>. It is seen only when the capability
/// probe says the caller could list backups, and it follows the active tenant.
/// </summary>
internal sealed class BackupsArea : IExplorerArea
{
    /// <summary>The palette command that opens the capture form.</summary>
    public const string CaptureCommandId = "backups.capture";

    private readonly BackupsAccess _access;
    private readonly ILatticeBackupControl _control;
    private readonly IServiceProvider _services;

    /// <summary>Creates the area.</summary>
    /// <param name="access">The area's probes.</param>
    /// <param name="control">The backup facade.</param>
    /// <param name="completions">The area's address completions.</param>
    /// <param name="services">
    /// The circuit's services. The navigator is resolved from them only when the
    /// palette runs the capture command, because the navigator itself depends on
    /// the area directory that holds this area.
    /// </param>
    public BackupsArea(BackupsAccess access, [FromKeyedServices(ShellFacades.Key)] ILatticeBackupControl control, BackupsCompletionSource completions, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(access);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(completions);
        ArgumentNullException.ThrowIfNull(services);
        _access = access;
        _control = control;
        _services = services;
        Completions = completions;
        Commands =
        [
            new ExplorerCommand(CaptureCommandId, "Capture backup...")
            {
                Detail = "Capture a full, incremental or set backup",
                Target = BackupsAddresses.Root,
                InvokeAsync = OpenCaptureAsync,
            },
        ];
    }

    /// <inheritdoc />
    public string Key => BackupsAddresses.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Backups";

    /// <inheritdoc />
    public int DirectoryOrder => 70;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken) =>
        await _access.GetAvailabilityAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        if (await _access.GetInventoryAsync(cancellationToken).ConfigureAwait(false) is { } inventory)
        {
            return inventory.TotalBackupCount == 0
                ? "No backups yet"
                : Plural(inventory.TotalBackupCount, "backup", "backups")
                    + (inventory.NewestBackupUtc is { } newest ? ", newest " + BackupsFormat.Time(newest) : string.Empty);
        }

        var page = await _control.ListBackupsAsync(
            new BackupCatalogRequest { PageSize = 1, OrderByCreatedDescending = true },
            cancellationToken).ConfigureAwait(false);
        return page.Entries.Count == 0
            ? "No backups yet"
            : "Newest backup " + BackupsFormat.Time(page.Entries[0].CreatedAtUtc);
    }

    private ValueTask OpenCaptureAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _services.GetRequiredService<ExplorerNavigator>().NavigateTo(BackupsAddresses.Capture);
        return ValueTask.CompletedTask;
    }

    private static string Plural(long count, string one, string many) =>
        count.ToString("N0", CultureInfo.InvariantCulture) + " " + (count == 1 ? one : many);
}
