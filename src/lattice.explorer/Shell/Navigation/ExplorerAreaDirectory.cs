using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// The directory of native areas: every registered <see cref="IExplorerArea"/>,
/// validated once and held in spine order, and the fail-closed questions the
/// chrome asks of them.
/// </summary>
/// <remarks>
/// <para>
/// Keys are validated when the directory is built, so a misdeclared area fails
/// the circuit loudly rather than producing an unreachable stop: every key must be
/// a lower-case keyword, unique, and not one of the <see cref="ReservedKeys"/>.
/// </para>
/// <para>
/// Nothing is cached across calls: availability can change with sign-in or a
/// grant, so the chrome asks again on navigation, and an area memoizes its own
/// capability probe per circuit. <see cref="Invalidate"/> raises
/// <see cref="Changed"/> for a change the chrome cannot see, such as a sign-in.
/// </para>
/// </remarks>
internal sealed class ExplorerAreaDirectory
{
    private readonly IExplorerArea[] _areas;
    private readonly Dictionary<string, IExplorerArea> _byKey;
    private readonly ExplorerChromeOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    /// <summary>Builds the directory over every registered area.</summary>
    /// <param name="areas">The registered areas.</param>
    /// <param name="options">The chrome's time bounds.</param>
    /// <param name="time">The clock the bounds are measured on.</param>
    /// <param name="logger">Where a failing area is reported.</param>
    /// <exception cref="InvalidOperationException">An area's key is invalid, reserved or duplicated.</exception>
    public ExplorerAreaDirectory(
        IEnumerable<IExplorerArea> areas,
        ExplorerChromeOptions options,
        TimeProvider time,
        ILogger<ExplorerAreaDirectory>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(areas);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);

        _options = options;
        _time = time;
        _logger = logger ?? NullLogger<ExplorerAreaDirectory>.Instance;
        _byKey = new Dictionary<string, IExplorerArea>(StringComparer.Ordinal);

        foreach (var area in areas)
        {
            var key = area.Key;
            if (!ExplorerAddressEncoding.IsKeyword(key) || ReservedKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    $"The area {area.GetType().Name} declares the key '{key}', which is not a lower-case area key or is reserved ({string.Join(", ", ReservedKeys)}).");
            }

            if (!_byKey.TryAdd(key, area))
            {
                throw new InvalidOperationException(
                    $"The areas {_byKey[key].GetType().Name} and {area.GetType().Name} both declare the key '{key}'.");
            }
        }

        _areas = [.. _byKey.Values.OrderBy(area => area.DirectoryOrder).ThenBy(area => area.Key, StringComparer.Ordinal)];
    }

    /// <summary>Keys no area may take, because the grammar or a Shell page owns them.</summary>
    public static IReadOnlySet<string> ReservedKeys { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        ExplorerAddress.TenantSegment,
        ExplorerRoutes.NotFoundSegment,
    };

    /// <summary>Raised when something the chrome cannot observe may have changed which areas are shown.</summary>
    public event Action? Changed;

    /// <summary>Every registered area, in spine order, whatever its availability.</summary>
    public IReadOnlyList<IExplorerArea> Areas => _areas;

    /// <summary>The area with <paramref name="key"/>, or <see langword="null"/>.</summary>
    /// <param name="key">The area key.</param>
    public IExplorerArea? Find(string? key) =>
        key is not null && _byKey.TryGetValue(key, out var area) ? area : null;

    /// <summary>Announces that availability may have changed, so the chrome asks again.</summary>
    public void Invalidate() => Changed?.Invoke();

    /// <summary>
    /// The shown stops, in spine order: every area that answered
    /// <see cref="AreaAvailabilityKind.Visible"/> or
    /// <see cref="AreaAvailabilityKind.Unavailable"/> in time. All areas are asked
    /// in parallel.
    /// </summary>
    /// <param name="cancellationToken">Cancels the whole request.</param>
    public async Task<IReadOnlyList<ExplorerAreaEntry>> GetEntriesAsync(CancellationToken cancellationToken = default)
    {
        var probes = new Task<(AreaAvailability Availability, string? Badge)>[_areas.Length];
        for (var i = 0; i < _areas.Length; i++)
        {
            probes[i] = ProbeAsync(_areas[i], cancellationToken);
        }

        var answers = await Task.WhenAll(probes).ConfigureAwait(false);

        var entries = new List<ExplorerAreaEntry>(_areas.Length);
        for (var i = 0; i < _areas.Length; i++)
        {
            if (answers[i].Availability.IsShown)
            {
                entries.Add(new ExplorerAreaEntry(_areas[i], answers[i].Availability, answers[i].Badge));
            }
        }

        return entries;
    }

    /// <summary>
    /// Asks one area for its spine badge, failing closed to <see langword="null"/>
    /// on a fault or after <see cref="ExplorerChromeOptions.AvailabilityTimeout"/>.
    /// </summary>
    /// <param name="area">The area.</param>
    /// <param name="cancellationToken">The caller's token; its cancellation propagates.</param>
    public async Task<string?> GetDirectoryBadgeAsync(IExplorerArea area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);

        var (outcome, value, error) = await TimeBoxed
            .RunAsync(area.GetDirectoryBadgeAsync, _options.AvailabilityTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        if (outcome == TimeBoxed.Outcome.Completed)
        {
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }

        _logger.LogInformation(error, "The {Area} area did not report its directory badge ({Outcome}).", area.Key, outcome);
        return null;
    }

    /// <summary>
    /// Asks one area whether it may be seen, failing closed: a fault, a
    /// cancellation or no answer within <see cref="ExplorerChromeOptions.AvailabilityTimeout"/>
    /// is <see cref="AreaAvailability.Hidden"/>.
    /// </summary>
    /// <param name="area">The area.</param>
    /// <param name="cancellationToken">The caller's token; its cancellation propagates.</param>
    public async Task<AreaAvailability> GetAvailabilityAsync(IExplorerArea area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);

        var (outcome, value, error) = await TimeBoxed
            .RunAsync(area.GetAvailabilityAsync, _options.AvailabilityTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        if (outcome == TimeBoxed.Outcome.Completed)
        {
            return value;
        }

        _logger.LogWarning(
            error,
            "The {Area} area did not report its availability ({Outcome}); it is hidden.",
            area.Key,
            outcome);
        return AreaAvailability.Hidden;
    }

    private async Task<(AreaAvailability Availability, string? Badge)> ProbeAsync(IExplorerArea area, CancellationToken cancellationToken)
    {
        var availability = await GetAvailabilityAsync(area, cancellationToken).ConfigureAwait(false);
        var badge = availability.Kind == AreaAvailabilityKind.Visible
            ? await GetDirectoryBadgeAsync(area, cancellationToken).ConfigureAwait(false)
            : null;
        return (availability, badge);
    }

    /// <summary>
    /// Asks one area for its Home status line, failing closed to
    /// <see langword="null"/> (no line) on a fault or after
    /// <see cref="ExplorerChromeOptions.HomeStatusTimeout"/>.
    /// </summary>
    /// <param name="area">The area.</param>
    /// <param name="cancellationToken">The caller's token; its cancellation propagates.</param>
    public async Task<string?> GetHomeStatusAsync(IExplorerArea area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);

        var (outcome, value, error) = await TimeBoxed
            .RunAsync(area.GetHomeStatusAsync, _options.HomeStatusTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        if (outcome == TimeBoxed.Outcome.Completed)
        {
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }

        _logger.LogWarning(error, "The {Area} area did not report its Home status ({Outcome}).", area.Key, outcome);
        return null;
    }
}
