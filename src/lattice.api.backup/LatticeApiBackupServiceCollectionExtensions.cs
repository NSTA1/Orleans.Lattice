using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.Backup</c> backup / restore control facade on an
/// Orleans silo.
/// </summary>
public static class LatticeApiBackupServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic backup / restore control facade to the silo:
    /// binds <see cref="LatticeApiBackupOptions"/>, registers the
    /// <see cref="ILatticeBackupControl"/> singleton every transport binding
    /// (gRPC now, MCP later) adapts over, and registers an idempotency marker.
    /// It adds no transport behaviour of its own.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(ISiloBuilder, Action{LatticeBackupOptions})"/>:
    /// the backup engine is the source of truth for the capture, restore,
    /// catalog, sink, and authorization seams this facade drives. Calling it
    /// first fails fast with a clear message, mirroring how the sibling
    /// control-API add-on guards its ordering relative to the core registration.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiBackupOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLatticeBackup(...)</c> has not been called on the same
    /// builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeBackupApi(
        this ISiloBuilder builder,
        Action<LatticeApiBackupOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLatticeBackup registers the capture engine. Its
        // absence means the facade would have no engine to drive, so fail fast at
        // registration with an actionable message rather than failing obscurely
        // at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeBackupCaptureService)))
        {
            throw new InvalidOperationException(
                "AddLatticeBackupApi() must be called after AddLatticeBackup(). Register the backup " +
                "engine (siloBuilder.AddLatticeBackup(...)) before adding the backup control API.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiBackupOptions>();

        // The transport-agnostic control facade. Registered as a silo singleton
        // that every transport binding (gRPC now, MCP later) adapts over.
        builder.Services.TryAddSingleton<ILatticeBackupControl, LatticeBackupControl>();

        // Idempotency marker: the structural wiring runs once regardless of how
        // many times the host calls this method. A repeat call still layers any
        // supplied configure delegate above, matching how the sibling add-ons
        // treat repeated registration.
        builder.Services.TryAddSingleton<LatticeApiBackupMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeBackupApi"/> call a no-op for the structural wiring
    /// while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiBackupMarker
    {
    }
}
