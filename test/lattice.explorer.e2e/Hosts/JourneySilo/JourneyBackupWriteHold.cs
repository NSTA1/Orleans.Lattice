using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// A fault seam (#5377): holds every backup artifact write this silo starts until the harness
/// releases it, as a congested backup storage account would. A capture started while the hold is
/// on stays running - long enough for a journey to cancel it - and is cancelled at once when the
/// operation's cancellation reaches it, because the hold honours the write's cancellation token.
/// </summary>
/// <remarks>
/// Only artifact writes are held: a capture writes its artifacts, then its manifest, then its
/// catalogue row, so a capture cancelled while held leaves no manifest and no catalogue row. A
/// hold lapses on its own after the duration it was given, so a harness that never releases it
/// cannot wedge the rest of the run.
/// </remarks>
internal sealed class JourneyBackupWriteHold
{
    private readonly Lock _gate = new();
    private TaskCompletionSource? _held;
    private CancellationTokenSource? _lapse;

    /// <summary>Whether artifact writes are held now.</summary>
    public bool IsHeld
    {
        get
        {
            lock (_gate)
            {
                return _held is not null;
            }
        }
    }

    /// <summary>
    /// Registers the hold and decorates the silo's registered <see cref="ILatticeBackupSink"/> with it.
    /// Call after the backup sink is registered.
    /// </summary>
    /// <param name="services">The silo's services.</param>
    public static void Decorate(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var registered = services.LastOrDefault(descriptor => descriptor.ServiceType == typeof(ILatticeBackupSink))
            ?? throw new InvalidOperationException("No ILatticeBackupSink is registered to hold.");
        services.AddSingleton<JourneyBackupWriteHold>();
        services.Replace(ServiceDescriptor.Singleton<ILatticeBackupSink>(provider => new HeldBackupSink(
            Resolve(registered, provider),
            provider.GetRequiredService<JourneyBackupWriteHold>())));
    }

    /// <summary>Holds every artifact write from now on, for at most <paramref name="lapse"/>.</summary>
    /// <param name="lapse">How long the hold lasts unless released first.</param>
    public void Hold(TimeSpan lapse)
    {
        CancellationTokenSource timer;
        lock (_gate)
        {
            _held ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _lapse?.Dispose();
            _lapse = timer = new CancellationTokenSource(lapse);
        }

        timer.Token.Register(() => Release(timer));
    }

    /// <summary>Releases every held write.</summary>
    public void Release() => Release(expected: null);

    /// <summary>Waits while the hold is on, or until <paramref name="cancellationToken"/> cancels the write.</summary>
    /// <param name="cancellationToken">The write's cancellation token.</param>
    public Task WaitAsync(CancellationToken cancellationToken)
    {
        TaskCompletionSource? held;
        lock (_gate)
        {
            held = _held;
        }

        return held is null ? Task.CompletedTask : held.Task.WaitAsync(cancellationToken);
    }

    private void Release(CancellationTokenSource? expected)
    {
        TaskCompletionSource? held;
        lock (_gate)
        {
            if (expected is not null && !ReferenceEquals(expected, _lapse))
            {
                // A newer hold replaced the one whose lapse fired.
                return;
            }

            held = _held;
            _held = null;
            _lapse = null;
        }

        held?.TrySetResult();
    }

    private static ILatticeBackupSink Resolve(ServiceDescriptor descriptor, IServiceProvider provider) =>
        (ILatticeBackupSink)(descriptor.ImplementationInstance
            ?? descriptor.ImplementationFactory?.Invoke(provider)
            ?? ActivatorUtilities.CreateInstance(provider, descriptor.ImplementationType
                ?? throw new InvalidOperationException("The registered ILatticeBackupSink has no implementation.")));
}
