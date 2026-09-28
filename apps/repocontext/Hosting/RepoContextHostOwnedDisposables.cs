namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Owns the disposable instances <see cref="RepoContextHostBuilder"/> constructs
/// eagerly, before the service container exists, and ties their disposal to the
/// lifetime of the host that was built around them.
/// </summary>
/// <remarks>
/// <para>
/// <b>The gap this closes (issue #3792).</b> The builder constructs its meters, the
/// metrics collector and the activation census eagerly, so their listeners are
/// running and their instruments published before the first scrape can arrive. They
/// were then registered as <b>instances</b>, and the service provider never disposes
/// an instance it did not create. The only disposal path was an
/// <c>ApplicationStopped</c> callback, which never fires for an application that is
/// built and disposed without being started, and which is never registered at all
/// when the build throws part-way through.
/// </para>
/// <para>
/// Each such host therefore left a live, process-wide <c>MeterListener</c> behind:
/// the metrics collector kept enabling every Lattice instrument and allocating a
/// label string on every measurement for the rest of the process. That is invisible
/// in production, where the host is always started and stopped, and it is fatal to
/// any zero-allocation assertion that later shares a test process with a fixture
/// that built a host - which is how it surfaced.
/// </para>
/// <para>
/// This owner is registered through a <b>factory</b>, which the container does track,
/// and is resolved as soon as the container is built, so disposing the application
/// disposes it. The builder also disposes it directly when the build throws. The
/// existing <c>ApplicationStopped</c> registrations are kept, because some members
/// must be released at stop rather than at disposal; every owned member's
/// <see cref="IDisposable.Dispose"/> is idempotent, so being released twice is safe.
/// </para>
/// </remarks>
internal sealed class RepoContextHostOwnedDisposables : IDisposable
{
    private readonly object _gate = new();
    private readonly List<IDisposable> _owned = [];
    private bool _disposed;

    /// <summary>The number of instances currently owned.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _owned.Count;
            }
        }
    }

    /// <summary>Takes ownership of <paramref name="instance"/> and returns it.</summary>
    /// <typeparam name="T">The owned instance's type.</typeparam>
    /// <param name="instance">The instance to dispose with the host.</param>
    /// <returns><paramref name="instance"/>, so construction and ownership read as one expression.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="instance"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The owner has already been disposed.</exception>
    public T Add<T>(T instance)
        where T : IDisposable
    {
        ArgumentNullException.ThrowIfNull(instance);

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _owned.Add(instance);
        }

        return instance;
    }

    /// <summary>
    /// Disposes every owned instance, most recently added first, so a listener is
    /// released after the instruments that were registered after it. Idempotent.
    /// </summary>
    /// <exception cref="AggregateException">One or more owned instances threw while being disposed; every other instance was still disposed.</exception>
    public void Dispose()
    {
        IDisposable[] owned;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            owned = [.. _owned];
            _owned.Clear();
        }

        List<Exception>? failures = null;
        for (var index = owned.Length - 1; index >= 0; index--)
        {
            try
            {
                owned[index].Dispose();
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException("One or more host-owned instances failed to dispose.", failures);
        }
    }
}
