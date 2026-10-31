namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// A switch that makes the world's gRPC surface fail every call to the named
/// facade services with <c>Internal</c>, as the facades answered while a silo was
/// leaving the cluster and its grain activations were lost (issue #5420).
/// </summary>
/// <remarks>
/// The world is shared by every fixture, so a test arms it for one step and disposes
/// what <see cref="Fail"/> returns. Only calls whose method falls under an armed
/// service are failed; every other facade answers normally.
/// </remarks>
internal sealed class FacadeFaults
{
    private readonly Lock _gate = new();
    private readonly List<string> _armed = [];

    /// <summary>Fails every call to <paramref name="services"/> until the returned handle is disposed.</summary>
    /// <param name="services">The gRPC service names, such as <c>orleans.lattice.api.auth</c>.</param>
    /// <returns>A handle that disarms exactly these services.</returns>
    public IDisposable Fail(params string[] services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var prefixes = services.Select(service => "/" + service + "/").ToArray();
        lock (_gate)
        {
            _armed.AddRange(prefixes);
        }

        return new Disarm(this, prefixes);
    }

    /// <summary>Whether a call to <paramref name="method"/> (<c>/{service}/{method}</c>) is to fail.</summary>
    /// <param name="method">The full gRPC method path.</param>
    public bool Fails(string method)
    {
        lock (_gate)
        {
            foreach (var prefix in _armed)
            {
                if (method.StartsWith(prefix, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
    }

    private sealed class Disarm(FacadeFaults faults, string[] prefixes) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            lock (faults._gate)
            {
                foreach (var prefix in prefixes)
                {
                    faults._armed.Remove(prefix);
                }
            }
        }
    }
}
