using Yarp.ReverseProxy;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Gateway;

/// <summary>
/// The gateway's pending dropped calls: each armed match tears down the next proxied call
/// whose path contains it, once.
/// </summary>
internal sealed class GatewayFaults
{
    /// <summary>The cluster id of a region's console replicas.</summary>
    public const string ConsoleCluster = "console";

    private readonly Lock _gate = new();
    private readonly List<string> _armed = [];
    private readonly HashSet<(string Cluster, string Destination)> _drained = [];
    private volatile int _drainedCount;

    /// <summary>Takes <paramref name="destination"/> of <paramref name="cluster"/> out of rotation, or returns it.</summary>
    /// <param name="cluster">The cluster id.</param>
    /// <param name="destination">The destination id, such as <c>explorer-0</c>.</param>
    /// <param name="drained">Whether it is drained.</param>
    public void SetDrained(string cluster, string destination, bool drained)
    {
        lock (_gate)
        {
            if (drained)
            {
                _drained.Add((cluster, destination));
            }
            else
            {
                _drained.Remove((cluster, destination));
            }

            _drainedCount = _drained.Count;
        }
    }

    /// <summary>Whether any destination of <paramref name="cluster"/> is drained; lock-free when none is.</summary>
    /// <param name="cluster">The cluster id.</param>
    public bool HasDrained(string cluster)
    {
        if (_drainedCount == 0)
        {
            return false;
        }

        lock (_gate)
        {
            return _drained.Any(entry => entry.Cluster == cluster);
        }
    }

    /// <summary>Whether <paramref name="destination"/> of <paramref name="cluster"/> is drained.</summary>
    /// <param name="cluster">The cluster id.</param>
    /// <param name="destination">The destination id.</param>
    public bool IsDrained(string cluster, string destination)
    {
        lock (_gate)
        {
            return _drained.Contains((cluster, destination));
        }
    }

    /// <summary>Arms one drop for the next call whose path contains <paramref name="match"/>.</summary>
    /// <param name="match">A substring of the gRPC method path, such as <c>BulkLoadAppend</c>.</param>
    public void Arm(string match)
    {
        lock (_gate)
        {
            _armed.Add(match);
        }
    }

    /// <summary>Whether <paramref name="path"/> consumes an armed drop.</summary>
    /// <param name="path">The request path.</param>
    public bool TryConsume(PathString path)
    {
        var value = path.Value ?? string.Empty;
        lock (_gate)
        {
            for (var i = 0; i < _armed.Count; i++)
            {
                if (value.Contains(_armed[i], StringComparison.Ordinal))
                {
                    _armed.RemoveAt(i);
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>How many drops are still armed.</summary>
    public int Pending
    {
        get
        {
            lock (_gate)
            {
                return _armed.Count;
            }
        }
    }
}

/// <summary>The harness's control surface, served on the gateway's loopback control port only.</summary>
internal static class GatewayControl
{
    /// <summary>Maps the control endpoints, reachable only on <paramref name="controlPort"/>.</summary>
    /// <param name="app">The gateway.</param>
    /// <param name="controlPort">The control listener's port.</param>
    public static void MapGatewayControl(this WebApplication app, int controlPort)
    {
        var control = app.MapGroup("/control").RequireHost($"*:{controlPort}");

        control.MapPost("/fail-next", (string match, GatewayFaults faults) =>
        {
            faults.Arm(match);
            return Results.Ok(new { pending = faults.Pending });
        });

        control.MapPost("/drain", (string cluster, string destination, bool drained, GatewayFaults faults) =>
        {
            faults.SetDrained(cluster, destination, drained);
            return Results.Ok();
        });

        control.MapGet("/destinations", (IProxyStateLookup state, GatewayFaults faults, string? cluster) =>
        {
            var clusterId = string.IsNullOrEmpty(cluster) ? "silos" : cluster;
            if (!state.TryGetCluster(clusterId, out var model))
            {
                return Results.NotFound();
            }

            var destinations = model.Destinations.Values
                .Select(destination => new
                {
                    id = destination.DestinationId,
                    address = destination.Model.Config.Address,
                    health = destination.Health.Active.ToString(),
                    drained = faults.IsDrained(clusterId, destination.DestinationId),
                })
                .OrderBy(destination => destination.id, StringComparer.Ordinal)
                .ToArray();
            return Results.Ok(new { pendingFaults = faults.Pending, destinations });
        });
    }
}
