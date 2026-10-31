using Yarp.ReverseProxy;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Gateway;

/// <summary>
/// The gateway's pending dropped calls: each armed match tears down the next proxied call
/// whose path contains it, once.
/// </summary>
internal sealed class GatewayFaults
{
    private readonly Lock _gate = new();
    private readonly List<string> _armed = [];

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

        control.MapGet("/destinations", (IProxyStateLookup state) =>
        {
            if (!state.TryGetCluster("silos", out var cluster))
            {
                return Results.NotFound();
            }

            var destinations = cluster.Destinations.Values
                .Select(destination => new
                {
                    id = destination.DestinationId,
                    address = destination.Model.Config.Address,
                    health = destination.Health.Active.ToString(),
                })
                .OrderBy(destination => destination.id, StringComparer.Ordinal)
                .ToArray();
            return Results.Ok(new { pendingFaults = app.Services.GetRequiredService<GatewayFaults>().Pending, destinations });
        });
    }
}
