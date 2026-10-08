# Configuration

The package exposes one options type. Configure it with `AddLatticeAppBridgeApi(options => ...)`; this registration also wires the control, catalogue, and workspace facades.

## Bridge rate limit

| Option | Type | Default | Validation and effect |
|---|---|---|---|
| `RateLimitPermitLimit` | `int` | 100 (`DefaultRateLimitPermitLimit`) | Must be at least 1. Maximum requests per caller, active tenant, and app slug during one fixed window. |
| `RateLimitWindow` | `TimeSpan` | 1 second (`DefaultRateLimitWindow`) | Must be positive; length of the fixed rate-limit window. |

The limiter tracks at most 10,000 partitions. A new partition is refused when the table is full and no expired window can be reclaimed. Invalid values fail when the bridge is first resolved, not during registration.

There are no package options for the control, catalogue, or workspace facades. The gRPC binding has separate transport options; see its [configuration](../lattice.api.apps.grpc/configuration.md).

## See also

- [Public API](api.md)
- [Architecture](architecture.md)
- [Control facade guide](README.md)