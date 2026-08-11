# RepoContext MCP test harness

`RepoContextMcpHarness` stands up the repository-context MCP server the way a
production host does - `AddLatticeMcp(...)` + `AddRepoContextTools()` +
`MapLatticeMcp()` - co-hosted on an in-memory Lattice cluster, served over an
in-process ASP.NET Core `TestServer`, and reachable with a real `McpClient` over
the streamable-HTTP transport using the test server's `HttpClient`.

It exists so every repository-context tool sub-issue (#1431 capture, #1432
maintenance, #1433 retrieval) and the end-to-end smoke (#1436) asserts tool
discovery, authorization gating, and request/response over the real MCP protocol
instead of unit-testing grain calls in isolation - and so the fail-closed
discovery seam (#1428) is asserted uniformly through one set of auth-posture
presets.

## Bring up a server and call a tool

```csharp
await using var harness = await RepoContextMcpHarness.StartAsync(
    new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer });
await using var client = await harness.ConnectAsync();

var names = await client.ListToolNamesAsync();
Assert.That(names, Does.Contain("repocontext_health"));

var result = await client.CallToolAsync("repocontext_health");
var json = result.RequireStructuredContent();
Assert.That(json.GetProperty("group").GetString(), Is.EqualTo("repocontext"));
```

## Assert the fail-closed seam

Switch the posture; nothing else changes:

- `RepoContextMcpAuthPosture.Unauthenticated` - the session is anonymous, so it is
  offered no tools at all (`ListToolNamesAsync()` is empty) and a direct
  `CallToolAsync` throws `McpException`.
- `RepoContextMcpAuthPosture.Reader` - granted the repository-context group but not
  the host-side write opt-in: the read-only tools are offered, no mutating tool is.
- `RepoContextMcpAuthPosture.Writer` - granted the group with the write opt-in: the
  read-only tools plus, once they land, the mutating tools (with the correct
  destructive/read-only hints) are offered.

The posture is driven through deterministic stub collaborators (a stub credential
bridge and a stub permission resolver), so it never depends on a real Auth policy
tree or its change-feed compile step.

## Operate on real trees

The harness co-hosts a real in-memory Lattice cluster, so a fixture can arrange or
assert tree state directly, off the MCP path:

```csharp
var tree = harness.GrainFactory.GetGrain<ILattice>("repo/facts");
await tree.SetAsync("key", value);
```

## Register extra facades or tool modules

A tool that adapts a transport-agnostic facade registers it through the hooks
without re-implementing bring-up:

```csharp
var options = new RepoContextMcpHarnessOptions
{
    Posture = RepoContextMcpAuthPosture.Writer,
    ConfigureSilo = silo => silo.AddLatticeDataApi(),
    ConfigureServices = services => services.AddSingleton<IMyCollaborator, MyCollaborator>(),
};
await using var harness = await RepoContextMcpHarness.StartAsync(options);
```

## Determinism and lifetime

Each harness co-hosts its own single-silo cluster with a fresh random cluster id
and its own in-memory store, so tests start from a clean slate and parallel
fixtures never share cluster state. `await using` the harness (and the clients it
hands out) to tear the silo and web host down.

## Test tier

Any fixture that starts a harness co-hosts a real Orleans silo and an in-process
HTTP server, so mark it `[Category("Integration")]` (as
`RepoContextMcpHarnessSmokeTests` does). The harness's building blocks - the
posture stubs, the client ergonomics, and the options - are covered by fast unit
fixtures that need no live server.
