namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for the loopback-origin classifier behind
/// <see cref="LatticeApiMcpOptions.AllowLoopbackOrigins"/>: only a well-formed
/// <c>http</c>/<c>https</c> origin naming <c>localhost</c> or a loopback IP is
/// treated as local, so a lookalike hostname cannot pass as loopback.
/// </summary>
[TestFixture]
public sealed class McpOriginValidationMiddlewareTests
{
    [TestCase("http://localhost")]
    [TestCase("http://LOCALHOST:6274")]
    [TestCase("https://localhost:8443")]
    [TestCase("http://127.0.0.1:3000")]
    [TestCase("http://127.8.9.10")]
    [TestCase("http://[::1]:3000")]
    public void Loopback_origins_are_recognised(string origin)
    {
        Assert.That(McpOriginValidationMiddleware.IsLoopbackOrigin(origin), Is.True);
    }

    [TestCase("null")]
    [TestCase("")]
    [TestCase("http://localhost.attacker.example")]
    [TestCase("http://attacker.example")]
    [TestCase("http://10.0.0.5")]
    [TestCase("http://0.0.0.0")]
    [TestCase("file://localhost")]
    [TestCase("ftp://127.0.0.1")]
    [TestCase("http://user@localhost")]
    [TestCase("http://localhost/path")]
    public void Non_loopback_or_malformed_origins_are_rejected(string origin)
    {
        Assert.That(McpOriginValidationMiddleware.IsLoopbackOrigin(origin), Is.False);
    }
}
