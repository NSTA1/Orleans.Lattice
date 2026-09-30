namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The loopback plumbing the in-process worlds share. It needs no browser, but it lives
/// with the browser suite, so it carries the <c>UI</c> category like every fixture here.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class LoopbackEndpointsTests
{
    /// <summary>
    /// A released port is free again, so the operating system may hand it straight back.
    /// A world reserves several ports before it binds any, so a repeat would make two of
    /// its listeners collide.
    /// </summary>
    [Test]
    public void Reserved_ports_are_never_handed_out_twice()
    {
        var ports = Enumerable.Range(0, 64).Select(_ => LoopbackEndpoints.ReservePort()).ToList();

        Assert.That(ports, Is.Unique);
    }
}
