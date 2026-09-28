namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>One request the <see cref="ShellTransportPeer"/> saw.</summary>
/// <param name="Path">The RPC path, <c>/service/Method</c>.</param>
/// <param name="Headers">The request headers, keyed by lower-case name.</param>
internal sealed record ShellTransportRequest(string Path, IReadOnlyDictionary<string, string> Headers)
{
    /// <summary>The <c>authorization</c> header, or <see langword="null"/> when none was sent.</summary>
    public string? Authorization => Headers.TryGetValue("authorization", out var value) ? value : null;
}
