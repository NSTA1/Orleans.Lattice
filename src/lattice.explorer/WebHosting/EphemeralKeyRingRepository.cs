using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// An in-memory Data Protection key repository: the keys it holds live only as
/// long as the process, so every restart starts a fresh key ring and every
/// cookie the previous process issued stops decrypting. The Explorer installs it
/// when the host configured no key repository of its own, so the default is
/// ephemeral on every platform rather than persisting to the user profile.
/// </summary>
internal sealed class EphemeralKeyRingRepository : IXmlRepository
{
    private readonly List<XElement> _elements = [];
    private readonly Lock _gate = new();

    /// <inheritdoc />
    public IReadOnlyCollection<XElement> GetAllElements()
    {
        lock (_gate)
        {
            return _elements.Select(e => new XElement(e)).ToList();
        }
    }

    /// <inheritdoc />
    public void StoreElement(XElement element, string friendlyName)
    {
        ArgumentNullException.ThrowIfNull(element);

        lock (_gate)
        {
            _elements.Add(new XElement(element));
        }
    }
}
