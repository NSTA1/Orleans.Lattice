using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Baseline;

/// <summary>
/// Grain holding the baseline-backend view of a single part. The grain key
/// is the part's serial-number string.
/// </summary>
public interface IBaselinePartGrain : IGrainWithStringKey
{
    /// <summary>Appends <paramref name="fact"/> in arrival order and advances the cached state.</summary>
    Task AppendAsync(Fact fact);

    /// <summary>Returns the current state under the naïve (arrival-order) fold.</summary>
    Task<ComplianceState> GetStateAsync();

    /// <summary>Returns the arrival-order fact log — useful for the evidence trail.</summary>
    Task<IReadOnlyList<Fact>> GetFactsAsync();
}
