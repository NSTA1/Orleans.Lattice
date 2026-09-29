namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Resolves an optional service without failing: a reader whose own dependency
/// (the circuit's state connection) the head does not register reads as absent,
/// so the Data area degrades to "not available here" instead of faulting a render.
/// </summary>
internal static class DataServices
{
    /// <summary>The service, or <see langword="null"/> when it is not registered or cannot be built.</summary>
    /// <typeparam name="TService">The service type.</typeparam>
    /// <param name="services">The circuit's services.</param>
    public static TService? Find<TService>(IServiceProvider services)
        where TService : class
    {
        ArgumentNullException.ThrowIfNull(services);
        try
        {
            return services.GetService(typeof(TService)) as TService;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }
}
