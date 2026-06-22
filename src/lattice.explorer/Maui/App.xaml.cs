namespace Orleans.Lattice.Explorer;

/// <summary>
/// The MAUI application root for the Windows desktop head.
/// </summary>
public partial class App : Application
{
    /// <summary>
    /// Initializes a new instance of the <see cref="App"/> class.
    /// </summary>
    public App()
    {
        InitializeComponent();
    }

    /// <inheritdoc />
    protected override Window CreateWindow(IActivationState? activationState)
    {
        return new Window(new MainPage()) { Title = "Orleans Lattice Explorer" };
    }
}
