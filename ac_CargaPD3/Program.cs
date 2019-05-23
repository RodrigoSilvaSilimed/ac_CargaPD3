using System;

namespace ac_CargaPD3
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Carga Iniciada!");
            Iniciar();

        }

        static async void Iniciar ()
        {
            Export export = new Export();
            
            int i = await export.IniciarCarga();
            Environment.Exit(i);

        }
    }
}
