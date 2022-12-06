using CsvHelper;
using CsvHelper.Configuration;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Globalization;
using System.Text;
using System.Configuration;

namespace App.CSVReaderWriter
{
    public partial class Form1 : Form
    {
        string csvPath = "";
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private void button1_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Title = "Open CSV file",
                Filter = "csv files (*.csv)|*.csv",
                CheckFileExists = true,
                CheckPathExists = true
            };

            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                MessageBox.Show(openFileDialog.FileName);
                csvPath = openFileDialog.FileName;
            }
        }

        public class SensorModel
        {
            public string MeasurementValue { get; set; }
            public string DeviceId { get; set; }
            public DateTime Timestamp { get => DateTime.UtcNow; }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "meteringQ",
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = false, };

            using (var reader = new StreamReader(csvPath))
            using (var csv = new CsvReader(reader, config))
            {
                List<string> records = new List<string>();

                //MessageBox.Show("Total Records: " + recordsList.Count().ToString());
                while (csv.Read())
                {
                    string stringField = csv.GetField<string>(0);
                    records.Add(stringField);
                }

                foreach (string record in records)
                {
                    var message = new SensorModel()
                    {
                        MeasurementValue = record,
                        DeviceId = ConfigurationManager.AppSettings.Get("DeviceId"),
                    };
                    var json = JsonConvert.SerializeObject(message);
                    var body = Encoding.UTF8.GetBytes(json);

                    channel.BasicPublish(exchange: "", routingKey: "meteringQ", body: body);
                    Thread.Sleep(5000);
                }
            }
        }
    }
}
