// loading configuration from config.cfg
using System;





/// config variables global use

namespace ConfigHandler
{
    public static class ConfigHandler
    {

        //variable init
        public static int TableChunkSize;



        // loading config from file
        public static void LoadConfig()
        {
            string configFilePath = "base/config.cfg";
            if (!System.IO.File.Exists(configFilePath))
            {
                throw new Exception("Config file not found: " + configFilePath);
            }

            var lines = System.IO.File.ReadAllLines(configFilePath);
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                {
                    continue; // skip empty lines and comments
                }

                var parts = trimmedLine.Split(new char[] { '=' }, 2);
                if (parts.Length != 2)
                {
                    continue; // skip malformed lines
                }

                var key = parts[0].Trim();
                var value = parts[1].Trim();

                switch (key)
                {
                    case "table_chunk_size":
                        TableChunkSize = int.Parse(value);
                        break;

                    // add more config variables here as needed

                    default:
                        // unknown config key, ignore or log warning
                        break;
                }
            }
        }

    }
}