using System;

namespace NetflixImporter
{
    public class MySqlConnectionInfo : ICloneable
    {
        public string Name;
        public string Host;
        public string Username;
        public string Password;
        public string Schema;
        public int    Port;
        public int    Timeout;

        public object Clone()
        {
            return new MySqlConnectionInfo
            {
                Host     = Host,
                Name     = Name,
                Password = Password,
                Schema   = Schema,
                Username = Username
            };
        }

        public MySqlConnectionInfo(string host, string username, string password, string schema)
        {
            Clear();

            Host     = host;
            Username = username;
            Password = password;
            Schema   = schema;
        }

        public MySqlConnectionInfo()
        {
            Clear();
        }

        public void TrimAll()
        {
            Name     = Name.Trim();
            Host     = Host.Trim();
            Username = Username.Trim();
            Schema   = Schema.Trim();
        }

        public string ConnectionString
        {
            get
            {
                return string.Format("Data Source={0};Database={1};Persist Security Info=yes;UserId={2};PWD={3};Connect Timeout={4};Port={5};",
                        Host,
                        Schema,
                        Username,
                        Password,
                        Timeout,
                        Port);
            }
        }

        public string SettingsString
        {
            get
            {
                return string.Format("Name={0}\tHost={1}\tUsername={2}\tPassword={3}\tSchema={4}\tPort={5}\tTimeout={6}",
                        Name, Host, Username, Password, Schema, Port, Timeout);
            }
        }
        public void Clear()
        {
            Name     = string.Empty;
            Host     = string.Empty;
            Username = string.Empty;
            Schema   = string.Empty;
            Port     = 3306;
            Timeout  = 30;
        }
    }
}
