using Microsoft.Extensions.Configuration;
using System;
using System.IO;

public static class ConfigManager
{
	private static IConfiguration _config;
	private static string _environment;
	private static string _configPath;

	public static void Initialize(string environment = null, string configPath = null)
	{
		if (environment == null)
		{
			environment = Environment.GetEnvironmentVariable("ENVIRONMENT") ?? "dev";
		}

		if (configPath == null)
		{
			configPath = Environment.GetEnvironmentVariable("SHARED_CONFIG_PATH") ?? @"D:\SharedConfigs";
		}

		_environment = environment;
		_configPath = configPath;

		_config = new ConfigurationBuilder()
			.SetBasePath(configPath)
			.AddJsonFile($"appsettings.shared.{environment}.json", optional: false, reloadOnChange: true)
			.Build();
	}
	public static string AppEnvironment => _environment;

	public static string MySqlUrl => _config["DatabaseSettings:MySqlUrl"];
	public static string MongoDbUrl => _config["DatabaseSettings:MongoDbUrl"];
	public static string MySqlOdbcUrl => _config["DatabaseSettings:MySqlOdbcUrl"];
	public static string ApiSecuFarmBaseUrl => _config["ApiEndpoints:ApiSecuFarmBaseUrl"];
	public static string ApiSecuFarmBaseUrl2 => _config["ApiEndpoints:ApiSecuFarmBaseUrl2"];
	public static string ApiSecuFarmBaseUrl3 => _config["ApiEndpoints:ApiSecuFarmBaseUrl3"];
	public static string ApiSecuFarmBaseUrl4 => _config["ApiEndpoints:ApiSecuFarmBaseUrl4"];
	public static string ApiSecuFarmBaseUrl5 => _config["ApiEndpoints:ApiSecuFarmBaseUrl5"];
	public static string ApiSecuFarmBaseUrl6 => _config["ApiEndpoints:ApiSecuFarmBaseUrl6"];
	public static string SecuSenseBaseUrl => _config["ApiEndpoints:SecuSenseBaseUrl"];
}
