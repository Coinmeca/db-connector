package conf

type Config struct {
	Chains []string

	Common struct {
		ServiceId string
	}

	DataDirectory struct {
		Root     string
		Keystore string
		Journal  string
		Log      string
		ExAccKey string
	}

	Port struct {
		Server     string
		Http       int
		Prometheus int
	}

	Grpc struct {
		ServerAddr string
	}

	Gclient struct {
		GrpcPort string
	}

	Repositories map[string]map[string]interface{}

	Contracts map[string]map[string]interface{}

	Log struct {
		Terminal struct {
			Use       bool
			Verbosity int
		}
		File struct {
			Use       bool
			Verbosity int
			FileName  string
		}
	}

	CoinMarketCapAPI struct {
		Url    string
		ApiKey string
	}

	AlchemyAPI struct {
		Sepolia string
	}
}
