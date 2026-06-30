package config

import "time"

// Tags are just for documentation
type EmailConfig struct {
	EMAIL_GRPC_PORT           int           `default:"9007"`
	SMTP_HOST                 string        `required:"true"`
	SMTP_PORT                 int           `default:"587"`
	SMTP_USERNAME             string        `required:"true"`
	SMTP_PASSWORD             string        `required:"true"`
	SMTP_FROM_EMAIL           string        `required:"true"`
	SMTP_FROM_NAME            string        `default:"Errebus"`
	SMTP_TLS_MODE             string        `default:"starttls"`
	SMTP_CONNECTION_POOL_SIZE int           `default:"5"`
	SMTP_RETRY_COUNT          int           `default:"3"`
	SMTP_RETRY_DELAY          time.Duration `default:"5s"`
	EMAIL_BASE_URL            string        `required:"true"`
	OTEL_EXPORTER_ENDPOINT    string        `default:"otel-collector:4318"`
}

func LoadEmailConfig() (*EmailConfig, error) {
	// Required fields — fail fast if missing
	smtpHost, err := requireEnv("SMTP_HOST")
	if err != nil {
		return nil, err
	}
	smtpUsername, err := requireEnv("SMTP_USERNAME")
	if err != nil {
		return nil, err
	}
	smtpPassword, err := requireEnv("SMTP_PASSWORD")
	if err != nil {
		return nil, err
	}
	smtpFromEmail, err := requireEnv("SMTP_FROM_EMAIL")
	if err != nil {
		return nil, err
	}
	emailBaseURL, err := requireEnv("EMAIL_BASE_URL")
	if err != nil {
		return nil, err
	}

	// Build config — optional fields use defaults
	return &EmailConfig{
		EMAIL_GRPC_PORT:           getIntEnv("EMAIL_GRPC_PORT", 9007),
		SMTP_HOST:                 smtpHost,
		SMTP_PORT:                 getIntEnv("SMTP_PORT", 587),
		SMTP_USERNAME:             smtpUsername,
		SMTP_PASSWORD:             smtpPassword,
		SMTP_FROM_EMAIL:           smtpFromEmail,
		SMTP_FROM_NAME:            getEnv("SMTP_FROM_NAME", "Errebus"),
		SMTP_TLS_MODE:             getEnv("SMTP_TLS_MODE", "starttls"),
		SMTP_CONNECTION_POOL_SIZE: getIntEnv("SMTP_CONNECTION_POOL_SIZE", 5),
		SMTP_RETRY_COUNT:          getIntEnv("SMTP_RETRY_COUNT", 3),
		SMTP_RETRY_DELAY:          getEnvDuration("SMTP_RETRY_DELAY", 5*time.Second),
		EMAIL_BASE_URL:            emailBaseURL,
		OTEL_EXPORTER_ENDPOINT:    getEnv("OTEL_EXPORTER_ENDPOINT", "otel-collector:4318"),
	}, nil
}
