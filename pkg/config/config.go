package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// String env var with a default fallback value
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// string env with no fallback value, error on failed
func requireEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("required env var %s is not set", key)
	}
	return v, nil
}

// Integer env var with a default fallback value
func getIntEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fallback
		}
		return n
	}
	return fallback
}

// read bool env var with a default fallback
func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, _ := strconv.ParseBool(v)
		return b
	}
	return fallback
}

// read duration env var with a default
func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, _ := time.ParseDuration(v)
		return d
	}
	return fallback
}
