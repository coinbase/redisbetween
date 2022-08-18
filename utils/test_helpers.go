package utils

import "os"

func RedisHost() string {
	h := os.Getenv("REDIS_HOST")
	if h != "" {
		return h
	}
	return "127.0.0.1"
}
