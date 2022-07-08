package config

import (
	postgres "github.com/Kana-v1-exchange/enviroment/postgres"
	rmq "github.com/Kana-v1-exchange/enviroment/rmq"
	redis "github.com/Kana-v1-exchange/enviroment/redis"
	"os"
)

func GetPostgresConfig() *postgres.PostgreSettings {
	return &postgres.PostgreSettings{
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		Host:     os.Getenv("POSTGRES_HOST"),
		Port:     os.Getenv("POSTGRES_PORT"),
		DbName:   os.Getenv("POSTGRES_DB_NAME"),
	}
}

func GetRedisConfig() *redis.RedisSettings {
	return &redis.RedisSettings{
		Host:     os.Getenv("REDIS_HOST"),
		Port:     os.Getenv("REDIS_PORT"),
		Password: os.Getenv("REDIS_PASSWORD"),
	}
}

func GetRmqConfig() *rmq.RMQSettings {
	return &rmq.RMQSettings{
		User: os.Getenv("RMQ_USER"),
		Password: os.Getenv("RMQ_PASSWORD"),
		Host: os.Getenv("RMQ_HOST"),
		Port: os.Getenv("RMQ_PORT"),
	}
}
