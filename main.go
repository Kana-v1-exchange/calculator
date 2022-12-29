package main

import (
	"os"
	"strconv"
	"time"

	"github.com/Kana-v1-exchange/calculator/config"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(getEnvFilePath())
	if err != nil {
		panic(err)
	}

	operationsPerUser, err := strconv.ParseFloat(os.Getenv("OPERATIONS_PER_USER_LIMIT"), 64)
	if err != nil {
		panic(err)
	}

	calculator := &Calculator{
		PostgresHandler: config.GetPostgresConfig().Connect(),
		RedisHandler:    config.GetRedisConfig().Connect(),
		RmqHandler:      config.GetRmqConfig().Connect(),

		operationsPerUserBound: operationsPerUser,
	}

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			calculator.CalculateCurrencies()
		}
	}
}

func getEnvFilePath() string {
	if isExchangeLocal := os.Getenv("IS_EXCHANGE_IN_CONTAINER"); isExchangeLocal == "false" {
		return "./envs/.env"
	}

	return "./envs/.env"
}
