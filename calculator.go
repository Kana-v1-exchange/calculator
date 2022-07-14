package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	postgres "github.com/Kana-v1-exchange/enviroment/postgres"
	proto "github.com/Kana-v1-exchange/enviroment/protos/serverHandler"
	redis "github.com/Kana-v1-exchange/enviroment/redis"
	rmq "github.com/Kana-v1-exchange/enviroment/rmq"
)

type Calculator struct {
	PostgresHandler postgres.PostgresHandler
	RedisHandler    redis.RedisHandler
	RmqHandler      rmq.RmqHandler

	operationsPerUserBound float64
}

func (calculator *Calculator) CalculateCurrencies() {
	usersNum, err := calculator.PostgresHandler.GetUsersNum()
	if err != nil {
		panic(err)
	}

	currencies, err := calculator.PostgresHandler.GetCurrencies()
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(currencies))

	for currency, value := range currencies {
		go func(currency string, value float64) {
			newCurrencyValue := calculator.recalculateCurrency(usersNum, currency, value)
			if newCurrencyValue == value {
				return
			}

			message, err := json.Marshal(proto.CurrencyValue{Value: float32(value), Currency: currency})
			if err != nil {
				panic(err)
			}

			err = calculator.RmqHandler.Write(string(message))
			if err != nil {
				panic(err)
			}

			err = calculator.PostgresHandler.UpdateCurrency(currency, newCurrencyValue)
			if err != nil {
				panic(err)
			}

			wg.Done()
		}(currency, value)
	}

	wg.Wait()
}

func (calculator *Calculator) recalculateCurrency(usersNum int, currency string, currencyValue float64) float64 {
	if currency == "USD" {
		return 1
	}

	opsKey := currency + redis.RedisCurrencyOperationsSuffix
	operations, err := calculator.RedisHandler.Get(opsKey)
	if err != nil {
		fmt.Println(err)
		return currencyValue
	}

	ops, err := strconv.ParseFloat(operations, 64)
	if err != nil {
		panic(err)
	}

	if ops/float64(usersNum) <= calculator.operationsPerUserBound {
		return currencyValue
	}

	opsPricesKey := currency + redis.RedisCurrencyPriceSuffix
	operationsPrices, err := calculator.RedisHandler.GetList(opsPricesKey)
	if err != nil {
		panic(err)
	}

	go func(opsKey, opsPricesKey string) {
		err := calculator.RedisHandler.Remove(opsKey, opsPricesKey)
		if err != nil {
			panic(err)
		}
	}(opsKey, opsPricesKey)

	prices := make([]float64, len(operationsPrices))

	for i, operationPrice := range operationsPrices {
		err = json.Unmarshal([]byte(operationPrice), &prices[i])
		if err != nil {
			panic(err)
		}
	}

	if len(prices) == 0 {
		return currencyValue
	}

	pricesSum := float64(0)

	for _, price := range prices {
		pricesSum += price
	}

	err = calculator.RedisHandler.Set(opsKey, "0")
	if err != nil {
		panic(err)
	}

	return pricesSum / float64(len(prices))
}
