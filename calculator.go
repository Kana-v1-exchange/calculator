package main

import (
	postgres "github.com/Kana-v1-exchange/enviroment/postgres"
	rmq "github.com/Kana-v1-exchange/enviroment/rmq"
	redis "github.com/Kana-v1-exchange/enviroment/redis"
	"github.com/Kana-v1-exchange/enviroment/helpers"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
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

			err := calculator.RmqHandler.Write(fmt.Sprintf(`{"%v":"%v"}`, currency, value))
			if err != nil {
				panic(err)
			}

			err = calculator.PostgresHandler.UpdateCurrency(currency, value)
			if err != nil {
				panic(err)
			}

			wg.Done()
		}(currency, value)
	}

	wg.Wait()
}

func (calculator *Calculator) recalculateCurrency(usersNum int, currency string, currencyValue float64) float64 {
	opsKey := currency + helpers.RedisCurrencyOperationsSuffix
	operations, err := calculator.RedisHandler.Get(opsKey)
	if err != nil {
		panic(err)
	}

	ops, err := strconv.ParseFloat(operations, 64)
	if err != nil {
		panic(err)
	}

	if ops/float64(usersNum) < calculator.operationsPerUserBound {
		return currencyValue
	}

	opsPricesKey := currency + helpers.RedisCurrencyPriceSuffix
	operationsPrices, err := calculator.RedisHandler.Get(opsPricesKey)
	if err != nil {
		panic(err)
	}

	go func(opsKey, opsPricesKey string) {
		err := calculator.RedisHandler.Remove(opsKey, opsPricesKey)
		if err != nil {
			panic(err)
		}
	}(opsKey, opsPricesKey)

	prices := make([]float64, 0)

	err = json.Unmarshal([]byte(operationsPrices), &prices)
	if err != nil {
		panic(err)
	}

	if len(prices) == 0 {
		return currencyValue
	}

	pricesSum := float64(0)

	for _, price := range prices {
		pricesSum += price
	}

	return pricesSum / float64(len(prices))
}
