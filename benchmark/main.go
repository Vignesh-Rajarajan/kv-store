package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var (
	iterations       = flag.Int("iterations", 1000, "Number of iterations")
	readIterations   = flag.Int("readIterations", 1000, "Number of read iterations")
	concurrencyLevel = flag.Int("concurrencyLevel", 2, "Number of concurrent requests")
	addr             = flag.String("addr", "localhost:8080", "Address of the server")
)

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 300,
		MaxConnsPerHost:     300,
		MaxIdleConns:        300,
		IdleConnTimeout:     time.Minute,
	},
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	flag.Parse()

	fmt.Println(fmt.Sprintf("Running benchmarks with %d iterations and %d concurrency level", *iterations, *concurrencyLevel))

	allKeys := benchmarkWrite()
	//go benchmarkWrite()
	benchmarkRead(allKeys)
}

func benchmark(name string, iterations int, fn func() string) (qps float64, allKeys []string) {
	var maxTime time.Duration
	var minTime = 1 * time.Hour

	start := time.Now()
	for i := 0; i < iterations; i++ {
		iterStart := time.Now()
		allKeys = append(allKeys, fn())
		fn()
		iterTime := time.Since(iterStart)
		if iterTime > maxTime {
			maxTime = iterTime
		}
		if iterTime < minTime {
			minTime = iterTime
		}
	}

	avg := time.Since(start) / time.Duration(iterations)
	qps = float64(iterations) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Println(fmt.Sprintf("%s: max = %s, min = %s, avg = %s, qps = %.1f", name, maxTime, minTime, avg, qps))

	return qps, allKeys
}

func benchmarkRead(allKeys []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalQps float64

	for i := 0; i < *concurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			qps, _ := benchmark("read", *readIterations, func() string {
				return randomRead(allKeys)
			})
			mu.Lock()
			defer mu.Unlock()
			totalQps += qps
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("Total QPS: %.1f for read %d keys", totalQps, len(allKeys))
}

func benchmarkWrite() (allKeys []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalQps float64

	for i := 0; i < *concurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			qps, keys := benchmark("write", *iterations, randomWrite)
			mu.Lock()
			defer mu.Unlock()
			totalQps += qps
			allKeys = append(allKeys, keys...)
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("Total QPS: %.1f for set %d keys", totalQps, len(allKeys))
	return allKeys
}

func randomWrite() string {
	key := fmt.Sprintf("key-%d", rand.Intn(100000))
	value := fmt.Sprintf("value-%d", rand.Intn(100000))
	values := url.Values{}
	values.Add("key", key)
	values.Add("value", value)
	setUrl := fmt.Sprintf("http://%s/set?%s", *addr, values.Encode())
	resp, err := httpClient.Get(setUrl)
	if err != nil {
		log.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	return key
}

func randomRead(allKeys []string) string {

	key := allKeys[rand.Intn(len(allKeys))]
	values := url.Values{}
	values.Add("key", key)
	getUrl := fmt.Sprintf("http://%s/get?%s", *addr, values.Encode())
	resp, err := httpClient.Get(getUrl)
	if err != nil {
		log.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	return key
}
