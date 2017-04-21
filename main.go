package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bmizerany/perks/quantile"
)

type TimingResult struct {
	URL          string
	StatusCode   int16
	ConnectTime  time.Duration
	HeadersTime  time.Duration
	CompleteTime time.Duration
}
type Report []TimingResult

const reportBatch = 20

// A Fence represents the boundaries for a Cow.
type Fence struct {
	URLs       []string
	Stagger    bool
	StaggerMax time.Duration

	Ctx    context.Context
	Report chan<- Report
	Wg     *sync.WaitGroup
}

const staggerMin = 1 * time.Second

func cow(c *Fence) {
	defer c.Wg.Done()
	if c.Stagger {
		dur := staggerMin + time.Duration(rand.Int63()%int64(c.StaggerMax-staggerMin))
		time.Sleep(dur)
	} else {
		time.Sleep(staggerMin)
	}

	lenURLs := len(c.URLs)
	var results []TimingResult = make([]TimingResult, 0, reportBatch)
	var startReq time.Time
	var dialComplete time.Time
	var headersComplete time.Time
	var bodyComplete time.Time
	defaultDialer := net.Dialer{}
	tr := &http.Transport{
		// Log when the connection finishes
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := defaultDialer.DialContext(ctx, network, addr)
			dialComplete = time.Now()
			return c, err
		},
	}
	cl := http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // do not follow redirects
		},
	}

	reqs := make(map[string]*http.Request)
	for _, u := range c.URLs {
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			panic(err) // this should've been checked for already, so crash the program
		}
		req = req.WithContext(c.Ctx)
		reqs[u] = req
	}

loop:
	for {
		select {
		case <-c.Ctx.Done():
			break loop
		default:
		}

		u := c.URLs[rand.Intn(lenURLs)]
		req := reqs[u]
		startReq = time.Now()
		// sometimes an extra dial is not necessary, measure this as 0ms
		dialComplete = startReq
		resp, err := cl.Do(req)

		if uErr, ok := err.(*url.Error); ok {
			if uErr.Err == context.Canceled || uErr.Err == context.DeadlineExceeded {
				break loop
			}
		}
		if err != nil {
			headersComplete = time.Now()
			bodyComplete = headersComplete
			results = append(results, TimingResult{
				URL:          u,
				StatusCode:   999, // todo: classify connection errors?
				ConnectTime:  dialComplete.Sub(startReq),
				HeadersTime:  headersComplete.Sub(startReq),
				CompleteTime: bodyComplete.Sub(startReq),
			})
			fmt.Printf("Error contacting %s: %T %v", u, err, err)
		} else {
			headersComplete = time.Now()
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			bodyComplete = time.Now()
			results = append(results, TimingResult{
				URL:          u,
				StatusCode:   int16(resp.StatusCode),
				ConnectTime:  dialComplete.Sub(startReq),
				HeadersTime:  headersComplete.Sub(startReq),
				CompleteTime: bodyComplete.Sub(startReq),
			})
			// REMOVE THIS, THIS TAKES A MUTEX LOCK
			//fmt.Println(resp.StatusCode, "in", bodyComplete.Sub(startReq))
		}

		if len(results) == reportBatch {
			c.Report <- results
			results = make([]TimingResult, 0, reportBatch)
		}
	}

	c.Report <- results
	return
}

func main() {
	numCattle := flag.Int("c", 8, "number of threads")
	duration := flag.Duration("d", 1*time.Minute, "total duration to run test")
	configFile := flag.String("f", "", "file with URLs, one per line")
	staggerDelay := flag.Duration("delay", staggerMin, "Delay a random time between 1s and this before starting each worker")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n"+
			"  %s [flags ...] url1 url2 ...\n"+
			"  %s -f={file} [flags ...]\n"+
			"Flags:\n", os.Args[0], os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	var urlStrs []string

	if *configFile != "" {
		f, err := os.Open(*configFile)
		if err != nil {
			fmt.Println("could not open config file:", err)
			os.Exit(1)
		}
		s := bufio.NewScanner(f)
		for s.Scan() {
			urlStrs = append(urlStrs, s.Text())
		}
		if s.Err() != nil {
			fmt.Println("error reading config file:", err)
			os.Exit(1)
		}
	} else {
		urlStrs = flag.Args()
	}

	anyErr := false
	var passingURLs []string
	for _, v := range urlStrs {
		if strings.HasPrefix(v, "#") {
			continue
		}
		_, err := url.Parse(v)
		if err != nil {
			fmt.Printf("url parse error: %s for '%s'\n", err, v)
			anyErr = true
			continue
		}
		passingURLs = append(passingURLs, v)
	}
	if anyErr {
		fmt.Println("Aborting.")
		os.Exit(3)
	}

	reportCh := make(chan Report)
	var wg sync.WaitGroup

	fence := &Fence{
		URLs:   passingURLs,
		Report: reportCh,
		Wg:     &wg,
	}
	if *staggerDelay > staggerMin {
		fence.Stagger = true
		fence.StaggerMax = *staggerDelay
	}

	fmt.Println("Starting", *numCattle, "threads")
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	fence.Ctx = ctx

	wg.Add(*numCattle)
	for i := 0; i < *numCattle; i++ {
		go cow(fence)
	}

	go func() {
		wg.Wait()
		close(reportCh)
	}()

	connectStats := make(map[string]*quantile.Stream)
	headersStats := make(map[string]*quantile.Stream)
	completeStats := make(map[string]*quantile.Stream)
	statusCodeStats := make(map[string]map[int16]int64)
	for _, u := range fence.URLs {
		connectStats[u] = quantile.NewBiased()
		headersStats[u] = quantile.NewBiased()
		completeStats[u] = quantile.NewBiased()
		statusCodeStats[u] = make(map[int16]int64)
	}
	// Collect results
	for v := range reportCh {
		for _, tr := range v {
			connectStats[tr.URL].Insert(float64(tr.ConnectTime))
			headersStats[tr.URL].Insert(float64(tr.HeadersTime))
			completeStats[tr.URL].Insert(float64(tr.CompleteTime))
			statusCodeStats[tr.URL][tr.StatusCode]++
		}
		fmt.Printf(".")
	}
	fmt.Printf("\n")

	fmt.Println("Results")
	for _, u := range fence.URLs {
		fmt.Println("URL:", u)
		q := connectStats[u]
		fmt.Printf("  Connect: %16s 50%% %16s 90%% %16s 99%%\n", time.Duration(q.Query(0.5)), time.Duration(q.Query(0.9)), time.Duration(q.Query(0.99)))
		q = headersStats[u]
		fmt.Printf("  Headers: %16s 50%% %16s 90%% %16s 99%%\n", time.Duration(q.Query(0.5)), time.Duration(q.Query(0.9)), time.Duration(q.Query(0.99)))
		q = completeStats[u]
		fmt.Printf("     Body: %16s 50%% %16s 90%% %16s 99%%\n", time.Duration(q.Query(0.5)), time.Duration(q.Query(0.9)), time.Duration(q.Query(0.99)))
		fmt.Printf(" RespCode: %v\n", statusCodeStats[u])
	}
}
