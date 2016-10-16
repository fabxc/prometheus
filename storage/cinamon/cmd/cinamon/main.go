package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/cinamon"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "cinamon",
		Short: "CLI tool for cinamon",
	}

	root.AddCommand(
		NewBenchCommand(),
	)

	root.Execute()
}

func NewBenchCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "bench",
		Short: "run benchmarks",
	}
	c.AddCommand(NewBenchWriteCommand())

	return c
}

type writeBenchmark struct {
	outPath    string
	cleanup    bool
	engine     string
	numMetrics int

	storage benchmarkStorage

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
}

func NewBenchWriteCommand() *cobra.Command {
	var wb writeBenchmark
	c := &cobra.Command{
		Use:   "write <file>",
		Short: "run a write performance benchmark",
		Run:   wb.run,
	}
	c.PersistentFlags().StringVar(&wb.engine, "engine", "cinamon", "the storage engine to use")
	c.PersistentFlags().StringVar(&wb.outPath, "out", "benchout/", "set the output path")
	c.PersistentFlags().IntVar(&wb.numMetrics, "metrics", 10000, "number of metrics to read")
	return c
}

func (b *writeBenchmark) run(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		exitWithError(fmt.Errorf("missing file argument"))
	}
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tindex_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	dir := filepath.Join(b.outPath, "storage")

	switch b.engine {
	case "cinamon":
		st, err := newCinamonStorage(dir)
		if err != nil {
			exitWithError(err)
		}
		b.storage = st
	case "default":
		st, err := newDefaultStorage(dir)
		if err != nil {
			exitWithError(err)
		}
		b.storage = st
	default:
		exitWithError(fmt.Errorf("unknown storage engine %q", b.engine))
	}
	var metrics []model.Metric

	measureTime("readData", func() {
		f, err := os.Open(args[0])
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		metrics, err = readPrometheusLabels(f, b.numMetrics)
		if err != nil {
			exitWithError(err)
		}
	})

	defer func() {
		reportSize(dir)
		if b.cleanup {
			os.RemoveAll(b.outPath)
		}
	}()

	measureTime("ingestScrapes", func() {
		b.startProfiling()
		if err := b.ingestScrapes(metrics, 2000); err != nil {
			exitWithError(err)
		}
	})
	measureTime("stopStorage", func() {
		if err := b.storage.stop(); err != nil {
			exitWithError(err)
		}
		b.stopProfiling()
	})
}

func (b *writeBenchmark) ingestScrapes(metrics []model.Metric, scrapeCount int) error {
	var wg sync.WaitGroup

	for len(metrics) > 0 {
		l := 1000
		if len(metrics) < 1000 {
			l = len(metrics)
		}
		batch := metrics[:l]
		metrics = metrics[l:]

		wg.Add(1)
		go func() {
			if err := b.ingestScrapesShard(batch, scrapeCount); err != nil {
				// exitWithError(err)
				fmt.Println(" err", err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

func (b *writeBenchmark) ingestScrapesShard(metrics []model.Metric, scrapeCount int) error {
	var sc cinamon.Scrape
	base := model.Now()

	for i := 0; i < scrapeCount; i++ {
		sc.Reset(base + model.Time(i*10000))

		for _, m := range metrics {
			sc.Add(m, model.SampleValue(rand.Float64()))
		}
		if err := b.storage.ingestScrape(&sc); err != nil {
			return err
		}
	}
	return nil
}

type benchmarkStorage interface {
	ingestScrape(*cinamon.Scrape) error
	stop() error
}

type cinamonStorage struct {
	c *cinamon.Cinamon
}

func (c *cinamonStorage) stop() error {
	return c.c.Stop()
}

func (c *cinamonStorage) ingestScrape(s *cinamon.Scrape) error {
	return c.c.Append(s)
}

func newCinamonStorage(path string) (*cinamonStorage, error) {
	c, err := cinamon.Open(path)
	if err != nil {
		return nil, err
	}
	return &cinamonStorage{
		c: c,
	}, nil
}

type defaultStorage struct {
	s local.Storage
}

func (s *defaultStorage) ingestScrape(scrape *cinamon.Scrape) error {
	for _, sa := range scrape.Dump() {
		if err := s.s.Append(sa); err != nil {
			return err
		}
	}
	return nil
}

func (s *defaultStorage) stop() error {
	return s.s.Stop()
}

func newDefaultStorage(path string) (*defaultStorage, error) {
	s := local.NewMemorySeriesStorage(&local.MemorySeriesStorageOptions{
		PersistenceStoragePath: path,
		SyncStrategy:           local.Adaptive,
		CheckpointInterval:     5 * time.Minute,
	})
	return &defaultStorage{s: s}, s.Start()
}

func (b *writeBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v\n", err))
	}
	pprof.StartCPUProfile(b.cpuprof)

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v\n", err))
	}
	runtime.MemProfileRate = 4096

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v\n", err))
	}
	runtime.SetBlockProfileRate(1)
}

func (b *writeBenchmark) stopProfiling() {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		pprof.Lookup("heap").WriteTo(b.memprof, 0)
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		pprof.Lookup("block").WriteTo(b.blockprof, 0)
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
}

func reportSize(dir string) {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || path == dir {
			return err
		}
		if info.Size() < 10*1024*1024 {
			return nil
		}
		fmt.Printf(" > file=%s size=%.04fGiB\n", path[len(dir):], float64(info.Size())/1024/1024/1024)
		return nil
	})
	if err != nil {
		exitWithError(err)
	}
}

func measureTime(stage string, f func()) {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
}

func readPrometheusLabels(r io.Reader, n int) ([]model.Metric, error) {
	dec := expfmt.NewDecoder(r, expfmt.FmtProtoText)

	var mets []model.Metric
	fps := map[model.Fingerprint]struct{}{}
	var mf dto.MetricFamily
	var dups int

	for i := 0; i < n; {
		if err := dec.Decode(&mf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		for _, m := range mf.GetMetric() {
			met := make(model.Metric, len(m.GetLabel())+1)
			met["__name__"] = model.LabelValue(mf.GetName())

			for _, l := range m.GetLabel() {
				met[model.LabelName(l.GetName())] = model.LabelValue(l.GetValue())
			}
			if _, ok := fps[met.Fingerprint()]; ok {
				dups++
			} else {
				mets = append(mets, met)
				fps[met.Fingerprint()] = struct{}{}
			}
			i++
		}
	}
	if dups > 0 {
		fmt.Println("dropped duplicate metrics:", dups)
	}
	fmt.Println("read metrics", len(mets))

	return mets, nil
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
