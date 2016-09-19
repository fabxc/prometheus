all: bench svg

bench_default: build
	@echo ">> running benchmark"
	@./cinamon bench write --out=benchout/default --engine=default --metrics=$(NUM_METRICS) testdata.100k

bench_cinamon: build 
	@echo ">> running benchmark"
	@./cinamon bench write --out=benchout/cinamon --engine=cinamon --metrics=$(NUM_METRICS) testdata.100k

build:
	@go build .

svg:
	@echo ">> create svgs"
	@go tool pprof -svg ./cinamon benchout/cinamon/cpu.prof > benchout/cinamon/cpuprof.svg
	@go tool pprof -svg ./cinamon benchout/cinamon/mem.prof > benchout/cinamon/memprof.svg
	@go tool pprof -svg ./cinamon benchout/cinamon/block.prof > benchout/cinamon/blockprof.svg
	@go tool pprof -svg ./cinamon benchout/default/cpu.prof > benchout/default/cpuprof.svg
	@go tool pprof -svg ./cinamon benchout/default/mem.prof > benchout/default/memprof.svg
	@go tool pprof -svg ./cinamon benchout/default/block.prof > benchout/default/blockprof.svg
