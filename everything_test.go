package pipeline

import (
	"go.uber.org/atomic"
	"log"
	"math/rand"
	"testing"
	"time"
)

func Generator(n int) func(chan<- interface{}) {
	return func(out chan<- interface{}) {
		for i := 0; i < n; i++ {
			out <- rand.Intn(10)
		}

		close(out)
	}
}

func Mult(in <-chan interface{}, out chan<- interface{}) {
	for i := range in {
		out <- i.(int) * 10
	}
	close(out)
}

func Plus(in <-chan interface{}, out chan<- interface{}) {
	for i := range in {
		out <- i.(int) + 1
	}
	close(out)
}

func Printer(in <-chan interface{}) {
	for i := range in {
		log.Println(i)
	}
}

func Counter() (func(<-chan interface{}), *atomic.Int64) {
	res := atomic.NewInt64(0)

	return func(in <-chan interface{}) {
		for range in {
			res.Add(1)
		}
	}, res
}

func TestWorks(t *testing.T) {
	counter, cnt := Counter()
	const N = 10

	NewPipeLine(Generator(N), 1).WithPipe(Mult, 1).WithPipe(Plus, 1).WithSinks(counter, 1).Up().Wait()

	if cnt.Load() != N {
		t.Fail()
	}
}


func TestWorksInParallel(t *testing.T) {
	counter1, cnt1 := Counter()
	counter2, cnt2 := Counter()
	const N = 100

	NewPipeLine(Generator(N), 1).WithPipe(Mult, 1).WithPipe(Plus, 1).WithSinks(counter1, 1).Up().Wait()
	NewPipeLine(Generator(1), N).WithPipe(Mult, 1).WithPipe(Plus, 1).WithSinks(counter2, 1).Up().Wait()

	if cnt1.Load() != cnt2.Load() {
		t.Fail()
	}
}

func TestWaits(t *testing.T) {
	counter, cnt := Counter()

	NewPipeLine(Generator(1000), 1).WithPipe(PipeFromFunc(func(arg interface{}) interface{} {
		time.Sleep(100 * time.Millisecond)
		return arg
	}), 100).WithSinks(counter, 1).Up().Wait()

	if cnt.Load() != 1000 {
		t.Fatalf("cnt != 1000: %v", cnt)
	}
}

func Benchmark1(b *testing.B) {
	counter, _ := Counter()
	p := NewPipeLine(Generator(b.N), 1).WithSinks(counter, 1)
	b.ResetTimer()

	p.Up().Wait()
	b.StopTimer()
}

func Benchmark2(b *testing.B) {
	counter, _ := Counter()
	p := NewPipeLine(Generator(b.N / 2), 2).WithSinks(counter, 2)
	b.ResetTimer()

	p.Up().Wait()
	b.StopTimer()
}

func Benchmark4(b *testing.B) {
	counter, _ := Counter()
	p := NewPipeLine(Generator(b.N / 4), 4).WithSinks(counter, 4)
	b.ResetTimer()

	p.Up().Wait()
	b.StopTimer()
}

func Benchmark10(b *testing.B) {
	counter, _ := Counter()
	p := NewPipeLine(Generator(b.N / 10), 10).WithSinks(counter, 10)
	b.ResetTimer()

	p.Up().Wait()
	b.StopTimer()
}

func Benchmark100(b *testing.B) {
	counter, _ := Counter()
	p := NewPipeLine(Generator(b.N / 100), 100).WithSinks(counter, 100)
	b.ResetTimer()

	p.Up().Wait()
	b.StopTimer()
}